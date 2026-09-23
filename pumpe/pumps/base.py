from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any
from uuid import uuid4

import anyio
from aioitertools.itertools import batched as abatched
from sqlalchemy.exc import IntegrityError
from sqlmodel import col, or_, select, update
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpLock, PumpMeta, PumpMode


class BasePump(ABC):
    # Must exceed the longest pause between two batches: a run that stays silent longer loses its lease.
    lease_timeout = timedelta(minutes=10)

    def __init__(
        self,
        session: AsyncSession,
        full_interval: timedelta,
        part_interval: timedelta,
        past_interval: timedelta,
        batch_size: int = 100,
    ) -> None:
        self.session = session
        self.full_interval = full_interval
        self.part_interval = part_interval
        self.past_interval = past_interval
        self.batch_size = batch_size
        self._lease: str | None = None
        self._generation = 0

        self.logger = getLogger("pumpe")

    @abstractmethod
    def _fetch(
        self,
        modified_since: datetime | None,
        created_after: datetime | None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield source records; implement as an async generator (``async def`` with ``yield``)."""

    async def run(self) -> PumpMeta | None:
        try:
            meta = await self._run()
        except BaseException:
            # The session outlives a single run, so a failed flush must not leave it unusable for the next one.
            with anyio.CancelScope(shield=True):
                await self.session.rollback()
                try:
                    await self._release_lease()
                except Exception:
                    # Never mask the run's own failure: an unreleased lease just expires.
                    self.logger.warning("Could not release the lease: %s", self.title, exc_info=True)
            raise

        await self._release_lease()

        if meta is None:
            return None

        self.logger.info(
            "Pumped (%s) %s in %.1f seconds: skipped/%d, created/%d, updated/%d, deleted/%d",
            meta.mode.value,
            self.title,
            meta.elapsed,
            meta.skipped,
            meta.created,
            meta.updated,
            meta.deleted,
        )

        return meta

    async def _run(self) -> PumpMeta | None:
        if not await self._acquire_lease():
            self.logger.debug("Skip pumping, another run holds the lease: %s", self.title)
            return None

        meta = await self._new_meta()
        if not meta:
            self.logger.debug("Skip pumping: %s", self.title)
            return None

        self.logger.debug("Start pumping (%s): %s", meta.mode, self.title)
        await self._process_all(meta)
        self.logger.debug("Finish pumping (%s): %s", meta.mode, self.title)

        await self._save_meta(meta)

        return meta

    @property
    def title(self) -> str:
        return self.__class__.__name__

    async def _acquire_lease(self) -> bool:
        # Overlapping runs of one pump cannot be ordered safely (an older run could write after a newer one),
        # so runs are serialized; an expired lease is taken over, as its holder is presumed dead.
        owner = uuid4().hex
        now = datetime.now(UTC)
        takeover = (
            update(PumpLock)
            .where(
                col(PumpLock.pump) == self.title,
                or_(col(PumpLock.owner).is_(None), col(PumpLock.expires) < now),
            )
            .values(owner=owner, expires=now + self.lease_timeout, generation=col(PumpLock.generation) + 1)
        )
        # Owned before commit: if the commit is interrupted, run() still releases whatever it may have taken.
        self._lease = owner
        if (await self.session.exec(takeover)).rowcount == 1:
            query = select(PumpLock.generation).where(PumpLock.pump == self.title)
            self._generation = (await self.session.exec(query)).one()
            await self.session.commit()
            return True

        self._generation = 1
        self.session.add(PumpLock(pump=self.title, owner=owner, expires=now + self.lease_timeout, generation=1))
        try:
            await self.session.commit()
        except IntegrityError:
            self._lease = None
            await self.session.rollback()
            return False

        return True

    async def _renew_lease(self) -> None:
        """Fence the current transaction: call it before the first write of every transaction a run commits."""
        # The row lock taken by this update is held until commit, so the lease cannot change hands in between.
        query = (
            update(PumpLock)
            .where(col(PumpLock.pump) == self.title, col(PumpLock.owner) == self._lease)
            .values(expires=datetime.now(UTC) + self.lease_timeout)
        )
        if self._lease is None or (await self.session.exec(query)).rowcount != 1:
            raise RuntimeError(f"Pump lease lost, another run has taken over: {self.title}")

    async def _release_lease(self) -> None:
        if self._lease is None:
            return

        query = (
            update(PumpLock)
            .where(col(PumpLock.pump) == self.title, col(PumpLock.owner) == self._lease)
            .values(owner=None, expires=None)
        )
        self._lease = None
        await self.session.exec(query)
        await self.session.commit()

    async def _new_meta(self) -> PumpMeta | None:
        started = datetime.now(UTC)

        last_full = await self._get_last(PumpMode.FULL)
        last_part = await self._get_last(PumpMode.PARTIAL)

        if not last_full or last_full.started + self.full_interval < started:
            mode = PumpMode.FULL
        elif last_part and last_part.started + self.part_interval < started:
            mode = PumpMode.PARTIAL
        else:
            return None

        return PumpMeta(pump=self.title, mode=mode, started=started)

    async def _get_last(self, mode: PumpMode) -> PumpMeta | None:
        query = select(PumpMeta).where(PumpMeta.pump == self.title).order_by(col(PumpMeta.id).desc()).limit(1)
        if mode == PumpMode.FULL:
            query = query.where(PumpMeta.mode == mode)

        return (await self.session.exec(query)).first()

    async def _process_all(self, meta: PumpMeta) -> None:
        if meta.mode == PumpMode.FULL:
            modified_since = None
            created_after = None
        else:
            last = await self._get_last(PumpMode.PARTIAL)
            if last is None:
                raise RuntimeError(f"Partial pumping requires a previous run: {self.title}")
            modified_since = last.started
            created_after = modified_since - self.past_interval

        generator = self._fetch(modified_since=modified_since, created_after=created_after)
        async for batch in abatched(generator, self.batch_size):
            await self._process_batch(batch, meta)

        meta.elapsed = (datetime.now(UTC) - meta.started).total_seconds()

    async def _process_batch(self, batch: tuple[dict[str, Any], ...], meta: PumpMeta) -> None:
        meta.skipped += len(batch)

    async def _save_meta(self, meta: PumpMeta) -> None:
        await self._renew_lease()
        self.session.add(meta)
        await self.session.commit()
        await self.session.refresh(meta)
