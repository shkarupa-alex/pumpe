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
        # Tokens of earlier runs that a failed release or takeover may have left in pump_lock.
        self._unreleased: set[str] = set()

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
                    # Never mask the run's own failure; the next run takes the unreleased lease back by its token.
                    await self.session.rollback()
                    self.logger.warning("Could not release the lease: %s", self.title, exc_info=True)
            raise

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
        # Checked before the lease too, so polling a pump that is not due writes nothing.
        if not await self._new_meta():
            self.logger.debug("Skip pumping: %s", self.title)
            await self.session.commit()
            return None

        if not await self._acquire_lease():
            self.logger.debug("Skip pumping, another run holds the lease: %s", self.title)
            return None

        # Decided again under the lease: another run may have finished in between.
        meta = await self._new_meta()
        if not meta:
            self.logger.debug("Skip pumping: %s", self.title)
            await self._release_lease()
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
        await self._ensure_lease_row()

        owner = uuid4().hex
        now = datetime.now(UTC)
        if self._lease is not None:
            self._unreleased.add(self._lease)
        free = or_(col(PumpLock.owner).is_(None), col(PumpLock.expires) < now)
        if self._unreleased:
            # This instance's own leases, left behind by a release or takeover that failed.
            free = or_(free, col(PumpLock.owner).in_(self._unreleased))
        takeover = (
            update(PumpLock)
            .where(col(PumpLock.pump) == self.title, free)
            .values(owner=owner, expires=now + self.lease_timeout)
        )

        # Owned before commit: if the commit is interrupted, run() still releases whatever it may have taken.
        self._lease = owner
        if (await self.session.exec(takeover)).rowcount != 1:
            # Not even one of the unreleased tokens is in pump_lock, or the update would have matched it.
            await self.session.rollback()
            self._forget_lease()
            return False

        await self.session.commit()
        # The takeover replaced whichever token was there.
        self._unreleased.clear()
        return True

    async def _ensure_lease_row(self) -> None:
        query = select(PumpLock.pump).where(PumpLock.pump == self.title)
        exists = (await self.session.exec(query)).first() is not None
        await self.session.commit()
        if exists:
            return

        # Created on its own: in the takeover's transaction, InnoDB's gap lock on the missing key would
        # deadlock two first runs of the same pump against each other.
        self.session.add(PumpLock(pump=self.title))
        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()

    @property
    def _run_token(self) -> str:
        """The current run's lease token, unique per run."""
        if self._lease is None:
            raise RuntimeError(f"Pump holds no lease: {self.title}")
        return self._lease

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
        if self._lease is None and not self._unreleased:
            return

        await self._exec_release()
        await self.session.commit()
        # Forgotten only once committed: an interrupted release is retried by run()'s failure handling.
        self._forget_lease()

    async def _exec_release(self) -> None:
        tokens = self._unreleased | ({self._lease} if self._lease is not None else set())
        query = (
            update(PumpLock)
            .where(col(PumpLock.pump) == self.title, col(PumpLock.owner).in_(tokens))
            .values(owner=None, expires=None)
        )
        await self.session.exec(query)

    def _forget_lease(self) -> None:
        self._lease = None
        self._unreleased.clear()

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

        # Source I/O can be slow: never wait on it inside the transaction the metadata queries opened.
        await self.session.commit()
        generator = self._fetch(modified_since=modified_since, created_after=created_after)
        async for batch in abatched(generator, self.batch_size):
            # One fenced transaction per batch: a run that has lost its lease cannot commit a stale batch.
            await self._renew_lease()
            await self._process_batch(batch, meta)
            await self.session.commit()

        meta.elapsed = (datetime.now(UTC) - meta.started).total_seconds()

    async def _process_batch(self, batch: tuple[dict[str, Any], ...], meta: PumpMeta) -> None:
        """Write one batch inside the transaction the caller fences and commits; do not commit here."""
        meta.skipped += len(batch)

    async def _save_meta(self, meta: PumpMeta) -> None:
        # The lease is released with the last write: a later commit would expire meta's loaded attributes.
        await self._renew_lease()
        self.session.add(meta)
        await self._exec_release()
        await self.session.commit()
        self._forget_lease()
        await self.session.refresh(meta)
        # End the refresh's transaction too, so the session does not idle inside one until the next run;
        # expunged first, meta keeps its loaded attributes whatever expire_on_commit is.
        self.session.expunge(meta)
        await self.session.commit()
