from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any

from aioitertools import batched as abatched
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpMeta, PumpMode


class BasePump(ABC):
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

        self.logger = getLogger("pumpe")

    @abstractmethod
    async def _fetch(
        self,
        modified_since: datetime | None,
        created_after: datetime | None,
    ) -> AsyncGenerator[dict[str, Any]]:
        pass

    async def run(self) -> PumpMeta | None:
        meta = await self._new_meta()
        if not meta:
            self.logger.debug("Skip pumping: %s", self.title)
            return None

        self.logger.debug("Start pumping (%s): %s", meta.mode, self.title)
        await self._process_all(meta)
        self.logger.debug("Finish pumping (%s): %s", meta.mode, self.title)

        await self._save_meta(meta)

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

    @property
    def title(self) -> str:
        return self.__class__.__name__

    async def _new_meta(self) -> PumpMeta | None:
        started = datetime.now(tz=UTC)

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
        query = select(PumpMeta).where(PumpMeta.pump == self.title).order_by(PumpMeta.id.desc()).limit(1)
        if mode == PumpMode.FULL:
            query = query.where(PumpMeta.mode == mode)

        return (await self.session.exec(query)).first()

    async def _process_all(self, meta: PumpMeta) -> None:
        if meta.mode == PumpMode.FULL:
            modified_since = None
            created_after = None
        else:
            modified_since = await self._get_last(PumpMode.PARTIAL)
            created_after = modified_since.started - self.past_interval

        generator = self._fetch(modified_since=modified_since, created_after=created_after)
        async for batch in abatched(generator, self.batch_size):
            await self._process_batch(batch, meta)

        meta.elapsed = (datetime.now(tz=UTC) - meta.started).total_seconds()

    async def _process_batch(self, batch: tuple[dict[str, Any]], meta: PumpMeta) -> None:
        meta.skipped += len(batch)

    async def _save_meta(self, meta: PumpMeta) -> None:
        self.session.add(meta)
        await self.session.commit()
