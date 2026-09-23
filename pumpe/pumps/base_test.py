from asyncio import sleep
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import anyio
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpMeta, PumpMode
from pumpe.pumps.base import BasePump
from pumpe.pumps.model_test import Gate, file_sessions


class CustomTaskPump(BasePump):
    @property
    def title(self) -> str:
        return "CustomTask"

    async def _fetch(
        self,
        modified_since: datetime | None,
        created_after: datetime | None,
    ) -> AsyncGenerator[dict[str, Any]]:
        assert isinstance(modified_since, datetime) or modified_since is None
        assert isinstance(created_after, datetime) or created_after is None
        assert modified_since is None or modified_since.utcoffset() == timedelta(0)
        assert created_after is None or created_after.utcoffset() == timedelta(0)
        for _ in range(100 // (2 if modified_since else 1)):
            yield {"success": True}


@pytest.mark.asyncio
async def test_api_pump() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        pump = CustomTaskPump(
            session,
            timedelta(seconds=60),
            timedelta(seconds=1),
            timedelta(seconds=60),
        )

        full = await pump.run()
        assert not session.in_transaction()
        assert isinstance(full, PumpMeta)
        assert full.mode == PumpMode.FULL
        assert full.started.utcoffset() == timedelta(0)
        assert full.skipped == 100
        assert full.created == 0
        assert full.updated == 0
        assert full.deleted == 0

        skip = await pump.run()
        assert skip is None
        assert not session.in_transaction()

        await sleep(2)
        part = await pump.run()
        assert isinstance(part, PumpMeta)
        assert part.mode == PumpMode.PARTIAL
        assert part.skipped == 50
        assert part.created == 0
        assert part.updated == 0
        assert part.deleted == 0
    await engine.dispose()


class WrittenValue(SQLModel, table=True):
    id: int = Field(primary_key=True)
    value: int


class WritingPump(BasePump):
    """A custom pump that writes its batches itself, without ModelPump."""

    script: tuple[dict[str, Any] | Gate, ...] = ()

    async def _fetch(
        self,
        modified_since: datetime | None,  # noqa: ARG002
        created_after: datetime | None,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any]]:
        for step in self.script:
            if isinstance(step, Gate):
                step.reached.set()
                await step.opened.wait()
                continue
            yield step

    async def _process_batch(self, batch: tuple[dict[str, Any], ...], meta: PumpMeta) -> None:
        for record in batch:
            await self.session.merge(WrittenValue(**record))
        meta.updated += len(batch)


def writing_pump(session: AsyncSession, *script: dict[str, Any] | Gate) -> WritingPump:
    pump = WritingPump(session, timedelta(0), timedelta(0), timedelta(0))
    pump.script = script
    return pump


async def written_value(session: AsyncSession) -> int:
    session.expunge_all()
    return (await session.exec(select(WrittenValue.value).where(WrittenValue.id == 1))).one()


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_expires", [True, False])
async def test_custom_batch_is_fenced_by_the_lease(tmp_path: Path, *, lease_expires: bool) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        gate = Gate()
        older = writing_pump(session_a, gate, {"id": 1, "value": 10})
        if lease_expires:
            older.lease_timeout = timedelta(0)
        results: list[PumpMeta | BaseException | None] = []

        async def run_older() -> None:
            try:
                results.append(await older.run())
            except RuntimeError as e:
                results.append(e)

        async with anyio.create_task_group() as tg:
            tg.start_soon(run_older)
            await gate.reached.wait()

            newer = await writing_pump(session_b, {"id": 1, "value": 20}).run()
            # A live lease makes the newer run skip; an expired one is taken over.
            assert (newer is not None) is lease_expires
            gate.opened.set()

        [older_result] = results
        if lease_expires:
            assert isinstance(older_result, RuntimeError)
            assert "lease lost" in str(older_result)
            assert await written_value(session_b) == 20
        else:
            assert isinstance(older_result, PumpMeta)
            assert await written_value(session_b) == 10


class SlowPump(BasePump):
    async def _fetch(
        self,
        modified_since: datetime | None,  # noqa: ARG002
        created_after: datetime | None,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any]]:
        for i in range(8):
            await anyio.sleep(0.1)
            yield {"i": i}


@pytest.mark.asyncio
async def test_base_pump_keeps_lease_between_batches(tmp_path: Path) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        # Every pause between batches stays well below the lease timeout, though the whole run exceeds it.
        slow = SlowPump(session_a, timedelta(0), timedelta(0), timedelta(0), batch_size=1)
        slow.lease_timeout = timedelta(seconds=0.3)
        results: list[PumpMeta | None] = []

        async def run_slow() -> None:
            results.append(await slow.run())

        async with anyio.create_task_group() as tg:
            tg.start_soon(run_slow)
            await anyio.sleep(0.5)
            assert await SlowPump(session_b, timedelta(0), timedelta(0), timedelta(0)).run() is None

        [meta] = results
        assert meta is not None
        assert meta.mode == PumpMode.FULL
        assert meta.skipped == 8
