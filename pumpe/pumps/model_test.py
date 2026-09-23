from asyncio import sleep
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpMeta, PumpMode, PumpModel
from pumpe.pumps.model import ModelPump


class CustomModel(PumpModel, table=True):
    source: str = Field(primary_key=True)
    field1: int
    field2: float | None = None


class AnotherModel(PumpModel, table=True):
    # This model used to test correct inheritance
    id: int = Field(primary_key=True)


class CustomModelPump(ModelPump):
    num_calls = 0

    _model: type[PumpModel] = CustomModel

    async def _fetch(
        self,
        modified_since: datetime | None,
        created_after: datetime | None,
    ) -> AsyncGenerator[dict[str, Any]]:
        assert isinstance(modified_since, datetime) or modified_since is None
        assert isinstance(created_after, datetime) or created_after is None
        assert modified_since is None or modified_since.utcoffset() == timedelta(0)
        assert created_after is None or created_after.utcoffset() == timedelta(0)

        if self.num_calls < 2:
            self.num_calls += 1

            start = 75 if modified_since else 0
            end = start + 100 // (2 if modified_since else 1)
            for i in range(start, end):
                yield {"source": f"source_{i}", "field1": i, "field2": float(i) if i % 2 else None, "field3": "extra"}


@pytest.mark.asyncio
async def test_api_pump() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        pump = CustomModelPump(
            session,
            timedelta(seconds=5),
            timedelta(seconds=1),
            timedelta(seconds=60),
        )

        full = await pump.run()
        assert isinstance(full, PumpMeta)
        assert full.mode == PumpMode.FULL
        assert full.started.utcoffset() == timedelta(0)
        assert full.skipped == 0
        assert full.created == 100
        assert full.updated == 0
        assert full.deleted == 0

        query = select(CustomModel).order_by(CustomModel.source).limit(1)
        record = (await session.exec(query)).first()
        assert isinstance(record, CustomModel)
        assert record.pump_hash__ is not None
        assert len(record.pump_hash__) == 64
        assert record.pump_modified__.utcoffset() == timedelta(0)
        assert full.started < record.pump_modified__ < full.started + timedelta(seconds=5)
        assert record.pump_touched__
        assert record.pump_extra__ == {"field3": "extra"}
        assert record.source == "source_0"
        assert record.field1 == 0
        assert record.field2 is None

        skip = await pump.run()
        assert skip is None

        await sleep(3)
        part = await pump.run()
        assert isinstance(part, PumpMeta)
        assert part.mode == PumpMode.PARTIAL
        assert part.skipped == 25
        assert part.created == 25
        assert part.updated == 0
        assert part.deleted == 0

        await sleep(3)
        part = await pump.run()
        assert isinstance(part, PumpMeta)
        assert part.mode == PumpMode.FULL
        assert part.skipped == 0
        assert part.created == 0
        assert part.updated == 0
        assert part.deleted == 125
    await engine.dispose()


class RecordModel(PumpModel, table=True):
    id: int = Field(primary_key=True)
    value: int
    at: datetime | None = None


class RecordModelPump(ModelPump):
    _model: type[PumpModel] = RecordModel

    records: tuple[dict[str, Any], ...] = ()

    async def _fetch(
        self,
        modified_since: datetime | None,  # noqa: ARG002
        created_after: datetime | None,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any]]:
        for record in self.records:
            yield record


@asynccontextmanager
async def memory_session() -> AsyncGenerator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def load_record(session: AsyncSession, record_id: int) -> RecordModel:
    session.expunge_all()
    return (await session.exec(select(RecordModel).where(RecordModel.id == record_id))).one()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [PumpMode.FULL, PumpMode.PARTIAL])
async def test_extra_only_change_updates_record(mode: PumpMode) -> None:
    full_interval = timedelta(0) if mode == PumpMode.FULL else timedelta(hours=1)
    async with memory_session() as session:
        pump = RecordModelPump(session, full_interval, timedelta(0), timedelta(0))

        pump.records = ({"id": 1, "value": 10, "extra": "A"},)
        first = await pump.run()
        assert first is not None
        assert first.created == 1

        pump.records = ({"id": 1, "value": 10, "extra": "B"},)
        second = await pump.run()
        assert second is not None
        assert second.mode == mode
        assert second.skipped == 0
        assert second.updated == 1

        record = await load_record(session, 1)
        assert record.pump_extra__ == {"extra": "B"}


@pytest.mark.asyncio
async def test_full_rescan_preserves_unchanged_modified_time() -> None:
    async with memory_session() as session:
        pump = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))

        pump.records = ({"id": 1, "value": 10},)
        await pump.run()
        modified = (await load_record(session, 1)).pump_modified__

        await sleep(0.05)
        unchanged = await pump.run()
        assert unchanged is not None
        assert unchanged.mode == PumpMode.FULL
        assert unchanged.skipped == 1
        assert unchanged.updated == 0
        assert (await load_record(session, 1)).pump_modified__ == modified

        pump.records = ({"id": 1, "value": 11},)
        changed = await pump.run()
        assert changed is not None
        assert changed.updated == 1
        assert (await load_record(session, 1)).pump_modified__ > modified


@pytest.mark.asyncio
async def test_run_recovers_after_db_error() -> None:
    async with memory_session() as session:
        pump = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))

        pump.records = ({"id": 1, "value": 10, "at": datetime(2025, 1, 1, 12)},)  # noqa: DTZ001
        with pytest.raises(StatementError, match="timezone information"):
            await pump.run()

        pump.records = ({"id": 1, "value": 10, "at": datetime(2025, 1, 1, 12, tzinfo=UTC)},)
        meta = await pump.run()
        assert meta is not None
        assert meta.mode == PumpMode.FULL
        assert meta.created == 1
