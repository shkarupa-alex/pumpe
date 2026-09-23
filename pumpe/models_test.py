from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import NaiveDatetime
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpModel


class EventModel(PumpModel, table=True):
    id: int = Field(primary_key=True)
    happened: datetime | None = None
    local: NaiveDatetime | None = None


def test_aware_datetime_normalized_to_utc() -> None:
    moscow = timezone(timedelta(hours=3))
    event = EventModel.model_validate({"id": 1, "happened": datetime(2025, 1, 1, 15, tzinfo=moscow)})

    assert event.happened == datetime(2025, 1, 1, 12, tzinfo=UTC)
    assert event.happened.utcoffset() == timedelta(0)


def test_same_instant_same_hash() -> None:
    moscow = timezone(timedelta(hours=3))
    utc = EventModel.model_validate({"id": 1, "happened": "2025-01-01T12:00:00Z"})
    msk = EventModel.model_validate({"id": 1, "happened": datetime(2025, 1, 1, 15, tzinfo=moscow)})

    assert utc.pump_hash__ == msk.pump_hash__


def test_extra_fields_affect_hash() -> None:
    first = EventModel.model_validate({"id": 1, "x": "v1"})
    second = EventModel.model_validate({"id": 1, "x": "v2"})
    plain = EventModel.model_validate({"id": 1})

    assert first.pump_extra__ == {"x": "v1"}
    assert first.pump_hash__ != second.pump_hash__
    assert first.pump_hash__ != plain.pump_hash__
    assert first.pump_hash__ == EventModel.model_validate({"id": 1, "x": "v1"}).pump_hash__


def test_equivalent_extra_mapping_order_has_same_hash() -> None:
    first = EventModel.model_validate({"id": 1, "x": {"a": 1, "b": {"c": 2, "d": [3, 4]}}})
    second = EventModel.model_validate({"id": 1, "x": {"b": {"d": [3, 4], "c": 2}, "a": 1}})
    reordered_list = EventModel.model_validate({"id": 1, "x": {"a": 1, "b": {"c": 2, "d": [4, 3]}}})

    assert first.pump_hash__ == second.pump_hash__
    assert first.pump_hash__ != reordered_list.pump_hash__


def test_naive_datetime_kept_naive() -> None:
    event = EventModel.model_validate({"id": 1, "local": datetime(2025, 1, 1, 12)})  # noqa: DTZ001

    assert event.local == datetime(2025, 1, 1, 12)  # noqa: DTZ001
    assert event.local.tzinfo is None


def test_pump_modified_is_aware() -> None:
    event = EventModel.model_validate({"id": 1})

    assert event.pump_modified__.utcoffset() == timedelta(0)


@pytest.mark.asyncio
async def test_datetime_roundtrip() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine) as session:
        moscow = timezone(timedelta(hours=3))
        happened = datetime(2025, 1, 1, 15, tzinfo=moscow)
        local = datetime(2025, 1, 1, 12)  # noqa: DTZ001
        session.add(EventModel.model_validate({"id": 1, "happened": happened, "local": local}))
        await session.commit()
        session.expunge_all()

        event = (await session.exec(select(EventModel).where(EventModel.happened == happened))).one()
        assert event.happened == happened
        assert event.happened.utcoffset() == timedelta(0)
        assert event.local == local
        assert event.local.tzinfo is None
        assert event.pump_modified__.utcoffset() == timedelta(0)
    await engine.dispose()


@pytest.mark.asyncio
async def test_naive_datetime_rejected_for_aware_column() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine) as session:
        session.add(EventModel.model_validate({"id": 1, "happened": datetime(2025, 1, 1, 12)}))  # noqa: DTZ001
        with pytest.raises(StatementError, match="timezone information"):
            await session.commit()
    await engine.dispose()
