from asyncio import sleep
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any

import pytest
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

    @property
    def _model(self) -> type[PumpModel]:
        return CustomModel

    async def _fetch(
        self,
        modified_since: datetime | None,
        created_after: datetime | None,
    ) -> AsyncGenerator[dict[str, Any]]:
        assert isinstance(modified_since, datetime) or modified_since is None
        assert isinstance(created_after, datetime) or created_after is None

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
        assert full.skipped == 0
        assert full.created == 100
        assert full.updated == 0
        assert full.deleted == 0

        query = select(CustomModel).order_by(CustomModel.source).limit(1)
        record = (await session.exec(query)).first()
        assert isinstance(record, CustomModel)
        assert len(record.pump_hash__) == 64
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
