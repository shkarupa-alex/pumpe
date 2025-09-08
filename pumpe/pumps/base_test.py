from asyncio import sleep
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpMeta, PumpMode
from pumpe.pumps.base import BasePump


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
        assert isinstance(full, PumpMeta)
        assert full.mode == PumpMode.FULL
        assert full.skipped == 100
        assert full.created == 0
        assert full.updated == 0
        assert full.deleted == 0

        skip = await pump.run()
        assert skip is None

        await sleep(2)
        part = await pump.run()
        assert isinstance(part, PumpMeta)
        assert part.mode == PumpMode.PARTIAL
        assert part.skipped == 50
        assert part.created == 0
        assert part.updated == 0
        assert part.deleted == 0
