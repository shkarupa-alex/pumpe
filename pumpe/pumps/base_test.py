from asyncio import sleep
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anyio
import pytest
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpLock, PumpMeta, PumpMode
from pumpe.pumps.base import BasePump, lock_contended
from pumpe.pumps.model_test import (
    ROW_1,
    Gate,
    RecordModel,
    file_sessions,
    lease_owner,
    memory_session,
    scripted_pump,
)


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


class TransactionProbeTaskPump(BasePump):
    in_transaction: list[bool]

    async def _fetch(
        self,
        modified_since: datetime | None,  # noqa: ARG002
        created_after: datetime | None,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any]]:
        for i in range(3):
            self.in_transaction.append(self.session.in_transaction())
            yield {"i": i}
        self.in_transaction.append(self.session.in_transaction())


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_on_commit", [True, False])
async def test_fetch_starts_outside_a_transaction(*, expire_on_commit: bool) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=expire_on_commit) as session:
        pump = TransactionProbeTaskPump(session, timedelta(hours=1), timedelta(0), timedelta(0), batch_size=2)
        for mode in (PumpMode.FULL, PumpMode.PARTIAL):
            pump.in_transaction = []
            meta = await pump.run()
            assert meta is not None
            assert meta.mode == mode
            assert pump.in_transaction == [False] * 4
    await engine.dispose()


class DriverError(Exception):
    def __init__(self, *args: object, **attributes: object) -> None:
        super().__init__(*args)
        for name, value in attributes.items():
            setattr(self, name, value)


# Shaped like each driver's errors: MySQL drivers also set a generic sqlstate, asyncpg's reach SQLAlchemy as DBAPIError.
LOCK_ERRORS = [
    pytest.param(OperationalError, DriverError("database is locked", sqlite_errorcode=5), id="sqlite-busy"),
    pytest.param(OperationalError, DriverError("database table is locked", sqlite_errorcode=262), id="sqlite-locked"),
    pytest.param(OperationalError, DriverError(1205, "Lock wait timeout exceeded", sqlstate="HY000"), id="mysql-wait"),
    pytest.param(OperationalError, DriverError(1213, "Deadlock found", sqlstate="40001"), id="mysql-deadlock"),
    pytest.param(OperationalError, DriverError(1205, "Lock wait timeout exceeded"), id="mysqlclient-wait"),
    pytest.param(DBAPIError, DriverError("could not obtain lock", sqlstate="55P03"), id="asyncpg-wait"),
    pytest.param(OperationalError, DriverError("could not obtain lock", sqlstate="55P03"), id="psycopg-wait"),
    pytest.param(OperationalError, DriverError("deadlock detected", pgcode="40P01"), id="psycopg2-deadlock"),
]
OTHER_ERRORS = [
    pytest.param(OperationalError, DriverError("disk I/O error", sqlite_errorcode=10), id="sqlite-io"),
    pytest.param(OperationalError, DriverError(2013, "Lost connection", sqlstate="HY000"), id="mysql-lost"),
    pytest.param(DBAPIError, DriverError("terminating connection", sqlstate="57P01"), id="asyncpg-terminated"),
    pytest.param(OperationalError, DriverError("injected failure"), id="no-code"),
]


@pytest.mark.parametrize(("error", "orig"), LOCK_ERRORS)
def test_lock_contended(error: type[DBAPIError], orig: Exception) -> None:
    assert lock_contended(error("UPDATE pump_lock", None, orig))


@pytest.mark.parametrize(("error", "orig"), OTHER_ERRORS)
def test_lock_not_contended(error: type[DBAPIError], orig: Exception) -> None:
    assert not lock_contended(error("UPDATE pump_lock", None, orig))


def fail_takeover(session: AsyncSession, error: DBAPIError) -> None:
    """Fail the next lease takeover once with the given error."""
    pending = [error]

    def hook(  # noqa: PLR0913, PLR0917
        conn: object,  # noqa: ARG001
        cursor: object,  # noqa: ARG001
        statement: str,
        parameters: object,
        context: object,  # noqa: ARG001
        executemany: bool,  # noqa: ARG001, FBT001
    ) -> None:
        taking = isinstance(parameters, tuple) and parameters[:1] != (None,)
        if pending and taking and statement.startswith("UPDATE pump_lock SET owner=?, expires=? WHERE"):
            raise pending.pop()

    assert session.bind is not None
    event.listen(session.bind.sync_engine, "before_cursor_execute", hook)


@pytest.fixture
def row_locks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let an in-memory SQLite session stand for a database that locks single rows."""
    monkeypatch.setattr(BasePump, "_locks_rows", property(lambda _: True))


async def expired_foreign_lease(session: AsyncSession) -> None:
    session.add(PumpLock(pump=RecordModel.__name__, owner="other", expires=datetime.now(UTC) - timedelta(hours=1)))
    await session.commit()


@pytest.mark.asyncio
@pytest.mark.usefixtures("row_locks")
@pytest.mark.parametrize(("error", "orig"), LOCK_ERRORS)
async def test_contended_takeover_skips(
    error: type[DBAPIError],
    orig: Exception,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with memory_session() as session:
        await expired_foreign_lease(session)
        fail_takeover(session, error("UPDATE pump_lock", None, orig))
        pump = scripted_pump(session, ROW_1)

        assert await pump.run() is None
        assert not session.in_transaction()
        assert "the expired lease is still locked by its holder" in caplog.text
        assert "Could not release the lease" not in caplog.text

        meta = await pump.run()
        assert meta is not None
        assert meta.created == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures("row_locks")
@pytest.mark.parametrize(("error", "orig"), OTHER_ERRORS)
async def test_failed_takeover_raises(error: type[DBAPIError], orig: Exception) -> None:
    async with memory_session() as session:
        await expired_foreign_lease(session)
        fail_takeover(session, error("UPDATE pump_lock", None, orig))

        with pytest.raises(error):
            await scripted_pump(session, ROW_1).run()
        assert await lease_owner(session) == "other"


@pytest.mark.asyncio
@pytest.mark.parametrize(("error", "orig"), LOCK_ERRORS)
async def test_contended_takeover_raises_on_sqlite(error: type[DBAPIError], orig: Exception) -> None:
    async with memory_session() as session:
        await expired_foreign_lease(session)
        fail_takeover(session, error("UPDATE pump_lock", None, orig))

        with pytest.raises(error):
            await scripted_pump(session, ROW_1).run()
        assert await lease_owner(session) == "other"
