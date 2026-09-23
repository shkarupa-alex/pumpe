import re
from asyncio import sleep
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anyio
import pytest
from sqlalchemy import event
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pumpe.models import PumpLock, PumpMeta, PumpMode, PumpModel
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
        assert record.pump_seen__ == 1
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


FRACTIONAL_DATETIME = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}$")


def drop_stored_fractions(parameters: object) -> object:
    if isinstance(parameters, list):
        return [drop_stored_fractions(p) for p in parameters]
    if isinstance(parameters, tuple):
        return tuple(
            p[:-6] + "000000" if isinstance(p, str) and FRACTIONAL_DATETIME.match(p) else p for p in parameters
        )
    return parameters


@pytest.mark.asyncio
async def test_full_rerun_keeps_rows_with_second_precision_storage() -> None:
    async with memory_session() as session:
        # Store datetimes the way MariaDB DATETIME does, without fractional seconds; bound comparisons keep them.
        def truncate(  # noqa: PLR0913, PLR0917
            conn: object,  # noqa: ARG001
            cursor: object,  # noqa: ARG001
            statement: str,
            parameters: object,
            context: object,  # noqa: ARG001
            executemany: bool,  # noqa: ARG001, FBT001
        ) -> tuple[str, object]:
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE")):
                parameters = drop_stored_fractions(parameters)
            return statement, parameters

        bind = session.bind
        assert bind is not None
        event.listen(bind.sync_engine, "before_cursor_execute", truncate, retval=True)

        pump = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))
        pump.records = tuple({"id": i, "value": i} for i in range(1, 51))
        for run in range(3):
            meta = await pump.run()
            assert meta is not None
            assert meta.mode == PumpMode.FULL
            assert meta.created == (50 if run == 0 else 0)
            assert meta.skipped == (0 if run == 0 else 50)
            assert meta.deleted == 0
            assert await record_ids(session) == set(range(1, 51))


@pytest.mark.asyncio
async def test_reordered_extra_mapping_is_skipped() -> None:
    async with memory_session() as session:
        pump = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))

        pump.records = ({"id": 1, "value": 10, "payload": {"a": 1, "b": 2}},)
        await pump.run()
        modified = (await load_record(session, 1)).pump_modified__

        pump.records = ({"id": 1, "value": 10, "payload": {"b": 2, "a": 1}},)
        meta = await pump.run()
        assert meta is not None
        assert meta.skipped == 1
        assert meta.updated == 0
        assert (await load_record(session, 1)).pump_modified__ == modified


@dataclass
class Gate:
    reached: anyio.Event = field(default_factory=anyio.Event)
    opened: anyio.Event = field(default_factory=anyio.Event)


class ScriptedRecordPump(RecordModelPump):
    script: tuple[dict[str, Any] | Gate | Exception, ...] = ()

    async def _fetch(
        self,
        modified_since: datetime | None,  # noqa: ARG002
        created_after: datetime | None,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any]]:
        for step in self.script:
            if isinstance(step, Exception):
                raise step
            if isinstance(step, Gate):
                step.reached.set()
                await step.opened.wait()
                continue
            yield step


@asynccontextmanager
async def file_sessions(path: Path) -> AsyncGenerator[tuple[AsyncSession, AsyncSession]]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{path / 'pump.sqlite'}")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with (
        AsyncSession(engine, expire_on_commit=False) as first,
        AsyncSession(engine, expire_on_commit=False) as second,
    ):
        yield first, second
    await engine.dispose()


async def seed_records(session: AsyncSession, *records: dict[str, Any]) -> None:
    seed = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))
    seed.records = records
    await seed.run()


async def record_ids(session: AsyncSession) -> set[int]:
    session.expunge_all()
    return set((await session.exec(select(RecordModel.id))).all())


def scripted_pump(session: AsyncSession, *script: dict[str, Any] | Gate | Exception) -> ScriptedRecordPump:
    pump = ScriptedRecordPump(session, timedelta(0), timedelta(0), timedelta(0), batch_size=1)
    pump.script = script
    return pump


ROW_1 = {"id": 1, "value": 10}
ROW_1_NEWER = {"id": 1, "value": 20}
ROW_2 = {"id": 2, "value": 20}


async def pump_in_background(pump: RecordModelPump, results: list[PumpMeta | BaseException | None]) -> None:
    try:
        results.append(await pump.run())
    except Exception as e:  # noqa: BLE001
        results.append(e)


@pytest.mark.asyncio
async def test_overlapping_run_is_skipped_while_lease_is_held(tmp_path: Path) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        await seed_records(session_a, ROW_1, ROW_2)

        gate = Gate()
        run_a = scripted_pump(session_a, ROW_1, gate, ROW_2)
        run_b = scripted_pump(session_b)
        results: list[PumpMeta | BaseException | None] = []

        async with anyio.create_task_group() as tg:
            tg.start_soon(pump_in_background, run_a, results)
            await gate.reached.wait()
            assert await run_b.run() is None
            gate.opened.set()

        [meta] = results
        assert isinstance(meta, PumpMeta)
        assert meta.mode == PumpMode.FULL
        assert meta.skipped == 2
        assert meta.deleted == 0
        assert await record_ids(session_a) == {1, 2}

        # The lease is released once the run ends, so the next run proceeds.
        after = await scripted_pump(session_b, ROW_1, ROW_2).run()
        assert after is not None
        assert after.skipped == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(("newer_source", "expected"), [((), {}), ((ROW_1_NEWER,), {1: 20})])
async def test_stale_run_cannot_write_after_newer_run_takes_over(
    tmp_path: Path,
    newer_source: tuple[dict[str, Any], ...],
    expected: dict[int, int],
) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        await seed_records(session_a, ROW_1)

        # An older partial run stalls past its lease before yielding a cached row.
        gate = Gate()
        stale = scripted_pump(session_b, gate, ROW_1)
        stale.full_interval = timedelta(hours=1)
        stale.lease_timeout = timedelta(0)
        results: list[PumpMeta | BaseException | None] = []

        async with anyio.create_task_group() as tg:
            tg.start_soon(pump_in_background, stale, results)
            await gate.reached.wait()

            newer = await scripted_pump(session_a, *newer_source).run()
            assert newer is not None
            assert newer.mode == PumpMode.FULL
            assert newer.deleted == 1 - len(expected)

            gate.opened.set()

        [error] = results
        assert isinstance(error, RuntimeError)
        assert "lease lost" in str(error)

        session_a.expunge_all()
        rows = (await session_a.exec(select(RecordModel))).all()
        assert {r.id: r.value for r in rows} == expected


async def lease_owner(session: AsyncSession) -> str | None:
    session.expunge_all()
    return (await session.exec(select(PumpLock.owner).where(PumpLock.pump == RecordModel.__name__))).one()


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_on_commit", [True, False])
async def test_run_with_expiring_session(*, expire_on_commit: bool) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=expire_on_commit) as session:
        pump = RecordModelPump(session, timedelta(0), timedelta(0), timedelta(0))
        pump.records = (ROW_1,)

        meta = await pump.run()
        assert meta is not None
        assert meta.id is not None
        assert meta.mode == PumpMode.FULL
        assert meta.created == 1
        assert meta.elapsed is not None
        assert await record_ids(session) == {1}
        assert await lease_owner(session) is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_cancel_during_final_commit_does_not_orphan_lease(tmp_path: Path) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        gate = Gate()
        pump = scripted_pump(session_a, ROW_1)
        commit = session_a.commit

        async def gated_commit() -> None:
            if not gate.reached.is_set() and any(isinstance(o, PumpMeta) for o in session_a.identity_map.values()):
                # Hold the last transaction open: meta and the lease release written, not yet committed.
                gate.reached.set()
                await gate.opened.wait()
            await commit()

        session_a.commit = gated_commit  # type: ignore[method-assign]

        async with anyio.create_task_group() as tg:
            tg.start_soon(pump.run)
            await gate.reached.wait()
            tg.cancel_scope.cancel()

        assert await lease_owner(session_b) is None
        after = await scripted_pump(session_b, ROW_1).run()
        assert after is not None
        assert after.mode == PumpMode.FULL


def fail_once(session: AsyncSession, prefix: str) -> None:
    failed: list[str] = []

    def hook(  # noqa: PLR0913, PLR0917
        conn: object,  # noqa: ARG001
        cursor: object,  # noqa: ARG001
        statement: str,
        parameters: object,  # noqa: ARG001
        context: object,  # noqa: ARG001
        executemany: bool,  # noqa: ARG001, FBT001
    ) -> None:
        if not failed and statement.startswith(prefix):
            failed.append(statement)
            raise OperationalError(statement, None, Exception("injected failure"))

    assert session.bind is not None
    event.listen(session.bind.sync_engine, "before_cursor_execute", hook)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_fails", [False, True])
async def test_failed_release_keeps_pump_usable(*, source_fails: bool) -> None:
    async with memory_session() as session:
        fail_once(session, "UPDATE pump_lock SET owner=?, expires=? WHERE")

        first = scripted_pump(session, ValueError("source") if source_fails else ROW_1)
        with pytest.raises(ValueError if source_fails else OperationalError):
            await first.run()
        assert not session.in_transaction()

        # The next run of the same pump takes its unreleased lease back instead of waiting for it to expire.
        first.script = (ROW_1,)
        meta = await first.run()
        assert meta is not None
        assert meta.mode == PumpMode.FULL
        assert (meta.created, meta.skipped) == ((1, 0) if source_fails else (0, 1))
        assert await lease_owner(session) is None


@pytest.mark.asyncio
async def test_lease_row_is_created_outside_the_takeover_transaction() -> None:
    async with memory_session() as session:
        log: list[str] = []
        assert session.bind is not None
        sync_engine = session.bind.sync_engine

        def statement(  # noqa: PLR0913, PLR0917
            conn: object,  # noqa: ARG001
            cursor: object,  # noqa: ARG001
            statement: str,
            parameters: object,  # noqa: ARG001
            context: object,  # noqa: ARG001
            executemany: bool,  # noqa: ARG001, FBT001
        ) -> None:
            log.append(re.split(r" \(| SET | WHERE ", statement, maxsplit=1)[0])

        event.listen(sync_engine, "before_cursor_execute", statement)
        event.listen(sync_engine, "commit", lambda _: log.append("COMMIT"))
        event.listen(sync_engine, "rollback", lambda _: log.append("ROLLBACK"))

        meta = await scripted_pump(session, ROW_1).run()
        assert meta is not None

        transactions: list[list[str]] = [[]]
        for entry in log:
            if entry in {"COMMIT", "ROLLBACK"}:
                transactions.append([])
            else:
                transactions[-1].append(entry)
        [creating] = [t for t in transactions if "INSERT INTO pump_lock" in t]
        assert "UPDATE pump_lock" not in creating


@pytest.mark.asyncio
async def test_concurrent_first_runs_one_wins(tmp_path: Path) -> None:
    async with file_sessions(tmp_path) as (session_a, session_b):
        gates = Gate(), Gate()
        first_done = anyio.Event()
        results: list[PumpMeta | BaseException | None] = []

        async def pump(session: AsyncSession, gate: Gate) -> None:
            await pump_in_background(scripted_pump(session, gate, ROW_1), results)
            first_done.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(pump, session_a, gates[0])
            tg.start_soon(pump, session_b, gates[1])

            # The loser returns while the winner still holds the lease at its gate.
            with anyio.fail_after(5):
                await first_done.wait()
            assert results == [None]
            assert sum(g.reached.is_set() for g in gates) == 1
            for gate in gates:
                gate.opened.set()

        assert sorted(type(r).__name__ for r in results) == ["NoneType", "PumpMeta"]
