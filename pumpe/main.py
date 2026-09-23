import signal
from collections.abc import Awaitable, Callable
from functools import partial

import anyio

from pumpe.health import HealthServer


async def signal_handler(scope: anyio.CancelScope) -> None:
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for _ in signals:
            scope.cancel()
            return


async def run_pump_task(pump_task: Callable[[], Awaitable[None]], scope: anyio.CancelScope) -> None:
    await pump_task()
    # Stop waiting for signals once the pump is done, so the process exits instead of reporting healthy forever.
    scope.cancel()


async def main_group(pump_task: Callable[[], Awaitable[None]]) -> None:
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        tg.start_soon(run_pump_task, pump_task, tg.cancel_scope)


def start_pump(pump_task: Callable[[], Awaitable[None]]) -> None:
    main_group_ = partial(main_group, pump_task)

    with HealthServer().in_background():
        anyio.run(main_group_)
