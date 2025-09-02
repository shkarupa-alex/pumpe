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


async def main_group(pump_task: Callable[[], Awaitable[None]]) -> None:
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        tg.start_soon(pump_task)


def start_pump(pump_task: Callable[[], Awaitable[None]]) -> None:
    main_group_ = partial(main_group, pump_task)

    with HealthServer().in_background():
        anyio.run(main_group_)
