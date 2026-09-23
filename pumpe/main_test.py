import signal
import subprocess
import sys
from textwrap import dedent

# start_pump() listens for signals, which is only possible in the main thread, so each case runs in a child process.
PRELUDE = """
from contextlib import contextmanager

import anyio

import pumpe.main


class NoHealthServer:
    @contextmanager
    def in_background(self):
        yield


pumpe.main.HealthServer = NoHealthServer
"""


def start_child(body: str) -> subprocess.Popen[str]:
    code = PRELUDE + dedent(body)
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_start_pump_returns_when_task_finishes() -> None:
    child = start_child("""
        async def task():
            pass

        pumpe.main.start_pump(task)
        print("returned")
    """)
    stdout, stderr = child.communicate(timeout=10)

    assert child.returncode == 0, stderr
    assert stdout.strip() == "returned"


def test_start_pump_propagates_task_error() -> None:
    child = start_child("""
        async def task():
            raise ValueError("pump failed")

        pumpe.main.start_pump(task)
    """)
    _, stderr = child.communicate(timeout=10)

    assert child.returncode != 0
    assert "ValueError: pump failed" in stderr


def test_signal_cancels_active_task() -> None:
    child = start_child("""
        async def task():
            print("started", flush=True)
            await anyio.sleep_forever()

        pumpe.main.start_pump(task)
        print("returned")
    """)
    assert child.stdout is not None
    assert child.stdout.readline().strip() == "started"

    child.send_signal(signal.SIGTERM)
    stdout, stderr = child.communicate(timeout=10)

    assert child.returncode == 0, stderr
    assert stdout.strip() == "returned"
