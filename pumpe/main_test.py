import signal
import subprocess
import sys
from textwrap import dedent

from pumpe.health_test import free_port

# start_pump() listens for signals, which is only possible in the main thread, so each case runs in a child process.
PRELUDE = """
from contextlib import contextmanager

import anyio

import pumpe.main


class NoHealthServer:
    def __init__(self, *args, **kwargs):
        pass

    @contextmanager
    def in_background(self):
        yield


pumpe.main.HealthServer = NoHealthServer
"""


def start_child(body: str, prelude: str = PRELUDE) -> subprocess.Popen[str]:
    code = prelude + dedent(body)
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


def test_start_pump_uses_given_health_port() -> None:
    port = free_port()
    child = start_child(
        f"""
        import httpx

        async def task():
            async with httpx.AsyncClient() as client:
                response = await client.get("http://127.0.0.1:{port}/health")
            print(response.status_code)

        pumpe.main.start_pump(task, health_host="127.0.0.1", health_port={port})
    """,
        prelude="import pumpe.main\n",
    )
    stdout, stderr = child.communicate(timeout=10)

    assert child.returncode == 0, stderr
    # Uvicorn's access log shares stdout, so the task's output is the last line.
    assert stdout.splitlines()[-1] == "204"


def test_start_pump_without_health_server() -> None:
    child = start_child("""
        class RefusedHealthServer:
            def __init__(self, *args, **kwargs):
                raise AssertionError("health server constructed")

        pumpe.main.HealthServer = RefusedHealthServer

        async def task():
            pass

        pumpe.main.start_pump(task, health_port=None)
        print("returned")
    """)
    stdout, stderr = child.communicate(timeout=10)

    assert child.returncode == 0, stderr
    assert stdout.strip() == "returned"
