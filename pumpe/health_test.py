import socket
from threading import Thread

import httpx
import pytest

from pumpe.health import HealthServer


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_in_background_serves_health() -> None:
    port = free_port()
    with HealthServer(host="127.0.0.1", port=port).in_background():
        response = httpx.get(f"http://127.0.0.1:{port}/health")
        assert response.status_code == httpx.codes.NO_CONTENT

    with pytest.raises(httpx.ConnectError):
        httpx.get(f"http://127.0.0.1:{port}/health")


def test_in_background_raises_when_port_busy() -> None:
    errors: list[BaseException] = []

    def enter() -> None:
        try:
            with HealthServer(host="127.0.0.1", port=busy.getsockname()[1]).in_background():
                pass
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()

        thread = Thread(target=enter, daemon=True)
        thread.start()
        thread.join(timeout=5)

    assert not thread.is_alive(), "in_background() hangs when the server cannot start"
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
