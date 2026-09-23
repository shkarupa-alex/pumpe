from collections.abc import Generator
from contextlib import contextmanager
from threading import Thread
from time import sleep

from fastapi import FastAPI, Response, status
from uvicorn import Config, Server


class HealthServer(Server):
    # Listens on all interfaces by default so a container orchestrator can probe it.
    def __init__(self, host: str = "0.0.0.0", port: int = 8000) -> None:  # noqa: S104
        app = FastAPI()

        @app.get("/health", status_code=status.HTTP_204_NO_CONTENT)
        async def health() -> Response:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        config = Config(app, host=host, port=port)

        super().__init__(config)

        self.failure: BaseException | None = None

    def _run_in_thread(self) -> None:
        try:
            self.run()
        except BaseException as e:  # noqa: BLE001 - uvicorn reports startup failures (e.g. a busy port) via sys.exit()
            self.failure = e

    @contextmanager
    def in_background(self) -> Generator[None]:
        thread = Thread(target=self._run_in_thread)
        thread.start()
        try:
            while not self.started:
                if not thread.is_alive():
                    message = f"Health server failed to start on {self.config.host}:{self.config.port}"
                    raise RuntimeError(message) from self.failure
                sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()
