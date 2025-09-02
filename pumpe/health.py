from collections.abc import Generator
from contextlib import contextmanager
from threading import Thread
from time import sleep

from fastapi import FastAPI, Response, status
from uvicorn import Config, Server


class HealthServer(Server):
    def __init__(self) -> None:
        app = FastAPI()

        @app.get("/health", status_code=status.HTTP_204_NO_CONTENT)
        async def health() -> Response:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        config = Config(app, host="0.0.0.0", port=8000)

        super().__init__(config)

    @contextmanager
    def in_background(self) -> Generator[None]:
        thread = Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()
