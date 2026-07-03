import multiprocessing
import uvicorn

from app.worker_main import run_worker
from app.main import app


def run_api():
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )


if __name__ == "__main__":

    worker = multiprocessing.Process(
        target=run_worker
    )

    api = multiprocessing.Process(
        target=run_api
    )

    worker.start()
    api.start()

    worker.join()
    api.join()