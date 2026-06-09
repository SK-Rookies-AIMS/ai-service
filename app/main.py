from fastapi import FastAPI

from app.routers import health


def create_app() -> FastAPI:
    app = FastAPI(
        title="AI Service",
        version="0.1.0",
    )

    app.include_router(health.router)

    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"message": "AI Service is running"}

    return app


app = create_app()

