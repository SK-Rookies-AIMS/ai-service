from fastapi import FastAPI

from app.api.router import api_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="AI Service",
        version="0.1.0",
    )

    app.include_router(api_router)

    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"message": "AI Service is running"}

    return app


app = create_app()
