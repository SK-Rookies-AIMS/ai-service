from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="AI Service", alias="APP_NAME")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    ai_manual_api_url: str | None = Field(default=None, alias="AI_MANUAL_API_URL")
    colleague_skill_api_url: str | None = Field(
        default=None,
        alias="COLLEAGUE_SKILL_API_URL",
    )

    kafka_bootstrap_servers: str | None = Field(
        default=None,
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

