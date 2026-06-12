from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """환경 변수와 .env 파일에서 애플리케이션 설정을 로드한다."""

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

    redis_url: str | None = Field(default=None, alias="REDIS_URL")
    redis_key_prefix: str = Field(default="aims:ai-service", alias="REDIS_KEY_PREFIX")
    redis_cache_ttl_seconds: int = Field(default=300, alias="REDIS_CACHE_TTL_SECONDS")

    main_database_url: str | None = Field(default=None, alias="MAIN_DATABASE_URL")
    sample_database_url: str | None = Field(default=None, alias="SAMPLE_DATABASE_URL")

    @property
    def bottleneck_database_url(self) -> str:
        """병목 분석 결과를 저장할 maindb MySQL URL을 반환한다."""
        if self.main_database_url:
            return self.main_database_url

        raise ValueError("maindb MySQL 설정이 필요합니다: MAIN_DATABASE_URL")

    @property
    def sample_database_connection_url(self) -> str | None:
        """sampledb 설정이 있으면 MySQL URL을 반환한다."""
        if self.sample_database_url:
            return self.sample_database_url

        return None

    @property
    def redis_connection_url(self) -> str:
        """캐시 클라이언트가 사용할 Redis URL을 반환한다."""
        if self.redis_url:
            return self.redis_url

        raise ValueError("Redis 설정이 필요합니다: REDIS_URL")

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug_value(cls, value: object) -> object:
        """dev/prod 같은 환경 이름을 debug 여부로 변환한다."""
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"release", "prod", "production"}:
                return False
            if normalized in {"dev", "develop", "development", "debug"}:
                return True
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """프로세스마다 설정 객체를 한 번 생성해 재사용한다."""
    return Settings()


settings = get_settings()
