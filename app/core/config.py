from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def database_url_with_name(database_url: str, database_name: str | None) -> str:
    """Return database_url with its database path replaced by database_name."""
    if not database_name:
        return database_url

    scheme_separator = "://"
    scheme_index = database_url.find(scheme_separator)
    if scheme_index < 0:
        return database_url

    authority_start = scheme_index + len(scheme_separator)
    credential_end = database_url.rfind("@")
    host_start = credential_end + 1 if credential_end >= authority_start else authority_start
    suffix_candidates = [
        index
        for index in (
            database_url.find("?", host_start),
            database_url.find("#", host_start),
        )
        if index >= 0
    ]
    suffix_start = min(suffix_candidates) if suffix_candidates else len(database_url)
    path_start = database_url.find("/", host_start, suffix_start)
    prefix_end = path_start if path_start >= 0 else suffix_start

    return (
        database_url[:prefix_end]
        + "/"
        + database_name.strip("/")
        + database_url[suffix_start:]
    )


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env."""

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

    broker_url_1: str | None = Field(default=None, alias="BROKER_URL_1")
    broker_url_2: str | None = Field(default=None, alias="BROKER_URL_2")
    kafka_raw_topic: str = Field(
        default="factory.manufacturing.raw",
        alias="KAFKA_RAW_TOPIC",
    )
    kafka_analysis_topic: str = Field(
        default="factory.manufacturing.analysis",
        alias="KAFKA_ANALYSIS_TOPIC",
    )
    kafka_raw_consumer_group_id: str = Field(
        default="ai-analysis-consumer-group",
        alias="KAFKA_RAW_CONSUMER_GROUP_ID",
    )
    kafka_raw_auto_offset_reset: str = Field(
        default="earliest",
        alias="KAFKA_RAW_AUTO_OFFSET_RESET",
    )
    kafka_raw_consumer_concurrency: int = Field(
        default=2,
        alias="KAFKA_RAW_CONSUMER_CONCURRENCY",
    )
    kafka_raw_consumer_max_poll_interval_ms: int = Field(
        default=900_000,
        alias="KAFKA_RAW_CONSUMER_MAX_POLL_INTERVAL_MS",
    )
    kafka_raw_consumer_session_timeout_ms: int = Field(
        default=30_000,
        alias="KAFKA_RAW_CONSUMER_SESSION_TIMEOUT_MS",
    )
    kafka_raw_consumer_heartbeat_interval_ms: int = Field(
        default=10_000,
        alias="KAFKA_RAW_CONSUMER_HEARTBEAT_INTERVAL_MS",
    )
    kafka_raw_consumer_max_poll_records: int = Field(
        default=1,
        alias="KAFKA_RAW_CONSUMER_MAX_POLL_RECORDS",
    )
    kafka_raw_consumer_timeout_ms: int = Field(
        default=1_000,
        alias="KAFKA_RAW_CONSUMER_TIMEOUT_MS",
    )
    kafka_analysis_producer_retries: int = Field(
        default=3,
        alias="KAFKA_ANALYSIS_PRODUCER_RETRIES",
    )
    kafka_analysis_producer_linger_ms: int = Field(
        default=10,
        alias="KAFKA_ANALYSIS_PRODUCER_LINGER_MS",
    )

    redis_url: str | None = Field(default=None, alias="REDIS_URL")
    redis_key_prefix: str = Field(default="aims:ai-service", alias="REDIS_KEY_PREFIX")
    redis_cache_ttl_seconds: int = Field(default=60, alias="REDIS_CACHE_TTL_SECONDS")

    main_database_url: str | None = Field(default=None, alias="MAIN_DATABASE_URL")
    main_db_name: str | None = Field(default=None, alias="MAIN_DB_NAME")
    sample_db_name: str | None = Field(default=None, alias="SAMPLE_DB_NAME")
    manufacturing_event_scheduler_enabled: bool = Field(
        default=True,
        alias="MANUFACTURING_EVENT_SCHEDULER_ENABLED",
    )
    manufacturing_event_scheduler_events_per_day: int | None = Field(
        default=None,
        alias="MANUFACTURING_EVENT_SCHEDULER_EVENTS_PER_DAY",
    )
    manufacturing_event_template_event_count: int | None = Field(
        default=None,
        alias="MANUFACTURING_EVENT_TEMPLATE_EVENT_COUNT",
    )
    manufacturing_event_car_pool_size: int | None = Field(
        default=None,
        alias="MANUFACTURING_EVENT_CAR_POOL_SIZE",
    )
    manufacturing_event_insert_chunk_size: int = Field(
        default=1_000,
        alias="MANUFACTURING_EVENT_INSERT_CHUNK_SIZE",
    )

    @property
    def main_database_connection_url(self) -> str:
        """Return the main DB connection URL."""
        if self.main_database_url:
            return database_url_with_name(self.main_database_url, self.main_db_name)

        raise ValueError("maindb MySQL setting is required: MAIN_DATABASE_URL")

    @property
    def bottleneck_database_url(self) -> str:
        """Return the main DB URL used for bottleneck analysis results."""
        return self.main_database_connection_url

    @property
    def sample_database_connection_url(self) -> str | None:
        """Return the sample DB URL when SAMPLE_DB_NAME is configured."""
        if self.main_database_url and self.sample_db_name:
            return database_url_with_name(self.main_database_url, self.sample_db_name)

        return None

    @property
    def redis_connection_url(self) -> str:
        """Return the Redis connection URL."""
        if self.redis_url:
            return self.redis_url

        raise ValueError("Redis setting is required: REDIS_URL")

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug_value(cls, value: object) -> object:
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
    """Return the process-wide cached settings object."""
    return Settings()


settings = get_settings()
