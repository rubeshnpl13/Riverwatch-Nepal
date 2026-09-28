from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "local"

    bipad_base_url: str = "https://bipadportal.gov.np"

    http_timeout_seconds: float = Field(default=10.0, gt=0)
    http_max_retries: int = Field(default=3, ge=0)

    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()