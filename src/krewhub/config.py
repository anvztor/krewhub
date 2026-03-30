from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 8420
    database_path: str = str(Path.home() / ".krewhub" / "krewhub.db")
    api_key: str = "dev-api-key"
    retention_days: int = 7
    heartbeat_timeout_seconds: int = 30

    model_config = {"env_prefix": "KREWHUB_"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
