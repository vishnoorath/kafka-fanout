"""Application settings loaded from environment variables / .env file."""
from __future__ import annotations

from typing import Annotated, List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Settings for the FastAPI app.

    Reads from environment variables and an optional `.env` file in the
    backend working directory. Override values with real env vars in
    production.
    """

    database_url: str = Field(
        default="sqlite+aiosqlite:///./kafka_fanout.db",
        description="SQLAlchemy async URL (driver: aiosqlite).",
    )
    # `NoDecode` tells pydantic-settings to pass the raw env string to
    # our field validator instead of trying to JSON-decode it first.
    # Without this, `CORS_ORIGINS=http://localhost:5173` (a single
    # origin, as a plain string) trips `json.loads` before our comma-
    # split validator ever runs.
    cors_origins: Annotated[List[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:5173"],
        description="Allowed CORS origins.",
    )
    log_level: str = Field(default="INFO", description="Application log level.")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors(cls, v: object) -> object:
        """Allow `CORS_ORIGINS=a,b,c` or a JSON list in env vars."""
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v


settings = Settings()
