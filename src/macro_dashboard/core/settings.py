from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_project_root(start: Path | None = None) -> Path:
    """
    Walk upward from `start` (or this file) to find the project root.
    We define the project root as the directory containing pyproject.toml.
    """
    here = start or Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: repo root assumed two levels up from this file (src/macro_dashboard/...)
    return Path(__file__).resolve().parents[3]


def _load_local_config() -> dict:
    """
    Loads config.yml if it exists (project-root-relative) and normalizes nested keys.
    Used ONLY for local development as a low-priority fallback.
    """
    project_root = _find_project_root()
    config_path = project_root / "config.yml"

    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    normalized: dict = {}

    # api_keys.fred -> fred_api_key
    api_keys = raw.get("api_keys", {}) or {}
    fred_key = api_keys.get("fred")
    if fred_key:
        normalized["fred_api_key"] = fred_key

    # optional: support mongo.uri etc later if you want
    mongo = raw.get("mongo", {}) or {}
    mongo_uri = mongo.get("uri")
    if mongo_uri:
        normalized["mongo_uri"] = mongo_uri

    tracked = raw.get("tracked_series")
    if isinstance(tracked, list):
        normalized["tracked_series"] = tracked

    return normalized


class Settings(BaseSettings):
    # Pydantic Settings v2 config
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    # -------- runtime --------
    env: str = Field(default="local", alias="ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # -------- FRED --------
    fred_api_key: Optional[str] = Field(default=None, alias="FRED_API_KEY")
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    fred_timeout_seconds: int = 10
    fred_retry_count: int = 3

    # -------- Mongo --------
    mongo_uri: Optional[str] = Field(default=None, alias="MONGO_URI")
    mongo_db_name: str = "macro_dashboard"
    mongo_series_collection: str = "series_metadata"
    mongo_observations_collection: str = "observations"

    # -------- ingestion --------
    tracked_series: List[str] = Field(default_factory=list)

    @classmethod
    def from_sources(cls) -> "Settings":
        """
        Load settings with precedence:
          1) environment variables
          2) .env file (handled by pydantic-settings)
          3) config.yml (project root)
          4) defaults
        """
        yaml_fallback = _load_local_config()

        # Instantiate normally so env/.env are applied
        s = cls()

        # Fill missing values from YAML (low priority)
        # (Only apply YAML where env/.env didn't provide a value.)
        if s.fred_api_key is None and "fred_api_key" in yaml_fallback:
            s.fred_api_key = yaml_fallback["fred_api_key"]
        if s.mongo_uri is None and "mongo_uri" in yaml_fallback:
            s.mongo_uri = yaml_fallback["mongo_uri"]
        if not s.tracked_series and "tracked_series" in yaml_fallback:
            s.tracked_series = yaml_fallback["tracked_series"]

        return s


@lru_cache
def get_settings() -> Settings:
    settings = Settings.from_sources()

    # Example: enforce requirements for non-local envs
    if settings.env != "local" and not settings.mongo_uri:
        raise RuntimeError("MONGO_URI must be set")

    return settings
