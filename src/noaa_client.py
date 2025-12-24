import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


def load_dotenv(dotenv_path: str = ".env", override: bool = False) -> None:
    """
    Minimal .env loader (no external dependencies).
    - Supports KEY=VALUE lines, optional quotes, and comments starting with '#'.
    - If override=False, existing os.environ keys are not overwritten.
    """
    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if not key:
                continue

            if not override and key in os.environ:
                continue
            os.environ[key] = value


def default_dotenv_path() -> str:
    """
    Prefer a local .env if present, otherwise fall back to config.env (checked-in).
    """
    if os.path.exists(".env"):
        return ".env"
    return "config.env"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _optional_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return value


@dataclass(frozen=True)
class NOAAConfig:
    url: str
    token: str
    datasetid: str
    startdate: str
    enddate: str
    offset: int = 1
    limit: int = 1000
    units: Optional[str] = None
    sortfield: Optional[str] = None


class NOAAClient:
    def __init__(self, config: NOAAConfig, session: Optional[requests.Session] = None) -> None:
        self._config = config
        self._session = session or requests.Session()

    @classmethod
    def from_env(cls, dotenv_path: Optional[str] = None) -> "NOAAClient":
        dotenv_path = dotenv_path or os.getenv("DOTENV_PATH") or default_dotenv_path()
        load_dotenv(dotenv_path=dotenv_path, override=False)

        cfg = NOAAConfig(
            url=_require_env("NOAA_API_URL"),
            token=_require_env("NOAA_API_TOKEN"),
            datasetid=_require_env("NOAA_DATASETID"),
            startdate=_require_env("NOAA_STARTDATE"),
            enddate=_require_env("NOAA_ENDDATE"),
            offset=int(os.getenv("NOAA_OFFSET", "1")),
            limit=int(os.getenv("NOAA_LIMIT", "1000")),
            units=_optional_env("NOAA_UNITS"),
            sortfield=_optional_env("NOAA_SORTFIELD"),
        )
        return cls(cfg)

    def fetch_all_as_df(self) -> pd.DataFrame:
        params: Dict[str, Any] = {
            "datasetid": self._config.datasetid,
            "startdate": self._config.startdate,
            "enddate": self._config.enddate,
            "limit": self._config.limit,
        }
        if self._config.units is not None:
            params["units"] = self._config.units
        if self._config.sortfield is not None:
            params["sortfield"] = self._config.sortfield

        headers = {"token": self._config.token}

        offset = self._config.offset
        limit = self._config.limit
        total_count: Optional[int] = None
        all_results: List[Dict[str, Any]] = []

        while True:
            params["offset"] = offset
            resp = self._session.get(self._config.url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            resultset = (data.get("metadata") or {}).get("resultset") or {}
            if "count" in resultset and resultset["count"] is not None:
                total_count = int(resultset["count"])

            batch = data.get("results") or []
            all_results.extend(batch)

            if total_count is None:
                break
            if not batch:
                break

            offset += limit
            if offset > total_count:
                break

        return pd.DataFrame(all_results)


