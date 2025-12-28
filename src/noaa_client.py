import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from utils.logging_config import get_logger
logger = get_logger(__name__)


@dataclass(frozen=True)
class NOAAConfig:
    url: str
    token: str
    datasetid: str
    startdate: str
    enddate: str
    offset: int = 1
    limit: int = 1000


class NOAAClient:
    def __init__(self) -> None:
        try:
            load_dotenv(override=False)

            offset_raw = os.getenv("PRECIP_INITIAL_OFFSET", "1")
            limit_raw = os.getenv("PRECIP_LIMIT", "1000")

            url = os.getenv("NOAA_API_URL", "https://www.ncei.noaa.gov/cdo-web/api/v2/data")
            token = os.getenv("NOAA_API_TOKEN")
            datasetid = os.getenv("NOAA_DATASETID", "PRECIP_15")
            startdate = os.getenv("PRECIP_STARTDATE", "2010-01-01")
            enddate = os.getenv("PRECIP_ENDDATE", "2010-03-31")

            if token is None or str(token).strip() == "":
                raise ValueError("Missing NOAA API token. pass in 'NOAA_API_TOKEN' environment variable.")

            self._config = NOAAConfig(
                url=str(url),
                token=str(token),
                datasetid=str(datasetid),
                startdate=str(startdate),
                enddate=str(enddate),
                offset=int(offset_raw),
                limit=int(limit_raw),
            )
        except Exception:
            logger.exception("Failed initializing NOAA client configuration from environment")
            raise

        self._session = requests.Session()

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Required normalizations:
        - Expand concatenated strings to arrays (NOAA `attributes`)
        - Convert timestamps to epoch-ms (`date`)
        - Add ingestion timestamp column (`ingestion_timestamp_ms`)
        """
        if df.empty:
            return df

        # 1) Expand concatenated strings to arrays
        if "attributes" in df.columns:
            s = df["attributes"].astype("string")
            df["attributes"] = s.str.split(",").where(s.notna(), None)

        # 2) Convert timestamps to ms epoch
        if "date" in df.columns:
            dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
            df["date"] = ((dt.astype("int64") // 1_000_000).astype("Int64")).where(dt.notna(), pd.NA)

        # 3) Add ingestion timestamp column (ms epoch)
        ingestion_ms = int(pd.Timestamp.now(tz="UTC").value // 1_000_000)
        df["ingestion_timestamp_ms"] = ingestion_ms

        return df

    def fetch_all_as_df(self) -> pd.DataFrame:
        logger.info(
            "Starting NOAA ingestion datasetid=%s startdate=%s enddate=%s limit=%s offset=%s",
            self._config.datasetid,
            self._config.startdate,
            self._config.enddate,
            self._config.limit,
            self._config.offset,
        )

        params: Dict[str, Any] = {
            "datasetid": self._config.datasetid,
            "startdate": self._config.startdate,
            "enddate": self._config.enddate,
            "limit": self._config.limit,
        }

        headers = {"token": self._config.token}

        offset = self._config.offset
        limit = self._config.limit
        total_count: Optional[int] = None
        all_results: List[Dict[str, Any]] = []

        try:
            while True:
                params["offset"] = offset
                logger.info("Fetching NOAA batch offset=%s limit=%s", offset, limit)

                attempt = 0
                while True:
                    try:
                        res = self._session.get(self._config.url, headers=headers, params=params, timeout=30)
                        res.raise_for_status()
                        break
                    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
                        attempt += 1
                        logger.warning(
                            "NOAA request failed (offset=%s, attempt=%s): %s",
                            offset,
                            attempt,
                            str(e),
                        )
                        if attempt >= 3:
                            logger.exception("NOAA request failed after 3 attempts (offset=%s): %s", offset, str(e))
                            raise

                try:
                    data = res.json()
                except Exception:
                    logger.exception(
                        "Failed parsing NOAA response as JSON (offset=%s). First 500 chars: %r",
                        offset,
                        (res.text or "")[:500],
                    )
                    raise

                resultset = (data.get("metadata") or {}).get("resultset") or {}
                if "count" in resultset and resultset["count"] is not None:
                    total_count = int(resultset["count"])

                batch = data.get("results") or []
                all_results.extend(batch)

                logger.info(
                    "Fetched batch_size=%s total_fetched=%s total_count=%s",
                    len(batch),
                    len(all_results),
                    total_count,
                )

                if total_count is None:
                    logger.warning("NOAA response missing metadata.resultset.count; stopping after first batch")
                    break
                if not batch:
                    logger.info("NOAA returned empty batch; stopping")
                    break

                offset += limit
                if offset > total_count:
                    # Fetched all data, stop while loop
                    break

            logger.info("Completed NOAA ingestion total_rows=%s", len(all_results))
            df = pd.DataFrame(all_results)
            logger.info("Normalizing NOAA dataframe (rows=%s, cols=%s)", df.shape[0], df.shape[1])
            df = self._normalize_df(df)
            logger.info("Normalization complete (rows=%s, cols=%s)", df.shape[0], df.shape[1])
            return df
        except requests.exceptions.RequestException:
            logger.exception(
                "HTTP error while fetching NOAA data (url=%s offset=%s limit=%s)",
                self._config.url,
                offset,
                limit,
            )
            raise
        except Exception:
            logger.exception("Unexpected error while fetching NOAA data (offset=%s)", offset)
            raise


