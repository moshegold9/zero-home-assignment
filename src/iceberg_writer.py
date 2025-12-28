import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

from utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class IcebergConfig:
    rest_uri: str
    warehouse: str
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str = "us-east-1"
    table_identifier: str = "noaa.precip_15"
    s3_bucket: str = "warehouse"
    s3_prefix: str = "tmp/noaa/precip_15"


class IcebergWriter:
    """
    Write a pandas dataframe to a parquet file in MinIO and append it to an Iceberg table using the PyIceberg REST catalog.
    """

    def __init__(self) -> None:
        self._config = IcebergConfig(
            rest_uri=os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
            warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse"),
            s3_endpoint=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
            s3_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
            s3_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin12345"),
            s3_region=os.getenv("MINIO_REGION", "us-east-1"),
            table_identifier=os.getenv("ICEBERG_TABLE", "noaa.precip_15"),
            s3_bucket=os.getenv("MINIO_BUCKET", "warehouse"),
            s3_prefix=os.getenv("ICEBERG_STAGE_PREFIX", "tmp/noaa/precip_15"),
        )

    def write_df(self, df: pd.DataFrame) -> str:
        if df.empty:
            raise ValueError("Refusing to write empty dataframe to Iceberg")

        # Set AWS credentials for MinIO file system
        os.environ.setdefault("AWS_ACCESS_KEY_ID", self._config.s3_access_key_id)
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", self._config.s3_secret_access_key)
        os.environ.setdefault("AWS_DEFAULT_REGION", self._config.s3_region)
        os.environ.setdefault("AWS_REGION", self._config.s3_region)

        catalog = load_catalog(
            "rest",
            **{
                "uri": self._config.rest_uri,
                "warehouse": self._config.warehouse,
                "s3.endpoint": self._config.s3_endpoint,
                "s3.access-key-id": self._config.s3_access_key_id,
                "s3.secret-access-key": self._config.s3_secret_access_key,
                "s3.region": self._config.s3_region,
                "s3.path-style-access": "true",
            },
        )
        table = catalog.load_table(self._config.table_identifier)

        arrow_table = pa.Table.from_pandas(df, preserve_index=False)

        now = datetime.now(tz=timezone.utc)
        ts_ms = int(now.timestamp() * 1000)
        key = f"{self._config.s3_prefix}/ts={ts_ms}/data.parquet"
        s3_uri = f"s3://{self._config.s3_bucket}/{key}"

        fs = pafs.S3FileSystem(
            access_key=self._config.s3_access_key_id,
            secret_key=self._config.s3_secret_access_key,
            region=self._config.s3_region,
            endpoint_override=self._config.s3_endpoint,
        )

        logger.info("Writing parquet to %s", s3_uri)
        # Write parquet to MinIO using PyArrow S3 filesystem in temporary directory
        pq.write_table(arrow_table, f"{self._config.s3_bucket}/{key}", filesystem=fs)

        logger.info("Appending parquet file to Iceberg table %s", self._config.table_identifier)
        table.append(arrow_table)

        # Cleanup: remove staged parquet file
        try:
            fs.delete_file(f"{self._config.s3_bucket}/{key}")
            logger.info("Deleted staged parquet file %s", s3_uri)
        except Exception as e:
            logger.warning("Failed to delete staged parquet file %s: %s", s3_uri, str(e))

        return s3_uri


