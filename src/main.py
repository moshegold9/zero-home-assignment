import os

from noaa_client import NOAAClient
from iceberg_writer import IcebergWriter
from utils.logging_config import setup_logging, get_logger
from trino_client import TrinoClient

setup_logging()
logger = get_logger(__name__)

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "iceberg")
ICEBERG_SCHEMA = os.getenv("ICEBERG_SCHEMA", "noaa")
ICEBERG_TABLE_PRECIP = os.getenv("ICEBERG_TABLE_PRECIP", "precip_15")
ICEBERG_TABLE_STATS = os.getenv("ICEBERG_TABLE_STATS", "station_missing_stats")
ICEBERG_TABLE_STATE = os.getenv("ICEBERG_TABLE_STATE", "pipeline_state")
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "station_missing_stats")



def init(trino: TrinoClient) -> None:
    """
    This function is used to initialise the datalake schema before running the pipelines.
    :return:
    """
    logger.info("Initialising datalake schema")
    trino.execute_sql_file(
        os.getenv("INIT_SQL_PATH", "src/sql/init_pipeline_schema.sql"),
        variables=_sql_vars(),
    )
    logger.info("Datalake schema initialised")


def ingest():
    """
    This function is used to ingest data from the NOAA NCEI API (paginated via offset/limit).
    :return:
    """
    client = NOAAClient()
    df = client.fetch_all_as_df()
    logger.info("Ingested %s rows from NOAA NCEI API", len(df))

    writer = IcebergWriter(schema=ICEBERG_SCHEMA, table=ICEBERG_TABLE_PRECIP)
    s3_uri = writer.write_df(df)
    logger.info(f"Appended parquet file to {ICEBERG_SCHEMA}.{ICEBERG_TABLE_PRECIP} from {s3_uri}")
    return df


def transform(trino: TrinoClient) -> None:
    """
    This function is used to transform the data from the iceberg table populated by the ingest method.
    :return:
    """
    logger.info("Transforming data into station_missing_stats")
    trino.execute_sql_file(
        os.getenv("TRANSFORM_SQL_PATH", "src/sql/transform_station_missing_stats.sql"),
        variables=_sql_vars(),
    )
    logger.info("Data transformed into station_missing_stats")


def maintain(trino: TrinoClient) -> None:
    """
    This function is used to run maintenance queries on the iceberg tables.
    :return:
    """
    # Using Trino to run maintenance queries on the iceberg tables
    logger.info("Running maintenance queries on the iceberg tables")
    trino.execute_sql_file(
        os.getenv("MAINTAIN_SQL_PATH", "src/sql/maintain_iceberg.sql"),
        variables=_sql_vars(),
    )
    logger.info("Maintenance queries on the iceberg tables completed")


def _sql_vars() -> dict[str, str]:
    return {
        "ICEBERG_CATALOG": ICEBERG_CATALOG,
        "ICEBERG_SCHEMA": ICEBERG_SCHEMA,
        "ICEBERG_TABLE_PRECIP": ICEBERG_TABLE_PRECIP,
        "ICEBERG_TABLE_STATS": ICEBERG_TABLE_STATS,
        "ICEBERG_TABLE_STATE": ICEBERG_TABLE_STATE,
        "PIPELINE_NAME": PIPELINE_NAME,
    }


def main():
    logger.info("Starting NOAA ETL pipeline")
    trino = TrinoClient()
    init(trino)
    ingest()
    transform(trino)
    maintain(trino)


if __name__ == '__main__':
    main()
