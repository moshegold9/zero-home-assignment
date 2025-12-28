from noaa_client import NOAAClient
from iceberg_writer import IcebergWriter
from utils.logging_config import setup_logging, get_logger
from trino_client import TrinoClient

setup_logging()
logger = get_logger(__name__)


def init(trino: TrinoClient) -> None:
    """
    This function is used to initialise the datalake schema before running the pipelines.
    :return:
    """
    trino.execute_sql_file("src/sql/init_pipeline_schema.sql")


def ingest():
    """
    This function is used to ingest data from the NOAA NCEI API (paginated via offset/limit).
    :return:
    """
    client = NOAAClient()
    df = client.fetch_all_as_df()
    logger.info("Ingested %s rows from NOAA NCEI API", len(df))

    writer = IcebergWriter()
    s3_uri = writer.write_df(df)
    logger.info("Appended parquet file to iceberg.noaa.precip_15 from %s", s3_uri)
    return df


def transform(trino: TrinoClient) -> None:
    """
    This function is used to transform the data from the iceberg table populated by the ingest method.
    :return:
    """
    trino.execute_sql_file("src/sql/transform_station_missing_stats.sql")


def maintain():
    """
    This function is used to maintain all data in the iceberg schema.
    :return:
    """
    pass


def main():
    logger.info("Starting NOAA ETL pipeline")
    trino = TrinoClient()
    init(trino)
    ingest()
    transform(trino)
    maintain()


if __name__ == '__main__':
    main()
