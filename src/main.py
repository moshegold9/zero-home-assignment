from noaa_client import NOAAClient


def init():
    """
    This function is used to initialise the datalake schema before running the pipelines.
    :return:
    """


def ingest():
    """
    This function is used to ingest data from the NOAA NCEI API (paginated via offset/limit).
    :return:
    """
    client = NOAAClient.from_env()
    return client.fetch_all_as_df()


def transform():
    """
    This function is used to transform the data from the iceberg table populated by the ingest method.
    :return:
    """
    pass


def maintain():
    """
    This function is used to maintain all data in the iceberg schema.
    :return:
    """
    pass


def main():
    init()
    ingest()
    transform()
    maintain()


if __name__ == '__main__':
    main()
