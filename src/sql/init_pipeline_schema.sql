-- Create schema for NOAA precipitation data pipeline
CREATE SCHEMA IF NOT EXISTS ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA};

-- Ingestion input table: precipitation data from NOAA NCEI API
CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP} (
  station VARCHAR,
  datatype VARCHAR,
  date BIGINT, -- epoch milliseconds (UTC)
  attributes ARRAY(VARCHAR),
  value BIGINT,
  ingestion_timestamp_ms BIGINT -- epoch milliseconds (UTC)
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['ingestion_timestamp_ms']
);

-- Transformation output table: missing data proportion per station (incremental)
CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATS} (
  station VARCHAR,
  total_observations BIGINT,
  missing_observations BIGINT,
  missing_pct DOUBLE
)
WITH (
  format = 'PARQUET'
);

-- Pipeline state table for incremental processing (one row per pipeline)
CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATE} (
  pipeline_name VARCHAR,
  last_ingestion_timestamp_ms BIGINT
)
WITH (
  format = 'PARQUET'
);


