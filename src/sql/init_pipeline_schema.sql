-- Create schema for NOAA precipitation data pipeline
CREATE SCHEMA IF NOT EXISTS iceberg.noaa;

-- Ingestion input table: precipitation data from NOAA NCEI API
CREATE TABLE IF NOT EXISTS iceberg.noaa.precip_15 (
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
CREATE TABLE IF NOT EXISTS iceberg.noaa.station_missing_stats (
  station VARCHAR,
  total_observations BIGINT,
  missing_observations BIGINT,
  missing_pct DOUBLE
)
WITH (
  format = 'PARQUET'
);

-- Pipeline state table for incremental processing (one row per pipeline)
CREATE TABLE IF NOT EXISTS iceberg.noaa.pipeline_state (
  pipeline_name VARCHAR,
  last_ingestion_timestamp_ms BIGINT
)
WITH (
  format = 'PARQUET'
);


