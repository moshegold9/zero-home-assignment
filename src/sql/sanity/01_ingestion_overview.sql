-- Prove ingestion is happening (row counts + time range)
SELECT
  COUNT(*) AS rows,
  MIN(from_unixtime(date / 1000.0)) AS min_observation_ts,
  MAX(from_unixtime(date / 1000.0)) AS max_observation_ts,
  MIN(from_unixtime(ingestion_timestamp_ms / 1000.0)) AS first_ingest_ts,
  MAX(from_unixtime(ingestion_timestamp_ms / 1000.0)) AS last_ingest_ts
FROM ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP};


