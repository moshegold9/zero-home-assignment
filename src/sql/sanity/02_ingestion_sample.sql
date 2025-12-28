-- Prove ingestion schema/values look correct (sample rows)
SELECT station, datatype, date, attributes, value, ingestion_timestamp_ms
FROM ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP}
ORDER BY ingestion_timestamp_ms DESC
LIMIT 20;


