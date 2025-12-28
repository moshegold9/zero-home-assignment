-- Iceberg maintenance (through Trino)
-- Ingestion table
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP} EXECUTE optimize;
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP} EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP} EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Transformation table
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATS} EXECUTE optimize;
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATS} EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATS} EXECUTE remove_orphan_files(retention_threshold => '7d');


