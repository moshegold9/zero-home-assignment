-- Merge per-station deltas into the aggregate table
MERGE INTO iceberg.noaa.station_missing_stats AS target
USING (
  WITH last_state AS (
    SELECT COALESCE(MAX(last_ingestion_timestamp_ms), 0) AS last_ts
    FROM iceberg.noaa.pipeline_state
    WHERE pipeline_name = 'station_missing_stats'
  ),
  deltas AS (
    SELECT
      station,
      COUNT(*) AS total_delta,
      SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) AS missing_delta
    FROM iceberg.noaa.precip_15
    WHERE ingestion_timestamp_ms > (SELECT last_ts FROM last_state)
    GROUP BY station
  )
  SELECT * FROM deltas
) AS source
ON (target.station = source.station)
WHEN MATCHED THEN UPDATE SET
  total_observations = target.total_observations + source.total_delta,
  missing_observations = target.missing_observations + source.missing_delta,
  missing_pct = (target.missing_observations + source.missing_delta) * 1.0 / (target.total_observations + source.total_delta)
WHEN NOT MATCHED THEN INSERT (
  station,
  total_observations,
  missing_observations,
  missing_pct
) VALUES (
  source.station,
  source.total_delta,
  source.missing_delta,
  source.missing_delta * 1.0 / source.total_delta
);

-- Advance the watermark (even if there were no deltas, it stays the same)
MERGE INTO iceberg.noaa.pipeline_state AS target
USING (
  WITH last_state AS (
    SELECT COALESCE(MAX(last_ingestion_timestamp_ms), 0) AS last_ts
    FROM iceberg.noaa.pipeline_state
    WHERE pipeline_name = 'station_missing_stats'
  ),
  new_max AS (
    SELECT COALESCE(MAX(ingestion_timestamp_ms), (SELECT last_ts FROM last_state)) AS new_ts
    FROM iceberg.noaa.precip_15
    WHERE ingestion_timestamp_ms > (SELECT last_ts FROM last_state)
  )
  SELECT 'station_missing_stats' AS pipeline_name, new_ts AS last_ingestion_timestamp_ms
  FROM new_max
) AS source
ON (target.pipeline_name = source.pipeline_name)
WHEN MATCHED THEN UPDATE SET
  last_ingestion_timestamp_ms = source.last_ingestion_timestamp_ms
WHEN NOT MATCHED THEN INSERT (pipeline_name, last_ingestion_timestamp_ms)
VALUES (source.pipeline_name, source.last_ingestion_timestamp_ms);


