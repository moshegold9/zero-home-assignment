-- Prove the transformation table matches a full recompute from the ingestion table.
-- If this returns 0 rows, the transformed aggregate is valid.

WITH recompute AS (
  SELECT
    station,
    COUNT(*) AS total_observations,
    SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) AS missing_observations,
    SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS missing_pct
  FROM ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_PRECIP}
  GROUP BY station
),
diff AS (
  SELECT
    COALESCE(r.station, s.station) AS station,
    r.total_observations AS recompute_total,
    s.total_observations AS stats_total,
    r.missing_observations AS recompute_missing,
    s.missing_observations AS stats_missing,
    r.missing_pct AS recompute_pct,
    s.missing_pct AS stats_pct
  FROM recompute r
  FULL OUTER JOIN ${ICEBERG_CATALOG}.${ICEBERG_SCHEMA}.${ICEBERG_TABLE_STATS} s
    ON r.station = s.station
)
SELECT *
FROM diff
WHERE
  recompute_total IS DISTINCT FROM stats_total
  OR recompute_missing IS DISTINCT FROM stats_missing
  OR ABS(recompute_pct - stats_pct) > 1e-9
LIMIT 50;


