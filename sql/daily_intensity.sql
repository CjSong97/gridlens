WITH bounds AS (
  SELECT
    GREATEST(
      TIMESTAMP_NTZ '2026-07-13 00:00:00',
      CAST(COALESCE(
        CAST(:reporting_dates.min AS DATE), DATE '2026-07-13'
      ) AS TIMESTAMP_NTZ)
    ) AS lower_utc,
    LEAST(
      TIMESTAMP_NTZ '2026-10-11 00:00:00',
      CAST(DATE_ADD(COALESCE(
        CAST(:reporting_dates.max AS DATE), DATE '2026-10-10'
      ), 1) AS TIMESTAMP_NTZ)
    ) AS upper_utc
),
grid AS (
  SELECT EXPLODE(
    CASE WHEN upper_utc > lower_utc THEN
      SEQUENCE(lower_utc, upper_utc - INTERVAL 30 MINUTES,
               INTERVAL 30 MINUTES)
    ELSE CAST(ARRAY() AS ARRAY<TIMESTAMP_NTZ>) END
  ) AS expected_start_utc
  FROM bounds
),
silver_utc AS (
  SELECT
    CONVERT_TIMEZONE('UTC', interval_start_utc) AS start_utc,
    CONVERT_TIMEZONE('UTC', interval_end_utc) AS end_utc,
    actual_intensity
  FROM workspace.silver.national_intensity
)
SELECT
  CAST(g.expected_start_utc AS DATE) AS reporting_date_utc,
  COUNT(*) AS expected_intervals,
  COUNT(s.start_utc) AS present_intervals,
  COUNT(s.actual_intensity) AS actual_observations,
  COUNT(*) - COUNT(s.start_utc) AS missing_intervals,
  COUNT(s.start_utc) - COUNT(s.actual_intensity)
    AS present_without_actual,
  AVG(s.actual_intensity) AS mean_actual_gco2_per_kwh,
  MIN(s.actual_intensity) AS min_actual_gco2_per_kwh,
  MAX(s.actual_intensity) AS max_actual_gco2_per_kwh,
  100.0 * COUNT(s.start_utc) / COUNT(*)
    AS interval_presence_percent,
  100.0 * COUNT(s.actual_intensity) / COUNT(*)
    AS actual_coverage_percent,
  CASE
    WHEN COUNT(*) < 48 THEN 'Partial publication day'
    WHEN COUNT(s.start_utc) < COUNT(*) THEN 'Missing intervals'
    WHEN COUNT(s.actual_intensity) < COUNT(*) THEN 'Missing actual values'
    ELSE 'Complete'
  END AS coverage_status
FROM grid g
LEFT JOIN silver_utc s
  ON s.start_utc = g.expected_start_utc
 AND s.end_utc = g.expected_start_utc + INTERVAL 30 MINUTES
GROUP BY CAST(g.expected_start_utc AS DATE)
ORDER BY reporting_date_utc