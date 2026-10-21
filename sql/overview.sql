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
  AVG(s.actual_intensity) AS mean_actual_gco2_per_kwh,
  COUNT(s.actual_intensity) AS available_actual_observations,
  COUNT(*) AS expected_intervals,
  COUNT(s.start_utc) AS present_intervals,
  MAX(s.start_utc) AS latest_available_interval_start_utc,
  MAX(s.end_utc) AS latest_available_interval_end_utc,
  MAX(CASE WHEN s.actual_intensity IS NOT NULL THEN s.end_utc END)
    AS latest_interval_end_with_actual_utc,
  100.0 * COUNT(s.start_utc) / NULLIF(COUNT(*), 0)
    AS interval_presence_percent,
  100.0 * COUNT(s.actual_intensity) / NULLIF(COUNT(*), 0)
    AS actual_coverage_percent
FROM grid g
LEFT JOIN silver_utc s
  ON s.start_utc = g.expected_start_utc
 AND s.end_utc = g.expected_start_utc + INTERVAL 30 MINUTES