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
),
profile AS (
  SELECT
    CASE WHEN DAYOFWEEK(g.expected_start_utc) IN (1, 7)
      THEN 'Weekend' ELSE 'Weekday'
    END AS day_type,
    HOUR(g.expected_start_utc) * 2
      + CAST(MINUTE(g.expected_start_utc) / 30 AS INT)
      AS half_hour_slot,
    COUNT(*) AS expected_intervals,
    COUNT(s.start_utc) AS present_intervals,
    COUNT(s.actual_intensity) AS actual_observations,
    AVG(s.actual_intensity) AS mean_actual_gco2_per_kwh
  FROM grid g
  LEFT JOIN silver_utc s
    ON s.start_utc = g.expected_start_utc
   AND s.end_utc = g.expected_start_utc + INTERVAL 30 MINUTES
  GROUP BY
    CASE WHEN DAYOFWEEK(g.expected_start_utc) IN (1, 7)
      THEN 'Weekend' ELSE 'Weekday'
    END,
    HOUR(g.expected_start_utc) * 2
      + CAST(MINUTE(g.expected_start_utc) / 30 AS INT)
)
SELECT
  day_type,
  half_hour_slot,
  CONCAT(
    LPAD(CAST(CAST(half_hour_slot / 2 AS INT) AS STRING), 2, '0'),
    ':',
    CASE WHEN PMOD(half_hour_slot, 2) = 0 THEN '00' ELSE '30' END
  ) AS slot_start_utc,
  mean_actual_gco2_per_kwh,
  expected_intervals,
  present_intervals,
  actual_observations,
  100.0 * present_intervals / expected_intervals
    AS interval_presence_percent,
  100.0 * actual_observations / expected_intervals
    AS actual_coverage_percent
FROM profile
ORDER BY day_type, half_hour_slot