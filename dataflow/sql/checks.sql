-- READ-ONLY checks. Replace ${PROJECT} / ${DATASET}, or run:
--   python -m dataflow.merge_job --project P --checks
-- Each query filters on the partition column: dry-run first if tables are big.

-- 1. At-least-once evidence: copies that reached staging (expect >= 0, often > 0)
SELECT COUNT(*) AS staging_rows, COUNT(DISTINCT transaction_id) AS distinct_sales,
       COUNT(*) - COUNT(DISTINCT transaction_id) AS copies_in_staging
FROM `${PROJECT}.${DATASET}.transactions_silver_staging`
WHERE processing_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

-- 2. Idempotent sink proof: copies in the final table (expect 0)
SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS copies_in_silver
FROM `${PROJECT}.${DATASET}.transactions_silver`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

-- 3. Average arrival latency and lateness coverage over the last day
SELECT
  COUNT(*) AS sales,
  ROUND(AVG(TIMESTAMP_DIFF(arrival_time, event_time, MILLISECOND)) / 1000, 2) AS avg_arrival_latency_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(arrival_time, event_time, SECOND), 100)[OFFSET(99)] AS p99_s,
  ROUND(100 * COUNTIF(TIMESTAMP_DIFF(arrival_time, event_time, SECOND) > 600) / COUNT(*), 2)
    AS pct_later_than_allowed_lateness
FROM `${PROJECT}.${DATASET}.transactions_silver`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

-- 4. How many firings per window kind (EARLY / ON_TIME / LATE)
SELECT pane_timing, COUNT(*) AS panes
FROM `${PROJECT}.${DATASET}.gold_sales_per_minute_staging`
WHERE emitted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY 1 ORDER BY 1;

-- 5. Gold vs silver for yesterday: the gap = sales too late for gold (still in silver)
WITH g AS (
  SELECT SUM(total_sales) AS gold_total, SUM(transaction_count) AS gold_count
  FROM `${PROJECT}.${DATASET}.gold_sales_per_minute`
  WHERE window_start >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND window_start <  TIMESTAMP(CURRENT_DATE())
), s AS (
  SELECT SUM(amount) AS silver_total, COUNT(*) AS silver_count
  FROM `${PROJECT}.${DATASET}.transactions_silver`
  WHERE event_time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_time <  TIMESTAMP(CURRENT_DATE())
)
SELECT gold_count, silver_count, silver_count - gold_count AS sales_missing_from_gold,
       ROUND(silver_total - gold_total, 2) AS amount_missing_from_gold
FROM g, s;
