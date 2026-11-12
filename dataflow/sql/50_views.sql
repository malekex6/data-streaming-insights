-- WRITES TO BIGQUERY (DDL: views only, no data copied). Run once.

-- Delivery latency, per minute of arrival and region.
--   arrival latency = arrival_time - event_time     (network, offline phone queue)
--   pipeline lag    = processing_time - arrival_time (Beam backlog)
-- Filter on arrival_minute when you query it: silver is partitioned by event day.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_arrival_latency_per_minute` AS
SELECT
  TIMESTAMP_TRUNC(arrival_time, MINUTE) AS arrival_minute,
  region,
  COUNT(*) AS sales,
  ROUND(AVG(TIMESTAMP_DIFF(arrival_time, event_time, MILLISECOND)) / 1000, 2) AS avg_arrival_latency_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(arrival_time, event_time, SECOND), 100)[OFFSET(50)] AS p50_arrival_latency_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(arrival_time, event_time, SECOND), 100)[OFFSET(95)] AS p95_arrival_latency_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(arrival_time, event_time, SECOND), 100)[OFFSET(99)] AS p99_arrival_latency_s,
  -- approximate: the exact drop rule is "arrived after watermark passed window_end + 10 min"
  COUNTIF(TIMESTAMP_DIFF(arrival_time, event_time, SECOND) > 600) AS likely_too_late_for_gold,
  ROUND(AVG(TIMESTAMP_DIFF(processing_time, arrival_time, MILLISECOND)) / 1000, 2) AS avg_pipeline_lag_s
FROM `${PROJECT}.${DATASET}.transactions_silver`
GROUP BY 1, 2;

-- Sliding window use case: alert when a region's last 10 min drop by half
-- versus the 10 min before (window_end - 10 min = the previous, non-overlapping block).
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_region_sales_drop_alert` AS
SELECT
  cur.window_end,
  cur.region,
  cur.total_sales AS last_10m_sales,
  prev.total_sales AS previous_10m_sales,
  ROUND(SAFE_DIVIDE(cur.total_sales - prev.total_sales, prev.total_sales) * 100, 1) AS change_pct,
  cur.total_sales < 0.5 * prev.total_sales AS drop_alert
FROM `${PROJECT}.${DATASET}.gold_region_sales_rolling_10m` cur
LEFT JOIN `${PROJECT}.${DATASET}.gold_region_sales_rolling_10m` prev
  ON prev.region = cur.region
 AND prev.window_end = TIMESTAMP_SUB(cur.window_end, INTERVAL 10 MINUTE);

-- Daily truth comes from silver (keeps every sale, even too late for gold).
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_daily_sales` AS
SELECT DATE(event_time) AS sale_date, region, product_id,
       ROUND(SUM(amount), 2) AS total_sales, COUNT(*) AS transaction_count
FROM `${PROJECT}.${DATASET}.transactions_silver`
GROUP BY 1, 2, 3;
