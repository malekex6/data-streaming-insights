-- WRITES TO BIGQUERY (DML). Idempotent MERGE of window results.
-- Key = (window_start, region, product_id): one row per window and key.
-- Beam fires the same window several times (EARLY, ON_TIME, LATE panes). Panes are
-- ACCUMULATING, so each one holds the full total so far: the newest pane wins.
-- pane_index is the version. "S.pane_index > T.pane_index" means an older or a
-- replayed pane can never overwrite a newer one, and a second run changes nothing.

DECLARE min_window_start TIMESTAMP;

CREATE TEMP TABLE src AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, region, product_id ORDER BY pane_index DESC, emitted_at DESC) AS rn
  FROM `${PROJECT}.${DATASET}.gold_sales_per_minute_staging`
  WHERE emitted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${LOOKBACK_HOURS} HOUR)
)
WHERE rn = 1;

SET min_window_start = (SELECT TIMESTAMP_TRUNC(MIN(window_start), DAY) FROM src);

MERGE `${PROJECT}.${DATASET}.gold_sales_per_minute` T
USING src S
ON T.window_start = S.window_start AND T.region = S.region AND T.product_id = S.product_id
   AND T.window_start >= min_window_start
WHEN MATCHED AND (S.pane_index > T.pane_index
                  OR (S.pane_index = T.pane_index AND S.emitted_at > T.emitted_at)) THEN
  UPDATE SET total_sales = S.total_sales, transaction_count = S.transaction_count,
             avg_amount = S.avg_amount, avg_arrival_latency_s = S.avg_arrival_latency_s,
             max_arrival_latency_s = S.max_arrival_latency_s, window_end = S.window_end,
             pane_index = S.pane_index, pane_timing = S.pane_timing,
             is_final_pane = S.is_final_pane, emitted_at = S.emitted_at
WHEN NOT MATCHED THEN
  INSERT (window_start, window_end, region, product_id, total_sales, transaction_count, avg_amount,
          avg_arrival_latency_s, max_arrival_latency_s, pane_index, pane_timing, is_final_pane, emitted_at)
  VALUES (S.window_start, S.window_end, S.region, S.product_id, S.total_sales, S.transaction_count, S.avg_amount,
          S.avg_arrival_latency_s, S.max_arrival_latency_s, S.pane_index, S.pane_timing, S.is_final_pane, S.emitted_at);
