-- WRITES TO BIGQUERY (DML). Idempotent MERGE of customer visits (session windows).
-- Key = (user_id, window_start). Newest pane wins, as for the other gold tables.
--
-- Extra step, only for sessions: a LATE sale can glue two visits together
-- (10:00-10:04 and 10:09-10:12 become 10:00-10:12) or start a visit earlier.
-- Beam then emits a NEW, wider session with another window_start. The older,
-- narrower rows are now wrong, so step 2 deletes any session fully covered
-- by a wider, newer session of the same customer.

DECLARE min_window_start TIMESTAMP;

CREATE TEMP TABLE src AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, window_start ORDER BY pane_index DESC, emitted_at DESC) AS rn
  FROM `${PROJECT}.${DATASET}.gold_user_sessions_staging`
  WHERE emitted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${LOOKBACK_HOURS} HOUR)
)
WHERE rn = 1;

-- One day of margin: a merged session can start before the oldest source row.
SET min_window_start = (SELECT TIMESTAMP_SUB(TIMESTAMP_TRUNC(MIN(window_start), DAY), INTERVAL 1 DAY) FROM src);

-- Step 1: upsert
MERGE `${PROJECT}.${DATASET}.gold_user_sessions` T
USING src S
ON T.user_id = S.user_id AND T.window_start = S.window_start
   AND T.window_start >= min_window_start
WHEN MATCHED AND (S.pane_index > T.pane_index
                  OR (S.pane_index = T.pane_index AND S.emitted_at > T.emitted_at)) THEN
  UPDATE SET window_end = S.window_end, purchase_count = S.purchase_count, revenue = S.revenue,
             first_purchase_time = S.first_purchase_time, last_purchase_time = S.last_purchase_time,
             visit_duration_s = S.visit_duration_s, pane_index = S.pane_index, pane_timing = S.pane_timing,
             is_final_pane = S.is_final_pane, emitted_at = S.emitted_at
WHEN NOT MATCHED THEN
  INSERT (window_start, window_end, user_id, purchase_count, revenue, first_purchase_time,
          last_purchase_time, visit_duration_s, pane_index, pane_timing, is_final_pane, emitted_at)
  VALUES (S.window_start, S.window_end, S.user_id, S.purchase_count, S.revenue, S.first_purchase_time,
          S.last_purchase_time, S.visit_duration_s, S.pane_index, S.pane_timing, S.is_final_pane, S.emitted_at);

-- Step 2: remove visits swallowed by a wider, newer visit (also idempotent)
DELETE FROM `${PROJECT}.${DATASET}.gold_user_sessions` T
WHERE T.window_start >= min_window_start
  AND EXISTS (
    SELECT 1 FROM `${PROJECT}.${DATASET}.gold_user_sessions` W
    WHERE W.user_id = T.user_id
      AND W.window_start >= min_window_start
      AND W.window_start <= T.window_start
      AND W.window_end >= T.window_end
      AND NOT (W.window_start = T.window_start AND W.window_end = T.window_end)
      AND W.emitted_at > T.emitted_at
  );
