-- WRITES TO BIGQUERY (DML). Idempotent: running it twice changes nothing the 2nd time.
-- At-least-once in, exactly-once effect out: one row per transaction_id.
--
-- 1. Take staging rows of the last ${LOOKBACK_HOURS} h (overlap between runs is fine: idempotent).
-- 2. Keep ONE row per transaction_id: the first arrival. A copy carries the same sale.
-- 3. Insert only ids the final table does not have yet.

DECLARE min_event_time TIMESTAMP;

CREATE TEMP TABLE src AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY arrival_time, processing_time) AS rn
  FROM `${PROJECT}.${DATASET}.transactions_silver_staging`
  WHERE processing_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${LOOKBACK_HOURS} HOUR)
)
WHERE rn = 1;

-- A constant lower bound lets BigQuery read only the target partitions it needs.
-- It is exact, not a guess: a copy of a sale always has the same event_time.
SET min_event_time = (SELECT TIMESTAMP_TRUNC(MIN(event_time), DAY) FROM src);

MERGE `${PROJECT}.${DATASET}.transactions_silver` T
USING src S
ON T.transaction_id = S.transaction_id
   AND T.event_time >= min_event_time
WHEN NOT MATCHED THEN
  INSERT (transaction_id, user_id, region, product_id, amount, currency, event_time,
          schema_version, message_id, arrival_time, processing_time)
  VALUES (S.transaction_id, S.user_id, S.region, S.product_id, S.amount, S.currency, S.event_time,
          S.schema_version, S.message_id, S.arrival_time, S.processing_time);
