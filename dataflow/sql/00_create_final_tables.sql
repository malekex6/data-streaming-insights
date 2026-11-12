-- WRITES TO BIGQUERY (DDL). Run once. Creates the final tables people read.
-- Only MERGE writes into them; Beam never does.
-- ${PROJECT} and ${DATASET} are filled by dataflow/merge_job.py.

CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.transactions_silver` (
  transaction_id STRING NOT NULL,
  user_id STRING NOT NULL,
  region STRING NOT NULL,
  product_id STRING NOT NULL,
  amount FLOAT64 NOT NULL,
  currency STRING NOT NULL,
  event_time TIMESTAMP NOT NULL,      -- when the sale happened (source clock)
  schema_version INT64 NOT NULL,
  message_id STRING,
  arrival_time TIMESTAMP NOT NULL,    -- when Pub/Sub received it (platform clock)
  processing_time TIMESTAMP NOT NULL  -- when Beam handled it (worker clock)
)
PARTITION BY DATE(event_time)
CLUSTER BY region, product_id;

CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.gold_sales_per_minute` (
  window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL,
  region STRING NOT NULL, product_id STRING NOT NULL,
  total_sales FLOAT64 NOT NULL, transaction_count INT64 NOT NULL, avg_amount FLOAT64 NOT NULL,
  avg_arrival_latency_s FLOAT64 NOT NULL, max_arrival_latency_s FLOAT64 NOT NULL,
  pane_index INT64 NOT NULL, pane_timing STRING NOT NULL, is_final_pane BOOL NOT NULL, emitted_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY region, product_id;

CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.gold_region_sales_rolling_10m` (
  window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL,
  region STRING NOT NULL,
  total_sales FLOAT64 NOT NULL, transaction_count INT64 NOT NULL, avg_amount FLOAT64 NOT NULL,
  avg_arrival_latency_s FLOAT64 NOT NULL, max_arrival_latency_s FLOAT64 NOT NULL,
  pane_index INT64 NOT NULL, pane_timing STRING NOT NULL, is_final_pane BOOL NOT NULL, emitted_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY region;

CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.gold_user_sessions` (
  window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL,
  user_id STRING NOT NULL,
  purchase_count INT64 NOT NULL, revenue FLOAT64 NOT NULL,
  first_purchase_time TIMESTAMP NOT NULL, last_purchase_time TIMESTAMP NOT NULL, visit_duration_s FLOAT64 NOT NULL,
  pane_index INT64 NOT NULL, pane_timing STRING NOT NULL, is_final_pane BOOL NOT NULL, emitted_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY user_id;
