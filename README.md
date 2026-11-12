# Data Streaming Insights: real-time sales analytics

Streaming pipeline for an online shop (mobile app + website). A simulator produces realistic sales (customer visits, offline phones that send late, app retries that send twice). Apache Beam reads them from Pub/Sub **by event time**, writes bronze/silver/gold to BigQuery, and idempotent `MERGE` jobs build the final tables.

## Architecture

```
simulator / FastAPI ──> Pub/Sub ──> Beam (Dataflow) ──> BigQuery *_staging (append, at-least-once)
   attributes:                         │                         │
   event_time, transaction_id          │                         └─ merge_job.py: idempotent MERGE ──> final tables
                                       ├─ bronze: every message, raw
                                       ├─ dead letter: bad JSON, broken rules
                                       ├─ silver: validated + deduplicated sales
                                       └─ gold: 3 windows (watermark, 10 min allowed lateness)
```

| Concept | Where | Setting and reason |
|---|---|---|
| Event time | `dataflow/pipeline.py` `timestamp_attribute="event_time"` | windows group by when the sale happened |
| Arrival time | `dataflow/transforms/parse.py` (Pub/Sub `publish_time`) | `arrival_time - event_time` = delivery latency |
| Tumbling window | `SalesPerMinute` | 1 min per region and product: live board, sums to hours/days |
| Sliding window | `RegionSalesRolling10m` | 10 min every 1 min per region: detect a sales drop fast |
| Session window | `UserSessions` | 5 min gap per customer: one row per shopping visit |
| Watermark + triggers | `dataflow/transforms/windows.py` | early every 30 s, on time at watermark, again for each late sale |
| Allowed lateness | `ALLOWED_LATENESS_SECONDS = 600` | covers ~96 % of sales; later ones stay in silver |
| Dedup in flight | `DropDuplicateSales` (`DeduplicatePerKey`, 1 h) | copies never reach the gold sums |
| Idempotent sink | `dataflow/sql/*_merge_*.sql` | at-least-once + MERGE on a business key |

## Tables (dataset `insightstream_analytics`)

| Table | Written by | Key / grain | Read it? |
|---|---|---|---|
| `transactions_bronze` | Beam, append | one row per Pub/Sub message, raw | audit, replay |
| `transactions_dead_letter` | Beam, append | one row per rejected message | to fix sources |
| `transactions_silver_staging` | Beam, append | may hold copies | no |
| `transactions_silver` | MERGE | `transaction_id` | yes |
| `gold_sales_per_minute(_staging)` | Beam / MERGE | `window_start, region, product_id` | final: yes |
| `gold_region_sales_rolling_10m(_staging)` | Beam / MERGE | `window_start, region` | final: yes, never SUM it |
| `gold_user_sessions(_staging)` | Beam / MERGE | `user_id, window_start` | final: yes |
| `v_arrival_latency_per_minute`, `v_region_sales_drop_alert`, `v_daily_sales` | views | | yes |

Tables from the first version (`transactions_raw`, `transactions_clean`, `transactions_gold`) are no longer written. Check that nothing reads them, then drop them.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill project, topic, key path
```

```bash
# 1. Create final tables and views (writes to BigQuery, once)
python -m dataflow.merge_job --project MY_PROJECT --setup
```

```bash
# 2. Start the pipeline (DirectRunner = local, free)
python -m dataflow.pipeline --project=MY_PROJECT --input_topic=projects/MY_PROJECT/topics/MY_TOPIC
```

```bash
# 3. Stream realistic sales for 30 min (another terminal)
python -m app.simulate --minutes 30
```

```bash
# 4. Merge staging into final tables every 5 min (another terminal)
python -m dataflow.merge_job --project MY_PROJECT --every-minutes 5
```

```bash
# 5. Read-only checks: copies, latency, panes, gold vs silver
python -m dataflow.merge_job --project MY_PROJECT --checks
```

On Dataflow add `--runner=DataflowRunner --temp_location=gs://... --staging_location=gs://...`, and prefer `--input_subscription` so a restart does not lose messages.

**Local vs Dataflow.** On the DirectRunner the Pub/Sub watermark is simply the wall clock (Beam source, `transform_evaluator.py`), and `id_label` is not supported. Dataflow estimates the watermark from the event times in the backlog and drops copies by `id_label`.

## Tests

```bash
python -m pytest tests -q
```

`tests/dataflow/test_windows.py` uses `TestStream` to move the watermark by hand: on-time, late and too-late sales, sliding overlap, session merge, dedup across windows. `tests/dataflow/test_pipeline_e2e.py` runs the whole graph with fake Pub/Sub and BigQuery. The MERGE SQL is syntax-checked only; it has not been run against BigQuery in the tests.
