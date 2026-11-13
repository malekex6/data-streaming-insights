"""BigQuery tables written by the pipeline (append only).

The pipeline is AT-LEAST-ONCE: on a retry or a worker restart a row can be
written twice. So Beam never writes the tables people read. It appends to
*_staging tables, and sql/merge_*.sql MERGEs them into the final tables on a
business key. Running a MERGE twice gives the same result: that is the
idempotent sink.

    Beam (append)                         MERGE key                      final table (read this)
    transactions_bronze                   none, audit + replay copy      -
    transactions_dead_letter              none, for humans to fix        -
    transactions_silver_staging           transaction_id                 transactions_silver
    gold_sales_per_minute_staging         window_start, region, product  gold_sales_per_minute
    gold_region_sales_rolling_10m_staging window_start, region           gold_region_sales_rolling_10m
    gold_user_sessions_staging            user_id, window_start          gold_user_sessions

Staging partitions expire after 7 days: once merged they are not needed.
"""

from typing import Any, Dict, List

DAY_MS = 24 * 3600 * 1000


def _schema(fields: List[tuple]) -> Dict[str, Any]:
    return {"fields": [{"name": n, "type": t, "mode": m} for n, t, m in fields]}


def _partitioned(field: str, days: int, clustering: List[str] = None) -> Dict[str, Any]:
    params = {"timePartitioning": {"type": "DAY", "field": field, "expirationMs": days * DAY_MS}}
    if clustering:
        params["clustering"] = {"fields": clustering}
    return params


_META = [
    ("message_id", "STRING", "NULLABLE"),
    ("arrival_time", "TIMESTAMP", "REQUIRED"),
    ("processing_time", "TIMESTAMP", "REQUIRED"),
]

BRONZE_SCHEMA = _schema([
    ("transaction_id", "STRING", "NULLABLE"),        # from the attribute; NULL if the producer forgot it
    ("event_time_attribute", "STRING", "NULLABLE"),
    ("raw_payload", "STRING", "REQUIRED"),
    *_META,
])
BRONZE_PARAMS = _partitioned("arrival_time", days=30)

DEAD_LETTER_SCHEMA = _schema([
    ("stage", "STRING", "REQUIRED"),
    ("error_type", "STRING", "REQUIRED"),
    ("errors", "STRING", "REQUIRED"),
    ("raw_payload", "STRING", "REQUIRED"),
    ("message_id", "STRING", "NULLABLE"),
    ("arrival_time", "TIMESTAMP", "NULLABLE"),
    ("processing_time", "TIMESTAMP", "NULLABLE"),
])
DEAD_LETTER_PARAMS = _partitioned("processing_time", days=30)

SILVER_STAGING_SCHEMA = _schema([
    ("transaction_id", "STRING", "REQUIRED"),
    ("user_id", "STRING", "REQUIRED"),
    ("region", "STRING", "REQUIRED"),
    ("product_id", "STRING", "REQUIRED"),
    ("amount", "FLOAT64", "REQUIRED"),
    ("currency", "STRING", "REQUIRED"),
    ("event_time", "TIMESTAMP", "REQUIRED"),
    ("schema_version", "INT64", "REQUIRED"),
    *_META,
])
SILVER_STAGING_PARAMS = _partitioned("processing_time", days=7)

_PANE = [
    ("pane_index", "INT64", "REQUIRED"),
    ("pane_timing", "STRING", "REQUIRED"),   # EARLY / ON_TIME / LATE
    ("is_final_pane", "BOOL", "REQUIRED"),
    ("emitted_at", "TIMESTAMP", "REQUIRED"),
]
_SALES_STATS = [
    ("total_sales", "FLOAT64", "REQUIRED"),
    ("transaction_count", "INT64", "REQUIRED"),
    ("avg_amount", "FLOAT64", "REQUIRED"),
    ("avg_arrival_latency_s", "FLOAT64", "REQUIRED"),
    ("max_arrival_latency_s", "FLOAT64", "REQUIRED"),
]

SALES_PER_MINUTE_SCHEMA = _schema([
    ("window_start", "TIMESTAMP", "REQUIRED"),
    ("window_end", "TIMESTAMP", "REQUIRED"),
    ("region", "STRING", "REQUIRED"),
    ("product_id", "STRING", "REQUIRED"),
    *_SALES_STATS,
    *_PANE,
])

REGION_ROLLING_SCHEMA = _schema([
    ("window_start", "TIMESTAMP", "REQUIRED"),
    ("window_end", "TIMESTAMP", "REQUIRED"),
    ("region", "STRING", "REQUIRED"),
    *_SALES_STATS,
    *_PANE,
])

USER_SESSIONS_SCHEMA = _schema([
    ("window_start", "TIMESTAMP", "REQUIRED"),   # first purchase
    ("window_end", "TIMESTAMP", "REQUIRED"),     # last purchase + 5 min gap
    ("user_id", "STRING", "REQUIRED"),
    ("purchase_count", "INT64", "REQUIRED"),
    ("revenue", "FLOAT64", "REQUIRED"),
    ("first_purchase_time", "TIMESTAMP", "REQUIRED"),
    ("last_purchase_time", "TIMESTAMP", "REQUIRED"),
    ("visit_duration_s", "FLOAT64", "REQUIRED"),
    *_PANE,
])

GOLD_STAGING_PARAMS = _partitioned("emitted_at", days=7)
