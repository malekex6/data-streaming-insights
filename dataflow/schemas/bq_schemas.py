
"""BigQuery table schemas for Medallion architecture."""

from typing import List, Dict, Any


# Bronze layer: Raw ingestion (append-only)
TRANSACTIONS_RAW_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "raw_payload", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "pubsub_message_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# Silver layer: Clean, deduplicated, validated
TRANSACTIONS_CLEAN_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "region", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "schema_version", "type": "INT64", "mode": "REQUIRED"},
        {"name": "is_valid", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# Gold layer: Aggregated business metrics
TRANSACTIONS_GOLD_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "region", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total_sales", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "transaction_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "avg_amount", "type": "FLOAT64", "mode": "REQUIRED"},
    ]
}

def get_table_schema_string(schema: Dict[str, Any]) -> str:
    """Convert schema dict to BigQuery schema string format."""
    fields = []
    for field in schema["fields"]:
        mode = field.get("mode", "NULLABLE")
        fields.append(f"{field['name']}:{field['type']}:{mode}")
    return ",".join(fields)
