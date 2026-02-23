"""Enrich records with metadata."""

import logging
from typing import Dict, Any, Tuple
from datetime import datetime

import apache_beam as beam
from apache_beam.transforms import DoFn, ParDo
from apache_beam.utils.timestamp import Timestamp

logger = logging.getLogger(__name__)


class EnrichMetadataDoFn(DoFn):
    """Add processing metadata to records."""

    def process(
        self,
        element: Tuple[Dict[str, Any], str, bool],
        window=DoFn.WindowParam,
        timestamp=DoFn.TimestampParam,
    ):
        """
        Add ingestion and processing timestamps.
        
        Args:
            element: (record_dict, message_id, is_valid)
            window: Beam window (unused for streaming, but available)
            timestamp: Pub/Sub publish timestamp
        
        Yields:
            Enhanced record dict with metadata
        """
        record, message_id, is_valid = element
        

        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        

        enriched_record = {
            **record,
            "processing_timestamp": now_iso,
            "pubsub_message_id": message_id or "unknown",
            "is_valid": is_valid,
        }
        
        yield enriched_record


def enrich_metadata_transform(pcoll):
    """
    Apply metadata enrichment to PCollection.
    
    Args:
        pcoll: Input PCollection of (record_dict, message_id, is_valid) tuples
    
    Returns:
        Enriched PCollection of full record dicts
    """
    return (
        pcoll
        | "EnrichMetadata" >> ParDo(EnrichMetadataDoFn())
    )


def prepare_for_bronze_write(record: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare record for Bronze (raw) table write."""
    import json
    
    return {
        "transaction_id": record.get("transaction_id"),
        "raw_payload": json.dumps({
            k: v for k, v in record.items()
            if k not in ["processing_timestamp", "is_valid", "pubsub_message_id"]
        }),
        "ingestion_time": record.get("ingestion_time"),
        "pubsub_message_id": record.get("pubsub_message_id"),
        "processing_time": record.get("processing_timestamp"),
    }


def prepare_for_silver_write(record: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare record for Silver (clean) table write."""
    return {
        "transaction_id": record.get("transaction_id"),
        "user_id": record.get("user_id"),
        "region": record.get("region"),
        "product_id": record.get("product_id"),
        "amount": float(record.get("amount", 0)),
        "currency": record.get("currency"),
        "event_time": record.get("event_time"),
        "ingestion_time": record.get("ingestion_time"),
        "schema_version": int(record.get("schema_version", 1)),
        "is_valid": record.get("is_valid", False),
        "processing_timestamp": record.get("processing_timestamp"),
    }
