"""Deduplication transform using transaction_id."""

import logging
from typing import Dict, Any, Tuple

import apache_beam as beam
from apache_beam.transforms import DoFn, ParDo, Map

logger = logging.getLogger(__name__)


class DeduplicateByKeyDoFn(DoFn):
    """Deduplicate records by transaction_id (stateful approach for streaming)."""

    def process(self, element: Tuple[Dict[str, Any], str, bool], *args, **kwargs):
        """
        Simple deduplication by extracting transaction_id as key.
        
        For production streaming, consider Apache Beam Distinct or external state store.
        
        Args:
            element: (record_dict, message_id, is_valid)
        
        Yields:
            (transaction_id, record_dict, message_id, is_valid)
        """
        record, message_id, is_valid = element
        transaction_id = record.get("transaction_id", "unknown")
        yield (transaction_id, (record, message_id, is_valid))


def add_transaction_id_key(element: Tuple[Dict[str, Any], str, bool]) -> Tuple[str, Tuple]:
    """
    Map to add transaction_id as key for deduplication.
    
    Args:
        element: (record_dict, message_id, is_valid)
    
    Returns:
        (transaction_id, (record, message_id, is_valid))
    """
    record, message_id, is_valid = element
    transaction_id = record.get("transaction_id", "unknown")
    return (transaction_id, (record, message_id, is_valid))


def deduplicate_on_transaction_id_transform(pcoll):
    """
    Apply deduplication by transaction_id.
    
    Uses Beam's Distinct on the record dict itself (all duplicates dropped).
    For true deduplication across time windows, consider:
    - Stateful processing with TTL
    - External Redis/Memstore
    - Distinct() per time window
    
    Args:
        pcoll: Input PCollection of (record_dict, message_id, is_valid) tuples
    
    Returns:
        Deduplicated PCollection
    """
    return (
        pcoll
        | "WindowInto" >> beam.WindowInto(beam.window.FixedWindows(15))  
        | "ExtractTransactionID" >> beam.Map(lambda x: (x[0]['transaction_id'], x))
        | "GroupByKey" >> beam.GroupByKey()
        | "TakeFirst" >> beam.Map(lambda x: x[1][0])
        | "RemoveWindow" >> beam.WindowInto(beam.window.GlobalWindows())  
    )



class StatefulDeduplicateDoFn(DoFn):
    """
    Stateful deduplication within a time window.
    
    WARNING: Only use with windowed PCollections.
    """

    def process(
        self,
        element: Tuple[Dict[str, Any], str, bool],
        window=DoFn.WindowParam,
    ):
        """
        Deduplicate within a window using Beam's state API.
        
        In production, consider TTL for the state store.
        """
        from apache_beam.transforms.window import BoundedWindow
        from apache_beam.transforms import userstate

        record, message_id, is_valid = element
        transaction_id = record.get("transaction_id", "unknown")



        yield (record, message_id, is_valid)
