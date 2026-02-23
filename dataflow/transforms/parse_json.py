"""JSON parsing transform with error handling."""

import logging
import json
from typing import Tuple, Dict, Any

import apache_beam as beam
from apache_beam.transforms import DoFn, ParDo, FlatMap
from apache_beam import pvalue


logger = logging.getLogger(__name__)


class ParseJSONDoFn(DoFn):
    """Parse Pub/Sub message payload from JSON to dict."""
    
    COUNTER_PARSE_SUCCESS = "parse_success"
    COUNTER_PARSE_ERROR = "parse_error"

    def setup(self):
        """Initialize counters (called once per worker instance)."""
        self.counter_parse_success = 0
        self.counter_parse_error = 0

    def process(self, element: bytes, *args, **kwargs):
        """
        Parse JSON message.
        
        Yields:
            Tuple[Dict[str, Any], str]: (parsed_dict, message_id)
        
        Side output:
            dead_letter: Raw message if parsing fails
        """
        try:
            # Decode bytes to string
            message_str = element.decode("utf-8")
            
            # Parse JSON
            parsed_data = json.loads(message_str)
            
            # Yield with increment counter
            self.counter_parse_success += 1
            yield pvalue.TaggedOutput(None, (parsed_data, None))
            
        except json.JSONDecodeError as exc:
            logger.error("JSON parse error: %s for message: %s", exc, element[:100])
            self.counter_parse_error += 1
            # Send to dead letter
            yield pvalue.TaggedOutput("dead_letter", {
                "raw_message": element.decode("utf-8", errors="replace"),
                "error": str(exc),
                "error_type": "json_decode_error"
            })
        except Exception as exc:
            logger.exception("Unexpected error parsing message")
            self.counter_parse_error += 1
            yield pvalue.TaggedOutput("dead_letter", {
                "raw_message": element.decode("utf-8", errors="replace"),
                "error": str(exc),
                "error_type": "unexpected_error"
            })

    def result(self):
        return {
            self.COUNTER_PARSE_SUCCESS: self.counter_parse_success,
            self.COUNTER_PARSE_ERROR: self.counter_parse_error,
        }


def parse_json_transform(pcoll):
    """
    Apply JSON parsing to PCollection.
    
    Args:
        pcoll: Input PCollection of bytes
    
    Returns:
        Tuple of (parsed_pcoll, dead_letter_pcoll)
    """
    results = (
        pcoll
        | "ParseJSON" >> ParDo(ParseJSONDoFn()).with_outputs(
            'dead_letter',
             main="parsed"
        ) 
    )           
        
    parsed_messages = results.parsed  
    parse_dead_letter = results.dead_letter
    
    return parsed_messages, parse_dead_letter