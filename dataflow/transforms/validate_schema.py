"""Schema validation transform."""

import logging
from typing import Dict, Any, Tuple

import apache_beam as beam
from apache_beam.transforms import DoFn, ParDo
from apache_beam.utils.timestamp import Timestamp

logger = logging.getLogger(__name__)


class ValidateSchemaDoFn(DoFn):
    """Validate transaction event schema."""

    REQUIRED_FIELDS = {
        "transaction_id": str,
        "user_id": str,
        "region": str,
        "product_id": str,
        "amount": (int, float),
        "currency": str,
        "event_time": str,
        "ingestion_time": str,
        "schema_version": int,
    }

    VALID_REGIONS = {"EU", "US", "APAC"}
    COUNTER_VALID = "validation_success"
    COUNTER_INVALID = "validation_error"

    def setup(self):
        """Initialize counters (called once per worker instance)."""
        self.counter_valid = 0
        self.counter_invalid = 0

    def process(self, element: Tuple[Dict[str, Any], str, bool], *args, **kwargs):
        """
        Validate required fields and types.
        
        Args:
            element: (parsed_dict, message_id, is_valid)
        
        Yields:
            Tuple: (validated_dict, message_id, is_valid)
        
        Side output:
            dead_letter: Invalid record
        """
        parsed_data, message_id, is_valid = element
        errors = []


        for field_name, expected_type in self.REQUIRED_FIELDS.items():
            if field_name not in parsed_data:
                errors.append(f"Missing required field: {field_name}")
                continue

            value = parsed_data[field_name]
            if not isinstance(value, expected_type):
                errors.append(
                    f"Field {field_name} has type {type(value).__name__}, "
                    f"expected {expected_type}"
                )


        if "region" in parsed_data and parsed_data["region"] not in self.VALID_REGIONS:
            errors.append(
                f"Invalid region: {parsed_data['region']}. "
                f"Must be one of {self.VALID_REGIONS}"
            )


        if "amount" in parsed_data and parsed_data["amount"] <= 0:
            errors.append(f"Amount must be > 0, got {parsed_data['amount']}")

        if errors:
            logger.warning("Validation errors for transaction %s: %s",
                         parsed_data.get("transaction_id", "unknown"), errors)
            self.counter_invalid += 1
            
            yield beam.pvalue.TaggedOutput("dead_letter", {
                "record": parsed_data,
                "errors": errors,
                "error_type": "schema_validation_error"
            })
        else:
            self.counter_valid += 1
            yield (parsed_data, message_id, True)

    def result(self):
        return {
            self.COUNTER_VALID: self.counter_valid,
            self.COUNTER_INVALID: self.counter_invalid,
        }


def validate_schema_transform(pcoll):
    """
    Apply schema validation to PCollection.
    
    Args:
        pcoll: Input PCollection of (parsed_dict, message_id) tuples
    
    Returns:
        Tuple of (valid_pcoll, dead_letter_pcoll)
    """
    validation_result = (
        pcoll
        | "ValidateSchema" >> ParDo(ValidateSchemaDoFn()).with_outputs(
            'dead_letter',
            main='main'
        )
    )
    valid_records = validation_result.main
    validation_dead_letter = validation_result.dead_letter
    return valid_records, validation_dead_letter 