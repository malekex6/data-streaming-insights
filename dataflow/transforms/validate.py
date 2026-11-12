"""Step 2: business rules. Good rows become silver rows, bad rows go to the dead letter table."""

import json
from datetime import timedelta

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from dataflow.utils.timeutil import parse_rfc3339, to_rfc3339

DEAD_LETTER = "dead_letter"

REQUIRED_FIELDS = {
    "transaction_id": str,
    "user_id": str,
    "region": str,
    "product_id": str,
    "amount": (int, float),
    "currency": str,
    "event_time": str,
    "schema_version": int,
}
VALID_REGIONS = {"EU", "US", "APAC"}

# A phone with a wrong clock can send a sale "from the future". Beyond this
# margin we refuse it: a future event_time would hold the window open and
# could push the watermark logic off.
MAX_CLOCK_SKEW = timedelta(minutes=5)


class ValidateSale(beam.DoFn):
    def __init__(self):
        self.valid = Metrics.counter("validate", "valid")
        self.invalid = Metrics.counter("validate", "invalid")
        # Beam distribution = min / max / mean / count, visible in the Dataflow UI.
        self.arrival_latency_ms = Metrics.distribution("latency", "arrival_latency_ms")

    def process(self, record):
        errors = []
        for name, expected in REQUIRED_FIELDS.items():
            if name not in record:
                errors.append(f"missing {name}")
            elif isinstance(record[name], bool) or not isinstance(record[name], expected):
                errors.append(f"{name} has type {type(record[name]).__name__}")

        event_time = arrival_time = None
        if not errors:
            if record["region"] not in VALID_REGIONS:
                errors.append(f"invalid region {record['region']}")
            if record["amount"] <= 0:
                errors.append(f"amount must be > 0, got {record['amount']}")
            try:
                event_time = parse_rfc3339(record["event_time"])
                arrival_time = parse_rfc3339(record["arrival_time"])
            except (ValueError, KeyError) as exc:
                errors.append(f"bad timestamp: {exc}")
            if event_time and arrival_time and event_time > arrival_time + MAX_CLOCK_SKEW:
                errors.append("event_time is in the future (device clock skew)")

        if errors:
            self.invalid.inc()
            yield pvalue.TaggedOutput(DEAD_LETTER, {
                "stage": "validate",
                "error_type": "rule_violation",
                "errors": "; ".join(errors),
                "raw_payload": json.dumps(record, default=str),
                "message_id": record.get("message_id"),
                "arrival_time": record.get("arrival_time"),
                "processing_time": record.get("processing_time"),
            })
            return

        self.valid.inc()
        self.arrival_latency_ms.update(int((arrival_time - event_time).total_seconds() * 1000))
        yield {
            "transaction_id": record["transaction_id"],
            "user_id": record["user_id"],
            "region": record["region"],
            "product_id": record["product_id"],
            "amount": float(record["amount"]),
            "currency": record["currency"].upper(),
            "event_time": to_rfc3339(event_time),
            "arrival_time": to_rfc3339(arrival_time),
            "processing_time": record["processing_time"],
            "message_id": record.get("message_id"),
            "schema_version": record["schema_version"],
        }


class ValidateSales(beam.PTransform):
    """Returns a tagged result with .valid and .dead_letter."""

    def expand(self, records):
        return records | beam.ParDo(ValidateSale()).with_outputs(DEAD_LETTER, main="valid")
