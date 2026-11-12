"""Step 1: turn a Pub/Sub message into a bronze row and a parsed record.

Three clocks are attached here, and they answer three different questions:

    event_time       when the sale happened         (set by the app, read from the payload)
    arrival_time     when it reached our platform   (Pub/Sub publish_time, set by Google)
    processing_time  when this pipeline handled it  (worker clock, now)

    arrival_time    - event_time    = delivery latency  (network, offline phone queue)
    processing_time - arrival_time  = pipeline lag      (backlog, slow workers)
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from dataflow.utils.timeutil import to_rfc3339, utc_now

BRONZE = "bronze"
DEAD_LETTER = "dead_letter"


def _arrival_time(msg, fallback: datetime) -> datetime:
    publish_time = getattr(msg, "publish_time", None)
    if isinstance(publish_time, datetime):
        return publish_time if publish_time.tzinfo else publish_time.replace(tzinfo=timezone.utc)
    return fallback  # e.g. tests that build messages by hand


class ParsePubsubMessage(beam.DoFn):
    """Outputs:
    - main:        dict = JSON payload + message_id, arrival_time, processing_time
    - bronze:      one row per message, ALWAYS, even if the JSON is broken
    - dead_letter: messages whose body is not a JSON object
    """

    def __init__(self):
        self.parsed = Metrics.counter("parse", "parsed")
        self.bad_json = Metrics.counter("parse", "bad_json")

    def process(self, msg):
        now = utc_now()
        arrival = _arrival_time(msg, now)
        attributes = dict(msg.attributes or {})
        raw = msg.data.decode("utf-8", errors="replace")
        meta = {
            "message_id": getattr(msg, "message_id", None),
            "arrival_time": to_rfc3339(arrival),
            "processing_time": to_rfc3339(now),
        }

        # Bronze = exactly what arrived, before any rule. It is the replay source.
        yield pvalue.TaggedOutput(BRONZE, {
            "transaction_id": attributes.get("transaction_id"),
            "event_time_attribute": attributes.get("event_time"),
            "raw_payload": raw,
            **meta,
        })

        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("payload is not a JSON object")
        except ValueError as exc:  # JSONDecodeError is a ValueError
            self.bad_json.inc()
            yield pvalue.TaggedOutput(DEAD_LETTER, {
                "stage": "parse",
                "error_type": "bad_json",
                "errors": str(exc),
                "raw_payload": raw,
                **meta,
            })
            return

        self.parsed.inc()
        yield {**payload, **meta}


class ParseMessages(beam.PTransform):
    """Returns a tagged result with .parsed, .bronze and .dead_letter."""

    def expand(self, messages):
        return messages | beam.ParDo(ParsePubsubMessage()).with_outputs(
            BRONZE, DEAD_LETTER, main="parsed")
