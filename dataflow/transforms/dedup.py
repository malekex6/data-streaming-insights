"""Step 3: drop re-sent copies of the same sale, in flight.

Why here, before the windows: gold counts sales. If a copy reaches a window,
the minute total is wrong, and no MERGE on the gold key can fix it later
(the copy is inside the sum). So copies must die before aggregation.

How: Beam's DeduplicatePerKey keeps, per transaction_id, a "seen" flag in
state, and a timer clears it after `ttl_seconds` (processing time).

Why 1 hour: copies come from (a) app retries, 2 to 30 s later, (b) Pub/Sub
redelivery, seconds to minutes, (c) an offline phone that flushes its queue
twice, up to ~25 min in our simulator. 1 hour covers all three with margin,
and the state stays small: one flag per sale seen in the last hour.

What it does NOT cover: a copy that arrives more than 1 hour later. That one
reaches silver staging, and the silver MERGE on transaction_id removes it at
rest. Gold could then be off by one sale for that minute: accepted, rare.
"""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.utils.timestamp import Duration

DEFAULT_DEDUP_TTL_SECONDS = 3600


class _Count(beam.DoFn):
    def __init__(self, name):
        self.counter = Metrics.counter("dedup", name)

    def process(self, element):
        self.counter.inc()
        yield element


class DropDuplicateSales(beam.PTransform):
    def __init__(self, ttl_seconds: int = DEFAULT_DEDUP_TTL_SECONDS):
        super().__init__()
        self.ttl_seconds = ttl_seconds

    def expand(self, sales):
        return (
            sales
            | "CountIn" >> beam.ParDo(_Count("in"))
            | "KeyByTransactionId" >> beam.Map(lambda s: (s["transaction_id"], s))
            | "Deduplicate" >> DeduplicatePerKey(processing_time_duration=Duration(seconds=self.ttl_seconds))
            | "DropKey" >> beam.Values()
            | "CountOut" >> beam.ParDo(_Count("out"))
        )
