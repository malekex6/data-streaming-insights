"""Runs the real build_pipeline graph with Pub/Sub and BigQuery replaced by fakes."""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from dataflow import pipeline as pl
from dataflow.schemas import bq_schemas as s

T0 = 1_767_225_600
SINKS = {}  # table name -> PCollection handed to the (fake) BigQuery write


def msg(tid, event_s, arrival_s, amount=10.0, body=None):
    event_iso = datetime.fromtimestamp(event_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = body if body is not None else json.dumps({
        "transaction_id": tid, "user_id": "user-001", "region": "EU", "product_id": "prod-001",
        "amount": amount, "currency": "EUR", "event_time": event_iso, "schema_version": 1})
    m = PubsubMessage(payload.encode(), {"event_time": event_iso, "transaction_id": tid},
                      message_id=f"m-{tid}-{arrival_s}",
                      publish_time=datetime.fromtimestamp(arrival_s, tz=timezone.utc))
    return TimestampedValue(m, event_s)


class FakeSink(beam.PTransform):
    def __init__(self, table):
        super().__init__()
        self.table = table.split(".")[-1]

    def expand(self, rows):
        SINKS[self.table] = rows
        return rows


def test_full_graph(monkeypatch):
    SINKS.clear()
    stream = (
        BeamTestStream()
        .add_elements([msg("A", T0 + 10, T0 + 11), msg("B", T0 + 20, T0 + 21),
                       msg("B", T0 + 20, T0 + 40),                 # retry copy
                       msg("X", T0 + 25, T0 + 26, body="{broken")])  # bad JSON
        .advance_watermark_to(T0 + 70)
        .add_elements([msg("C", T0 + 30, T0 + 300, amount=5.0)])   # late, offline phone
        .advance_watermark_to_infinity()
    )
    monkeypatch.setattr(pl.beam.io, "ReadFromPubSub", lambda **kwargs: stream)
    monkeypatch.setattr(pl, "append_to", lambda table, schema, params: FakeSink(table))

    args, _ = pl.parse_args(["--project=my-project", "--input_topic=projects/my-project/topics/t"])
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as p:
        pl.build_pipeline(p, args)

        def check(table, fn, expected):
            assert_that(SINKS[table] | f"Look{table}" >> beam.Map(fn), equal_to(expected), label=f"Check{table}")

        # bronze: every message, even the broken one and the retry copy
        check("transactions_bronze", lambda r: r["transaction_id"], ["A", "B", "B", "X", "C"])
        check("transactions_dead_letter", lambda r: r["error_type"], ["bad_json"])
        check("transactions_silver_staging", lambda r: r["transaction_id"], ["A", "B", "C"])
        check("gold_sales_per_minute_staging", lambda r: (r["pane_timing"], r["total_sales"]),
              [("ON_TIME", 20.0), ("LATE", 25.0)])
        # The visit (last sale T0+20, gap 5 min) is still open at watermark T0+70,
        # so C is NOT late for the session: one ON_TIME visit of 3 purchases.
        check("gold_user_sessions_staging", lambda r: (r["pane_timing"], r["purchase_count"]),
              [("ON_TIME", 3)])


def test_real_sink_builds_with_partitioning():
    # Construction only: checks the WriteToBigQuery arguments are accepted.
    pl.append_to("my-project:my_dataset.transactions_bronze", s.BRONZE_SCHEMA, s.BRONZE_PARAMS)
