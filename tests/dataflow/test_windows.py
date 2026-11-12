"""TestStream tests: we control the watermark by hand, so we can prove what
happens to an on-time sale, a late sale and a too-late sale."""

from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from dataflow.transforms.dedup import DropDuplicateSales
from dataflow.transforms.windows import (
    ALLOWED_LATENESS_SECONDS,
    RegionSalesRolling10m,
    SalesPerMinute,
    UserSessions,
)
from dataflow.utils.timeutil import to_rfc3339

T0 = 1_767_225_600  # 2026-01-01T00:00:00Z, a round minute


def iso(seconds: float) -> str:
    return to_rfc3339(datetime.fromtimestamp(seconds, tz=timezone.utc))


def sale(tid, event_s, amount, arrival_s=None, user="user-001", region="EU", product="prod-001"):
    arrival_s = event_s + 1 if arrival_s is None else arrival_s
    row = {
        "transaction_id": tid, "user_id": user, "region": region, "product_id": product,
        "amount": float(amount), "currency": "EUR",
        "event_time": iso(event_s), "arrival_time": iso(arrival_s),
    }
    # The Beam timestamp = event time, exactly what timestamp_attribute does in prod.
    return TimestampedValue(row, event_s)


def streaming_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def test_tumbling_on_time_then_late_then_too_late():
    stream = (
        BeamTestStream()
        .add_elements([sale("A", T0 + 10, 10), sale("B", T0 + 20, 20)])
        .advance_watermark_to(T0 + 70)                       # window [T0, T0+60) is closed: ON_TIME pane
        .add_elements([sale("C", T0 + 30, 5, arrival_s=T0 + 300)])  # 4 min late, within lateness: LATE pane
        .advance_watermark_to(T0 + 60 + ALLOWED_LATENESS_SECONDS + 10)  # lateness is over
        .add_elements([sale("D", T0 + 40, 1000)])            # too late: dropped from gold
        .advance_watermark_to_infinity()
    )
    with streaming_pipeline() as p:
        rows = p | stream | SalesPerMinute()
        out = rows | beam.Map(lambda r: (r["window_start"], r["pane_timing"], r["pane_index"],
                                         r["total_sales"], r["transaction_count"]))
        assert_that(out, equal_to([
            (iso(T0), "ON_TIME", 0, 30.0, 2),
            (iso(T0), "LATE", 1, 35.0, 3),   # ACCUMULATING: the late pane has the full total
        ]))


def test_tumbling_reports_arrival_latency():
    stream = (
        BeamTestStream()
        .add_elements([sale("A", T0 + 10, 10, arrival_s=T0 + 12), sale("B", T0 + 20, 20, arrival_s=T0 + 26)])
        .advance_watermark_to_infinity()
    )
    with streaming_pipeline() as p:
        rows = p | stream | SalesPerMinute()
        out = rows | beam.Map(lambda r: (r["avg_arrival_latency_s"], r["max_arrival_latency_s"]))
        assert_that(out, equal_to([(4.0, 6.0)]))


def test_sliding_counts_one_sale_in_ten_windows():
    stream = BeamTestStream().add_elements([sale("A", T0 + 30, 10)]).advance_watermark_to_infinity()
    with streaming_pipeline() as p:
        rows = p | stream | RegionSalesRolling10m()
        out = rows | beam.Map(lambda r: (r["window_start"], r["total_sales"]))
        # Windows of 10 min starting every minute that contain T0+30: T0-9min ... T0
        assert_that(out, equal_to([(iso(T0 - 60 * k), 10.0) for k in range(10)]))


def test_sessions_split_on_gap_then_late_sale_merges_them():
    stream = (
        BeamTestStream()
        .add_elements([sale("A", T0, 10), sale("B", T0 + 60, 20), sale("C", T0 + 500, 30)])
        .advance_watermark_to(T0 + 900)   # both visits closed: [T0, T0+360) and [T0+500, T0+800)
        .add_elements([sale("D", T0 + 250, 5, arrival_s=T0 + 900)])  # late sale in the silence
        .advance_watermark_to_infinity()
    )
    with streaming_pipeline() as p:
        rows = p | stream | UserSessions()
        out = rows | beam.Map(lambda r: (r["window_start"], r["window_end"], r["pane_timing"],
                                         r["purchase_count"], r["revenue"]))
        assert_that(out, equal_to([
            (iso(T0), iso(T0 + 360), "ON_TIME", 2, 30.0),
            (iso(T0 + 500), iso(T0 + 800), "ON_TIME", 1, 30.0),
            # D at T0+250 extends visit 1 to T0+550, which overlaps visit 2: one visit now
            (iso(T0), iso(T0 + 800), "LATE", 4, 65.0),
        ]))


def test_dedup_drops_copy_even_across_window_boundaries():
    stream = (
        BeamTestStream()
        .add_elements([sale("A", T0 + 59, 10)])
        .advance_processing_time(20)
        .add_elements([sale("A", T0 + 59, 10, arrival_s=T0 + 90)])  # app retry, same transaction_id
        .add_elements([sale("B", T0 + 61, 20)])
        .advance_watermark_to_infinity()
    )
    with streaming_pipeline() as p:
        out = p | stream | DropDuplicateSales() | beam.Map(lambda s: s["transaction_id"])
        assert_that(out, equal_to(["A", "B"]))
