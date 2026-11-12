"""Step 4: the three windowed aggregations (gold).

Every element here already carries its EVENT TIME as Beam timestamp
(ReadFromPubSub(timestamp_attribute="event_time")), so a sale made at 10:00:50
lands in the 10:00-10:01 window even if the phone sends it at 10:07.

Watermark, in one line: Beam's running guess "I have probably seen every sale
up to time T". A window [10:00, 10:01) fires its ON_TIME result when the
watermark passes 10:01.

Allowed lateness = 10 minutes (ALLOWED_LATENESS_SECONDS), chosen because:
- Delivery delay in this shop (simulator, 3 h x 3 seeds): ~90 % of sales
  arrive within 5 s, ~6 % between 5 s and 10 min, ~3.5 % after 10 min
  (offline phones, 1 to 25 min). 10 min keeps ~96 % of sales in gold and
  catches the short offline cases (a metro ride, a bad-signal zone).
- Gold is a LIVE dashboard. A correction 30 min later helps nobody watching
  it; the daily numbers are rebuilt from silver, which keeps every sale.
- Cost: each window keeps its state open for size + 10 min. For sliding
  windows (10 per sale) that is 20 min of state per region: still small.
Check the choice against real data with sql/checks.sql (query "lateness
coverage"): the share of sales with arrival latency above 10 min should
stay small. If it grows, raise the value.

Accumulation = ACCUMULATING: each new pane carries the FULL total of the
window so far, not only what is new. That makes the BigQuery MERGE simple and
idempotent: "the latest pane replaces the row". With DISCARDING panes we would
have to ADD panes together, and a replayed pane would be counted twice.
"""

import apache_beam as beam
from apache_beam.transforms import trigger, window
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.windowed_value import PaneInfoTiming

from dataflow.utils.timeutil import beam_ts_to_rfc3339, parse_rfc3339, to_rfc3339, utc_now

ALLOWED_LATENESS_SECONDS = 10 * 60

SALES_WINDOW_SECONDS = 60           # tumbling
EARLY_FIRING_SECONDS = 30           # partial result while the minute is still open
ROLLING_SIZE_SECONDS = 10 * 60      # sliding: look at the last 10 minutes...
ROLLING_PERIOD_SECONDS = 60         # ...recomputed every minute
SESSION_GAP_SECONDS = 5 * 60        # a visit ends after 5 min without a purchase

_TIMING = {
    PaneInfoTiming.EARLY: "EARLY",
    PaneInfoTiming.ON_TIME: "ON_TIME",
    PaneInfoTiming.LATE: "LATE",
    PaneInfoTiming.UNKNOWN: "UNKNOWN",
}


def _latency_s(sale) -> float:
    return (parse_rfc3339(sale["arrival_time"]) - parse_rfc3339(sale["event_time"])).total_seconds()


class SalesStatsFn(beam.CombineFn):
    """Total, count, average amount and delivery latency of the sales in a window."""

    def create_accumulator(self):
        return (0.0, 0, 0.0, 0.0)  # total, count, latency_sum, latency_max

    def add_input(self, acc, sale):
        total, count, lat_sum, lat_max = acc
        lat = _latency_s(sale)
        return (total + sale["amount"], count + 1, lat_sum + lat, max(lat_max, lat))

    def merge_accumulators(self, accs):
        accs = list(accs)
        return (sum(a[0] for a in accs), sum(a[1] for a in accs),
                sum(a[2] for a in accs), max((a[3] for a in accs), default=0.0))

    def extract_output(self, acc):
        total, count, lat_sum, lat_max = acc
        return {
            "total_sales": round(total, 2),
            "transaction_count": count,
            "avg_amount": round(total / count, 2) if count else 0.0,
            "avg_arrival_latency_s": round(lat_sum / count, 3) if count else 0.0,
            "max_arrival_latency_s": round(lat_max, 3),
        }


class SessionStatsFn(beam.CombineFn):
    """What happened in one customer visit."""

    def create_accumulator(self):
        return (0, 0.0, None, None)  # purchases, revenue, first, last (RFC3339 strings sort in time order)

    def add_input(self, acc, sale):
        n, rev, first, last = acc
        t = sale["event_time"]
        return (n + 1, rev + sale["amount"], min(first or t, t), max(last or t, t))

    def merge_accumulators(self, accs):
        accs = [a for a in accs if a[0]]
        if not accs:
            return self.create_accumulator()
        return (sum(a[0] for a in accs), sum(a[1] for a in accs),
                min(a[2] for a in accs), max(a[3] for a in accs))

    def extract_output(self, acc):
        n, rev, first, last = acc
        return {
            "purchase_count": n,
            "revenue": round(rev, 2),
            "first_purchase_time": first,
            "last_purchase_time": last,
            "visit_duration_s": (parse_rfc3339(last) - parse_rfc3339(first)).total_seconds() if n else 0.0,
        }


class AddWindowInfo(beam.DoFn):
    """Flattens (key, stats) and adds the window bounds and the pane facts.

    pane_index grows by 1 at every firing of the same window: it is the
    "version" the BigQuery MERGE uses to keep only the newest result.
    """

    def __init__(self, key_names):
        self.key_names = key_names

    def process(self, kv, w=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        key, stats = kv
        key = key if isinstance(key, tuple) else (key,)
        yield {
            "window_start": beam_ts_to_rfc3339(w.start),
            "window_end": beam_ts_to_rfc3339(w.end),
            **dict(zip(self.key_names, key)),
            **stats,
            "pane_index": pane.index,
            "pane_timing": _TIMING.get(pane.timing, "UNKNOWN"),
            "is_final_pane": pane.is_last,
            "emitted_at": to_rfc3339(utc_now()),
        }


class SalesPerMinute(beam.PTransform):
    """TUMBLING 1 min, per region and product.

    Real-life use: the live sales board ("what sold this minute") and the
    building block of hourly/daily totals. Because tumbling windows never
    overlap, SUM over 60 minute rows = the hour. That is only true here.
    """

    def __init__(self, allowed_lateness_s=ALLOWED_LATENESS_SECONDS):
        super().__init__()
        self.allowed_lateness_s = allowed_lateness_s

    def expand(self, sales):
        return (
            sales
            | "Tumbling1m" >> beam.WindowInto(
                window.FixedWindows(SALES_WINDOW_SECONDS),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(EARLY_FIRING_SECONDS),
                    late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=self.allowed_lateness_s))
            | "KeyRegionProduct" >> beam.Map(lambda s: ((s["region"], s["product_id"]), s))
            | "Stats" >> beam.CombinePerKey(SalesStatsFn())
            | "AddWindow" >> beam.ParDo(AddWindowInfo(["region", "product_id"]))
        )


class RegionSalesRolling10m(beam.PTransform):
    """SLIDING 10 min, every 1 min, per region.

    Real-life use: "is a region's checkout broken right now?". One minute
    alone is noisy (2 sales vs 5 sales means nothing); 10 minutes is stable,
    and sliding every minute means a drop is seen within ~1 min, not at the
    end of the next 10 min block. Each sale is counted in 10 windows, so NEVER
    sum these rows: compare them (see the alert view in sql/views.sql).
    """

    def __init__(self, allowed_lateness_s=ALLOWED_LATENESS_SECONDS):
        super().__init__()
        self.allowed_lateness_s = allowed_lateness_s

    def expand(self, sales):
        return (
            sales
            | "Sliding10mEvery1m" >> beam.WindowInto(
                window.SlidingWindows(size=ROLLING_SIZE_SECONDS, period=ROLLING_PERIOD_SECONDS),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=self.allowed_lateness_s))
            | "KeyRegion" >> beam.Map(lambda s: (s["region"], s))
            | "Stats" >> beam.CombinePerKey(SalesStatsFn())
            | "AddWindow" >> beam.ParDo(AddWindowInfo(["region"]))
        )


class UserSessions(beam.PTransform):
    """SESSION windows, 5 min gap, per customer.

    Real-life use: one row per shopping visit: basket size, visit revenue,
    visit length. There is no fixed clock here: the window is as long as the
    customer keeps buying. 5 min gap because in this shop two purchases of
    the same visit are 20 s to 3 min apart; 5 min of silence = they left.

    Late data can MERGE two sessions: a late sale at 10:06 can glue a visit
    10:00-10:04 to a visit 10:09-10:12. Beam then emits a new, wider session.
    The MERGE in sql/merge_gold_user_sessions.sql deletes the older, narrower
    rows that the wider one now covers.
    """

    def __init__(self, allowed_lateness_s=ALLOWED_LATENESS_SECONDS):
        super().__init__()
        self.allowed_lateness_s = allowed_lateness_s

    def expand(self, sales):
        return (
            sales
            | "Sessions5mGap" >> beam.WindowInto(
                window.Sessions(SESSION_GAP_SECONDS),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=self.allowed_lateness_s))
            | "KeyUser" >> beam.Map(lambda s: (s["user_id"], s))
            | "Stats" >> beam.CombinePerKey(SessionStatsFn())
            | "AddWindow" >> beam.ParDo(AddWindowInfo(["user_id"]))
        )
