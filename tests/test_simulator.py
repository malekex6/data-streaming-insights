import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from app.services.simulator import ShopSimulator

START = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)


def run(minutes=60, seed=7):
    sim = ShopSimulator(rng=random.Random(seed))
    sent = []  # (arrival, event)
    for s in range(minutes * 60):
        now = START + timedelta(seconds=s)
        sent.extend((now, e) for e in sim.step(now))
    return sim, sent


def test_most_sales_arrive_fast_some_late_some_too_late():
    _, sent = run()
    latencies = [(arrival - e.event_time).total_seconds() for arrival, e in sent]
    fast = sum(1 for x in latencies if x <= 5) / len(latencies)
    late = sum(1 for x in latencies if 60 < x <= 600)
    too_late = sum(1 for x in latencies if x > 600)
    assert fast > 0.8
    assert late > 0 and too_late > 0  # both cases exist, on purpose


def test_retries_resend_the_same_transaction_id():
    _, sent = run()
    counts = Counter(e.transaction_id for _, e in sent)
    assert any(c > 1 for c in counts.values())


def test_purchases_of_a_visit_are_closer_than_the_session_gap():
    _, sent = run(minutes=20)
    by_user = defaultdict(list)
    for _, e in sent:
        by_user[e.user_id].append(e.event_time)
    gaps = []
    for times in by_user.values():
        times = sorted(set(times))
        gaps += [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    assert any(g < 300 for g in gaps)
