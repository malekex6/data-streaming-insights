"""Realistic stream of sales for an online shop (mobile app + website).

Three real-life behaviours are simulated, one per stream-processing concept:

1. Customer visits  -> SESSION windows.
   A customer opens the app, buys 1 to 4 things, 20 s to 3 min apart, then
   leaves. Silence of more than 5 min means the visit is over.

2. Offline phones   -> LATE data, WATERMARK, ALLOWED LATENESS.
   Some customers buy while offline (metro, plane, bad signal). The app keeps
   the sale in a local queue and sends it when the phone is back online.
   The sale keeps its real event_time, so it arrives minutes late.
   Offline time is 1 to 25 min, so some sales are later than the 10 min
   allowed lateness on purpose: they are still kept in silver, but they no
   longer change the closed gold windows.

3. App retries      -> AT-LEAST-ONCE + DEDUP / IDEMPOTENT MERGE.
   When the app does not get an answer in time it sends the same sale again,
   with the same transaction_id.

The simulator is pure logic: you give it the clock (`now`), it returns what
the shop's devices would send at that second. That makes it testable.
"""

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from app.models.event_model import Region, TransactionEvent
from app.services.generator_service import USER_IDS, make_event


@dataclass
class SimulatorConfig:
    visits_per_minute: float = 20.0
    min_purchases: int = 1
    max_purchases: int = 4
    min_gap_s: float = 20.0           # time between two purchases in a visit
    max_gap_s: float = 180.0          # < 300 s session gap, so a visit = one session
    offline_ratio: float = 0.08       # share of visits done on an offline phone
    min_offline_s: float = 60.0
    max_offline_s: float = 25 * 60.0  # some go past the 10 min allowed lateness
    retry_ratio: float = 0.03         # share of sales the app sends twice
    min_retry_delay_s: float = 2.0
    max_retry_delay_s: float = 30.0
    max_network_jitter_s: float = 3.0


@dataclass
class _Visit:
    user_id: str
    region: str
    remaining: int
    next_purchase_at: datetime
    offline_until: Optional[datetime]
    queue: List[TransactionEvent] = field(default_factory=list)  # phone's local queue


@dataclass
class SimulatorStats:
    sent: int = 0
    sent_late_from_offline_queue: int = 0
    sent_as_retry_duplicate: int = 0
    visits_started: int = 0


class ShopSimulator:
    def __init__(self, config: Optional[SimulatorConfig] = None, rng: Optional[random.Random] = None):
        self.cfg = config or SimulatorConfig()
        self.rng = rng or random.Random()
        self.visits: Dict[str, _Visit] = {}
        self.retries: List[tuple] = []  # (send_at, event)
        self.stats = SimulatorStats()
        self._visit_carry = 0.0

    def _start_visits(self, now: datetime, seconds: float) -> None:
        self._visit_carry += self.cfg.visits_per_minute * seconds / 60.0
        while self._visit_carry >= 1.0:
            self._visit_carry -= 1.0
            free_users = [u for u in USER_IDS if u not in self.visits]
            if not free_users:
                return
            user = self.rng.choice(free_users)
            offline_until = None
            if self.rng.random() < self.cfg.offline_ratio:
                offline_until = now + timedelta(
                    seconds=self.rng.uniform(self.cfg.min_offline_s, self.cfg.max_offline_s))
            self.visits[user] = _Visit(
                user_id=user,
                region=self.rng.choice([r.value for r in Region]),
                remaining=self.rng.randint(self.cfg.min_purchases, self.cfg.max_purchases),
                next_purchase_at=now,
                offline_until=offline_until,
            )
            self.stats.visits_started += 1

    def step(self, now: datetime, seconds: float = 1.0) -> List[TransactionEvent]:
        """Advance the shop by `seconds` and return the events sent at `now`."""
        out: List[TransactionEvent] = []
        self._start_visits(now, seconds)

        for user, visit in list(self.visits.items()):
            online = visit.offline_until is None or now >= visit.offline_until

            if online and visit.queue:  # phone back online: flush the local queue
                out.extend(visit.queue)
                self.stats.sent_late_from_offline_queue += len(visit.queue)
                visit.queue = []

            if visit.remaining > 0 and now >= visit.next_purchase_at:
                jitter = 0.0 if not online else self.rng.uniform(0, self.cfg.max_network_jitter_s)
                event = make_event(user, visit.region, now - timedelta(seconds=jitter), self.rng)
                if online:
                    out.append(event)
                    if self.rng.random() < self.cfg.retry_ratio:
                        delay = self.rng.uniform(self.cfg.min_retry_delay_s, self.cfg.max_retry_delay_s)
                        self.retries.append((now + timedelta(seconds=delay), event))
                else:
                    visit.queue.append(event)
                visit.remaining -= 1
                visit.next_purchase_at = now + timedelta(
                    seconds=self.rng.uniform(self.cfg.min_gap_s, self.cfg.max_gap_s))

            if visit.remaining == 0 and not visit.queue:
                del self.visits[user]

        due = [e for (t, e) in self.retries if t <= now]
        self.retries = [(t, e) for (t, e) in self.retries if t > now]
        out.extend(due)
        self.stats.sent_as_retry_duplicate += len(due)

        self.stats.sent += len(out)
        return out
