"""One-shot batch of fake sales, used by POST /generate.

For a realistic stream (customer visits, offline phones, retries) use the
simulator instead: `python -m app.simulate`.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.models.event_model import Region, TransactionEvent

PRODUCT_IDS = [f"prod-{i:03d}" for i in range(1, 11)]
USER_IDS = [f"user-{i:03d}" for i in range(1, 51)]
CURRENCY_BY_REGION = {"EU": "EUR", "US": "USD", "APAC": "JPY"}


def make_event(
    user_id: str,
    region: str,
    event_time: datetime,
    rng: random.Random,
    schema_version: int = 1,
) -> TransactionEvent:
    return TransactionEvent(
        transaction_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        user_id=user_id,
        region=region,
        product_id=rng.choice(PRODUCT_IDS),
        amount=round(rng.uniform(3, 250), 2),
        currency=CURRENCY_BY_REGION[region],
        event_time=event_time,
        schema_version=schema_version,
    )


def generate_transactions(
    num_events: int,
    region: Optional[str] = None,
    schema_version: int = 1,
    now: Optional[datetime] = None,
    rng: Optional[random.Random] = None,
) -> List[TransactionEvent]:
    """Sales that happened in the last few seconds (small network delay only)."""
    rng = rng or random.Random()
    now = now or datetime.now(timezone.utc)
    events = []
    for _ in range(num_events):
        chosen_region = region or rng.choice([r.value for r in Region])
        event_time = now - timedelta(seconds=rng.uniform(0, 5))
        events.append(make_event(rng.choice(USER_IDS), chosen_region, event_time, rng, schema_version))
    return events
