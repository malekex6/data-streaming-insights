from datetime import datetime, timezone
from typing import List, Optional
import uuid

from faker import Faker

from app.models.event_model import TransactionEvent, Region


fake = Faker()


PRODUCT_IDS = [f"prod-{i:03d}" for i in range(1, 11)]
USER_IDS = [f"user-{i:03d}" for i in range(1, 21)]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]

def generate_transactions(
    num_events: int, region: Optional[str] = None, schema_version: int = 1
) -> List[TransactionEvent]:
    """Generate a list of transaction events.

    Args:
        num_events: number of events to create
        region: optional region to override random choice
        schema_version: version to assign to each event

    Returns:
        List[TransactionEvent]: generated event objects
    """
    events: List[TransactionEvent] = []

    DATES = [fake.date_this_year() for _ in range(5)]
    for _ in range(num_events):
        chosen_region = region if region else fake.random_element(elements=[r.value for r in Region])
        chosen_date = fake.random_element(elements=DATES)
        event_time = datetime.combine(chosen_date, fake.time_object())
        ingestion_time = datetime.now(timezone.utc)
        chosen_product = fake.random_element(elements=PRODUCT_IDS)
        chosen_user = fake.random_element(elements=USER_IDS)
        chosen_currency = fake.random_element(elements=CURRENCIES)
        event = TransactionEvent(
            transaction_id=str(uuid.uuid4()),
            user_id=chosen_user,
            region=chosen_region,
            product_id=chosen_product,
            amount=round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
            currency=chosen_currency,
            event_time=event_time,
            ingestion_time=ingestion_time,
            schema_version=schema_version,
        )
        events.append(event)
    return events
