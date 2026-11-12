from datetime import datetime, timedelta, timezone

from app.models.event_model import Region
from app.services.generator_service import generate_transactions


def test_generate_default_region():
    events = generate_transactions(5)
    assert len(events) == 5
    assert all(ev.region in set(Region) for ev in events)


def test_generate_specific_region():
    assert all(ev.region == "EU" for ev in generate_transactions(3, region="EU"))


def test_generate_schema_version():
    assert all(ev.schema_version == 7 for ev in generate_transactions(2, schema_version=7))


def test_event_time_is_recent_and_utc():
    now = datetime.now(timezone.utc)
    for ev in generate_transactions(20, now=now):
        assert now - timedelta(seconds=5) <= ev.event_time <= now
        assert ev.event_time.tzinfo == timezone.utc
