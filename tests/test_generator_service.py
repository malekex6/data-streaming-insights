from app.services.generator_service import generate_transactions
from app.models.event_model import Region


def test_generate_default_region():
    events = generate_transactions(5)
    assert len(events) == 5
    # regions should be valid
    for ev in events:
        assert ev.region in {r.value for r in Region}


def test_generate_specific_region():
    region = "EU"
    events = generate_transactions(3, region=region)
    assert all(ev.region == region for ev in events)


def test_generate_schema_version():
    version = 7
    events = generate_transactions(2, schema_version=version)
    assert all(ev.schema_version == version for ev in events)
