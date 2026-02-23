import pytest

from app.services import pubsub_service
from app.models.event_model import TransactionEvent, Region
from datetime import datetime


def test_publish_no_events(monkeypatch):
    # monkeypatch publisher client to avoid real network call
    class DummyFuture:
        def result(self, timeout=None):
            return None

    class DummyPublisher:
        def publish(self, topic, data):
            return DummyFuture()

    monkeypatch.setattr(pubsub_service, "_publisher", DummyPublisher())

    result = pubsub_service.publish_events([])
    assert result == 0


def make_event():
    return TransactionEvent(
        transaction_id="1",
        user_id="2",
        region=Region.US,
        product_id="prod-001",
        amount=10.0,
        currency="USD",
        event_time=datetime.utcnow(),
        ingestion_time=datetime.utcnow(),
        schema_version=1,
    )


def test_publish_success(monkeypatch):
    class DummyFuture:
        def result(self, timeout=None):
            return None

    class DummyPublisher:
        def publish(self, topic, data):
            return DummyFuture()

    monkeypatch.setattr(pubsub_service, "_publisher", DummyPublisher())
    events = [make_event(), make_event()]
    count = pubsub_service.publish_events(events)
    assert count == len(events)
