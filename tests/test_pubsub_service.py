from datetime import datetime, timezone

from app.models.event_model import Region, TransactionEvent
from app.services.pubsub_service import PubSubService


class DummyFuture:
    def result(self, timeout=None):
        return "message-id"


class DummyPublisher:
    def __init__(self):
        self.calls = []

    def publish(self, topic, data, **attributes):
        self.calls.append((topic, data, attributes))
        return DummyFuture()


def make_service():
    service = PubSubService(topic="projects/p/topics/t")
    service._publisher = DummyPublisher()
    return service


def make_event():
    return TransactionEvent(
        transaction_id="tx-1", user_id="user-001", region=Region.US, product_id="prod-001",
        amount=10.0, currency="usd", event_time=datetime(2026, 11, 12, 10, 3, 0, 123000, tzinfo=timezone.utc),
        schema_version=1,
    )


def test_publish_no_events():
    assert make_service().publish_events([]) == 0


def test_publish_sets_event_time_and_id_attributes():
    service = make_service()
    assert service.publish_events([make_event(), make_event()]) == 2
    _, _, attributes = service._publisher.calls[0]
    assert attributes == {"event_time": "2026-11-12T10:03:00.123Z", "transaction_id": "tx-1"}
