import json
import logging
from typing import List, Optional

from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

from app.models.event_model import TransactionEvent

logger = logging.getLogger(__name__)


class PubSubService:
    """Publishes sales to Pub/Sub.

    Two attributes ride on every message, outside the JSON body:
    - event_time: read by Beam with timestamp_attribute="event_time", so every
      element is stamped with WHEN THE SALE HAPPENED, not when it arrived.
    - transaction_id: read by Dataflow with id_label="transaction_id", so
      Dataflow drops re-sent copies of the same sale at the source.
    """

    def __init__(self, topic: Optional[str] = None) -> None:
        # Client and settings are created lazily so importing this module needs
        # no credentials (unit tests, local start without a .env).
        self._publisher: Optional[pubsub_v1.PublisherClient] = None
        self._topic = topic

    def _get_publisher(self) -> pubsub_v1.PublisherClient:
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    def _get_topic(self) -> str:
        if self._topic is None:
            from app.core.config import get_settings
            self._topic = get_settings().pubsub_topic
        return self._topic

    def publish_events(self, events: List[TransactionEvent]) -> int:
        """Publish events and return how many Pub/Sub acknowledged."""
        if not events:
            return 0
        publisher, topic = self._get_publisher(), self._get_topic()
        futures = []
        for event in events:
            data = json.dumps(event.to_payload()).encode("utf-8")
            futures.append((event, publisher.publish(
                topic,
                data,
                event_time=event.event_time_rfc3339(),
                transaction_id=event.transaction_id,
            )))
        published = 0
        for event, future in futures:
            try:
                future.result(timeout=30)
                published += 1
            except GoogleAPIError as exc:
                logger.error("Failed to publish event %s: %s", event.transaction_id, exc)
            except Exception:  # pragma: no cover - broad catch
                logger.exception("Unexpected error publishing event %s", event.transaction_id)
        return published


pubsub_service = PubSubService()
