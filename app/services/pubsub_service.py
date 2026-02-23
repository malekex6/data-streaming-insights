import json
import logging
from typing import List

from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError

from app.core.config import get_settings
from app.models.event_model import TransactionEvent


logger = logging.getLogger(__name__)
settings = get_settings()


class PubSubService:
    def __init__(self, topic: str = settings.pubsub_topic) -> None:
        # delay client creation so importing the module doesn't require
        # valid ADC (useful for unit tests and local startup without creds)
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._topic = topic

    def _get_publisher(self) -> pubsub_v1.PublisherClient:
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    def publish_events(self, events: List[TransactionEvent]) -> int:
        """Publish a list of transaction events to Pub/Sub.

        Args:
            events: list of validated TransactionEvent objects

        Returns:
            int: number of messages successfully published
        """
        published_count = 0
        for event in events:
            data = json.dumps(event.model_dump(), default=str).encode("utf-8")
            try:
                publisher = self._get_publisher()
                future = publisher.publish(self._topic, data)
                # we could add callbacks or result() to ensure delivery
                future.result(timeout=10)
                published_count += 1
            except GoogleAPIError as exc:
                logger.error("Failed to publish event %s: %s", event.transaction_id, exc)
            except Exception as exc:  # pragma: no cover - broad catch
                logger.exception("Unexpected error publishing event %s", event.transaction_id)
        return published_count


# provide a singleton for convenience
pubsub_service = PubSubService()