from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class Region(str, Enum):
    EU = "EU"
    US = "US"
    APAC = "APAC"


class TransactionEvent(BaseModel):
    """One sale, as the shop's app or website knows it.

    The event carries only what the source really knows: what was sold and
    WHEN it was sold (event_time). The arrival time is NOT part of the event:
    Pub/Sub stamps it (publish_time) when the message reaches the platform, so
    a device with a wrong clock or a long offline queue cannot fake it.
    """

    transaction_id: str = Field(..., description="UUID of the sale. Idempotency key end to end.")
    user_id: str = Field(..., description="Customer id. Key of the session window.")
    region: Region
    product_id: str
    amount: float = Field(..., gt=0)
    currency: str
    event_time: datetime = Field(..., description="When the sale happened, in UTC.")
    schema_version: int = Field(..., ge=1)

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v: str) -> str:
        return v.upper()

    @field_validator("event_time")
    @classmethod
    def force_utc(cls, v: datetime) -> datetime:
        # A naive datetime is ambiguous; we treat it as UTC so the RFC3339
        # value sent to Pub/Sub always ends with "Z".
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)

    def event_time_rfc3339(self) -> str:
        """event_time as RFC3339 with milliseconds, e.g. 2026-11-12T10:03:00.123Z.

        This exact string goes into the Pub/Sub attribute that Beam reads with
        timestamp_attribute="event_time"; Beam accepts RFC3339 or epoch millis.
        """
        return self.event_time.strftime("%Y-%m-%dT%H:%M:%S.") + f"{self.event_time.microsecond // 1000:03d}Z"

    def to_payload(self) -> dict:
        data = self.model_dump()
        data["region"] = self.region.value
        data["event_time"] = self.event_time_rfc3339()
        return data

    model_config = {
        "json_schema_extra": {
            "example": {
                "transaction_id": "123e4567-e89b-12d3-a456-426614174000",
                "user_id": "user-007",
                "region": "EU",
                "product_id": "prod-001",
                "amount": 49.99,
                "currency": "eur",
                "event_time": "2026-11-12T10:03:00.123Z",
                "schema_version": 1,
            }
        }
    }
