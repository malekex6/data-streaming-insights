from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Region(str, Enum):
    EU = "EU"
    US = "US"
    APAC = "APAC"


class TransactionEvent(BaseModel):
    transaction_id: str = Field(..., description="UUID of the transaction")
    user_id: str = Field(..., description="UUID of the user")
    region: Region
    product_id: str
    amount: float
    currency: str
    event_time: datetime
    ingestion_time: datetime
    schema_version: int = Field(..., ge=1)

    @field_validator("currency")
    def uppercase_currency(cls, v: str) -> str:  # type: ignore[override]
        return v.upper()

    model_config = {"json_schema_extra": {
            "example": {
                "transaction_id": "123e4567-e89b-12d3-a456-426614174000",
                "user_id": "987e6543-e21b-32d3-b456-426614174000",
                "region": "EU",
                "product_id": "prod-001",
                "amount": 49.99,
                "currency": "usd",
                "event_time": "2026-02-18T12:34:56.789Z",
                "ingestion_time": "2026-02-18T12:34:56.789Z",
                "schema_version": 1,
            }
        }}