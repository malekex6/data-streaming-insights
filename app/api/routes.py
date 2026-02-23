from typing import Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from app.services.generator_service import generate_transactions
from app.services.pubsub_service import pubsub_service
from app.core.logging import logger

router = APIRouter()


class GenerateRequest(BaseModel):
    num_events: int = Field(..., gt=0)  # must be positive
    region: Optional[str] = None  # optional

class GenerateResponse(BaseModel):
    published: int  # published count
    requested: int  # requested count


@router.post("/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest):
    """Endpoint to create transactions and publish to Pub/Sub."""
    logger.info("Received generate request: %s", request.dict())

    # generate events
    try:
        events = generate_transactions(request.num_events, request.region)
    except Exception as exc:
        logger.exception("Error generating events")
        raise HTTPException(status_code=500, detail="Failed to generate events")

    # publish events
    published = pubsub_service.publish_events(events)
    if published < len(events):
        logger.warning("Some messages failed to publish: %s/%s",
                       published, len(events))

    return GenerateResponse(published=published, requested=len(events))