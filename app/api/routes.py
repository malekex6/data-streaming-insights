from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.logging import logger
from app.services.generator_service import generate_transactions
from app.services.pubsub_service import pubsub_service

router = APIRouter()


class GenerateRequest(BaseModel):
    num_events: int = Field(..., gt=0, le=10_000)
    region: Optional[str] = None


class GenerateResponse(BaseModel):
    published: int
    requested: int


@router.post("/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest):
    """Publish a one-shot batch of sales that happened in the last few seconds.

    For late data, sessions and duplicates, run `python -m app.simulate`.
    """
    logger.info("Received generate request: %s", request.model_dump())
    try:
        events = generate_transactions(request.num_events, request.region)
    except Exception:
        logger.exception("Error generating events")
        raise HTTPException(status_code=500, detail="Failed to generate events")

    published = pubsub_service.publish_events(events)
    if published < len(events):
        logger.warning("Some messages failed to publish: %s/%s", published, len(events))
    return GenerateResponse(published=published, requested=len(events))
