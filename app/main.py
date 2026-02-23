from fastapi import FastAPI
from dotenv import load_dotenv

from app.api.routes import router as api_router
from app.core.logging import configure_logging, logger
from app.core.config import get_settings



def create_app() -> FastAPI:
    """Construct the FastAPI application with all routes and configuration."""
    # load environment variables from .env file first
    load_dotenv()
    # configure logging early
    configure_logging()
    settings = get_settings()
    logger.info("Starting InsightStream app with project %s", settings.gcp_project_id)

    app = FastAPI(title="InsightStream Mock Producer")
    app.include_router(api_router)
    return app


app = create_app()