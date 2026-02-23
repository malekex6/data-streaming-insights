import os
from functools import lru_cache

# BaseSettings was moved to pydantic-settings in v2
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    gcp_project_id: str
    pubsub_topic: str
    google_application_credentials: str
    default_region: str = "US"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }  # type: ignore


@lru_cache()

def get_settings() -> Settings:
    return Settings()
