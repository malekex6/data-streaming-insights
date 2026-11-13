from functools import lru_cache
from typing import Optional

# BaseSettings was moved to pydantic-settings in v2
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    gcp_project_id: str
    pubsub_topic: str
    # Path to a key file. Leave it out when gcloud already gives credentials
    # (Cloud Shell, or `gcloud auth application-default login`).
    google_application_credentials: Optional[str] = None
    default_region: str = "US"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }  # type: ignore


@lru_cache()

def get_settings() -> Settings:
    return Settings()
