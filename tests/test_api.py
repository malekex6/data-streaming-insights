from fastapi.testclient import TestClient


def test_generate_endpoint_publishes(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
    monkeypatch.setenv("PUBSUB_TOPIC", "projects/my-project/topics/t")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
    from app.core.config import get_settings
    get_settings.cache_clear()
    from app.services import pubsub_service as ps
    monkeypatch.setattr(ps.pubsub_service, "publish_events", lambda events: len(events))
    from app.main import create_app

    response = TestClient(create_app()).post("/generate", json={"num_events": 3, "region": "EU"})
    assert response.status_code == 200
    assert response.json() == {"published": 3, "requested": 3}
