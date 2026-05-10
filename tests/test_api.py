import os
import pytest
from fastapi.testclient import TestClient

# Set before importing app
os.environ["REDIS_URL"] = os.getenv("TEST_REDIS_URL", "redis://localhost:6380")

from api.main import app

def test_health_check():
    # TestClient automatically handles the app lifespan (startup/shutdown)
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "redis": "connected"}
        assert "x-process-time" in response.headers

def test_metrics_initial_state():
    with TestClient(app) as client:
        response = client.get("/metrics")
        assert response.status_code == 200
        data = response.json()
        assert data["req_per_sec"] == 0.0
        assert data["total_requests"] == 0

def test_check_endpoint_validation():
    with TestClient(app) as client:
        # Test missing key
        response = client.post("/check", json={})
        assert response.status_code == 422
        
        # Test valid key
        response = client.post("/check", json={"key": "api_test_user"})
        assert response.status_code == 200
        data = response.json()
        assert data["allowed"] is True
        assert data["algorithm"] in ["sliding_window", "token_bucket", "backoff"]
