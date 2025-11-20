import pytest
from httpx import AsyncClient
from main import app

@pytest.mark.asyncio
async def test_root_endpoint_success(test_client: AsyncClient):
    """
    Tests the '/' endpoint to ensure it returns the correct status and 
    application metadata (service_name and version).
    """
    response = await test_client.get("/")
    assert response.status_code == 200
    rj = response.json()
    assert rj["status"] == "Running"
    assert "message" in rj
    assert rj["service_name"] == app.title
    assert rj["version"] == app.version

@pytest.mark.asyncio
async def test_health_check_ok_status(test_client: AsyncClient):
    """
    Tests the '/health' endpoint for the default success case (all_healthy = True).
    Since the current implementation always sets 'all_healthy = True', this tests the nominal path.
    """
    response = await test_client.get("/health")
    assert response.status_code == 200
    rj = response.json()
    assert rj["status"] == "ok"
    assert rj["dependencies"] == {}