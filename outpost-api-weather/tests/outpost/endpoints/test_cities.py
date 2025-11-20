import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_get_city_detail_success(test_client: AsyncClient):
    """
    Tests successful retrieval of a city by:
    1. Inserting the record using the POST endpoint.
    2. Retrieving the record using the GET endpoint.
    """
    test_id = 101
    city_data = {
        "city_id": test_id,
        "city_name": "Denver",
        "latitude_deg": 39.7392,
        "longitude_deg": 143.15235
    }
    setup_response = await test_client.post("/cities/", json=city_data)
    assert setup_response.status_code == 200, "Setup via POST failed."
    response = await test_client.get(f"/cities/{test_id}")
    assert response.status_code == 200
    rj = response.json()
    assert rj["city_name"] == city_data["city_name"]
    assert rj["latitude_deg"] == pytest.approx(city_data["latitude_deg"])
    assert rj["longitude_deg"] == pytest.approx(city_data["longitude_deg"])


@pytest.mark.asyncio
async def test_get_city_detail_not_found(test_client: AsyncClient):
    """
    Tests 404 response when a city is not found. 
    The database starts empty for this test (due to the fixture scope).
    """
    response = await test_client.get("/cities/999")
    assert response.status_code == 404
    assert response.json()["detail"] == "City not found"


@pytest.mark.asyncio
async def test_post_city_success(test_client: AsyncClient):
    """
    Tests successful creation of a new city by:
    1. Posting the data.
    2. Verifying the data was correctly saved using the GET endpoint.
    """
    city_data = {
        "city_id": 202,
        "city_name": "Boulder",
        "latitude_deg": 40.0150,
        "longitude_deg": 90.51234,
    }
    response = await test_client.post("/cities/", json=city_data)
    assert response.status_code == 200
    assert response.json()["id"] == city_data["city_id"]
    verification_response = await test_client.get(f"/cities/{city_data['city_id']}")
    assert verification_response.status_code == 200
    rj = verification_response.json()
    assert rj["city_name"] == city_data["city_name"]
    assert rj["latitude_deg"] == pytest.approx(city_data["latitude_deg"])
    assert rj["longitude_deg"] == pytest.approx(city_data["longitude_deg"])


@pytest.mark.asyncio
async def test_post_city_validation_failure(test_client: AsyncClient):
    """Tests Pydantic validation failure without needing DB interaction."""
    response = await test_client.post("/cities/", json={
        "city_name": "Invalid City"
    })
    assert response.status_code == 422
    assert "Field required" in response.json()["detail"][0]["msg"]