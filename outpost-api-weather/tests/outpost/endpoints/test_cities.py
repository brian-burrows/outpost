import asyncio
from typing import List, Dict, Optional

import pytest
import pytest_asyncio
from httpx import AsyncClient

async def _insert_city(
    client: AsyncClient, 
    name: str, 
    state_name: str,
    lat: float, 
    lon: float
) -> Dict:
    """Inserts a single city record and returns its JSON response."""
    city_data = {
        "city_name": name,
        "state_name": state_name,
        "latitude_deg": lat,
        "longitude_deg": lon,
    }
    response = await client.post("/cities/", json=city_data)
    assert response.status_code == 201
    return response.json()

@pytest_asyncio.fixture(scope="function")
async def five_cities_setup(test_client: AsyncClient) -> List[Dict]:
    """
    Sets up exactly 5 cities in the database and returns the sorted list
    of the created city objects.
    """
    setup_cities = [
        {"name": f"City_{i}", "state_name": "Colorado", "lat": 10.0 + i, "lon": 20.0 + i} for i in range(5)
    ]
    created_cities = []
    for city in setup_cities:
        res_json = await _insert_city(test_client, **city)
        created_cities.append(res_json)
    created_cities.sort(key=lambda c: c["city_id"])
    return created_cities

@pytest.mark.asyncio
async def test_get_city_detail_success(test_client: AsyncClient):
    """
    Tests successful retrieval of a city by ID. (No changes needed here)
    """
    city_data = {
        "city_name": "Denver",
        "state_name": "Colorado",
        "latitude_deg": 39.7392,
        "longitude_deg": 143.15235
    }
    setup_response = await test_client.post("/cities/", json=city_data)
    assert setup_response.status_code == 201, "Setup via POST failed."
    test_id = setup_response.json()["city_id"]
    response = await test_client.get(f"/cities/{test_id}")
    assert response.status_code == 200
    rj = response.json()
    assert rj["city_name"] == city_data["city_name"]
    assert rj["state_name"] == city_data["state_name"]
    assert rj["latitude_deg"] == pytest.approx(city_data["latitude_deg"])
    assert rj["longitude_deg"] == pytest.approx(city_data["longitude_deg"])


@pytest.mark.asyncio
async def test_get_city_detail_not_found(test_client: AsyncClient):
    """
    Tests 404 response when a city is not found. (No changes needed here)
    """
    response = await test_client.get("/cities/999")
    assert response.status_code == 404
    assert response.json()["detail"] == "City not found"


@pytest.mark.asyncio
async def test_post_city_success(test_client: AsyncClient):
    """
    Tests successful creation of a new city. (No changes needed here)
    """
    city_data = {
        "city_name": "Boulder",
        "state_name": "Colorado",
        "latitude_deg": 40.0150,
        "longitude_deg": 90.51234,
    }
    response = await test_client.post("/cities/", json=city_data)
    assert response.status_code == 201
    city_id = response.json()['city_id']
    verification_response = await test_client.get(f"/cities/{city_id}")
    assert verification_response.status_code == 200
    rj = verification_response.json()
    assert rj["city_name"] == city_data["city_name"]
    assert rj["state_name"] == city_data["state_name"]
    assert rj["latitude_deg"] == pytest.approx(city_data["latitude_deg"])
    assert rj["longitude_deg"] == pytest.approx(city_data["longitude_deg"])


@pytest.mark.asyncio
async def test_post_city_validation_failure(test_client: AsyncClient):
    """Tests Pydantic validation failure (422). (No changes needed here)"""
    response = await test_client.post("/cities/", json={
        "city_name": "Invalid City"
    })
    assert response.status_code == 422
    assert "Field required" in response.json()["detail"][0]["msg"]

@pytest.mark.asyncio
async def test_list_cities_empty(test_client: AsyncClient):
    """Tests retrieval when no cities are present."""
    response = await test_client.get("/cities/")
    assert response.status_code == 200
    rj = response.json()
    assert rj["total_count"] == 0 
    assert rj["cities"] == []

@pytest.mark.asyncio
async def test_list_cities_basic_retrieval(test_client: AsyncClient):
    """Tests successful retrieval of cities without using pagination params."""
    cities_to_insert = [
        {"name": "Aachen", "state_name": "Colorado", "lat": 50.7753, "lon": 6.0839},
        {"name": "Berlin", "state_name": "Colorado", "lat": 52.5200, "lon": 13.4050},
    ]
    await asyncio.gather(*[_insert_city(test_client, **city) for city in cities_to_insert])
    response = await test_client.get("/cities/")
    assert response.status_code == 200
    rj = response.json()
    assert rj["total_count"] >= 2
    cities_list = rj["cities"] 
    assert len(cities_list) >= 2 
    city_names = {city["city_name"] for city in cities_list}
    assert "Aachen" in city_names
    assert all("city_id" in city for city in cities_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit, page, expected_count, expected_first_index", 
    [
        (3, 0, 3, 0),    
        (3, 1, 2, 3),    
        (3, 2, 0, None), 
        (10, 0, 5, 0),   
        (1, 0, 1, 0),    
    ]
)
async def test_list_cities_pagination_checks(
    test_client: AsyncClient,
    five_cities_setup: List[Dict],
    limit: int,
    page: int,
    expected_count: int,
    expected_first_index: Optional[int],
):
    """Tests limit and page parameters using the pre-inserted five_cities_setup data."""
    created_cities = five_cities_setup 
    query_params = f"limit={limit}&page={page}"
    response = await test_client.get(f"/cities/?{query_params}")
    assert response.status_code == 200
    rj = response.json()
    assert rj["total_count"] == 5
    assert rj["limit"] == limit
    assert rj["page"] == page
    cities_list = rj["cities"]
    assert len(cities_list) == expected_count, (
        f"Limit={limit}, Page={page} failed count check. "
        f"Expected {expected_count}, got {len(cities_list)}"
    )
    if expected_count > 0:
        expected_city = created_cities[expected_first_index]
        assert cities_list[0]["city_id"] == expected_city["city_id"], (
            f"Limit={limit}, Page={page} failed ID check. "
            f"Expected ID {expected_city['city_id']}, got {cities_list[0]['city_id']}"
        )