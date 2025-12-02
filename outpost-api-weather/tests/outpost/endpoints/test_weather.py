import pytest
from httpx import AsyncClient
from datetime import datetime, timezone, timedelta

TEST_CITY_ID = 500
TEST_CITY_DATA = {
    "city_name": "Testville",
    "state_name": "Teststate",
    "latitude_deg": 40.0,
    "longitude_deg": -105.0
}
NOW = datetime.now(timezone.utc)
NOW_ISO = NOW.isoformat().replace('+00:00', 'Z')
FUTURE_1H_ISO = (NOW + timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
FUTURE_2H_ISO = (NOW + timedelta(hours=2)).isoformat().replace('+00:00', 'Z')
EARLIER_NOW_ISO = (NOW - timedelta(minutes=5)).isoformat().replace('+00:00', 'Z')
PAST_1H_ISO = (NOW - timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
PAST_2H_ISO = (NOW - timedelta(hours=2)).isoformat().replace('+00:00', 'Z')

async def setup_city_prerequisite(test_client: AsyncClient):
    """Utility to ensure the required city record exists for foreign key constraints."""
    response = await test_client.post("/cities/", json=TEST_CITY_DATA)
    assert response.status_code == 201, "Failed to create prerequisite city for weather tests."
    return response.json()["city_id"]

@pytest.mark.asyncio
async def test_post_forecast_batch_success(test_client: AsyncClient):
    """Tests successful batch creation of new forecast records."""
    TEST_CITY_ID = await setup_city_prerequisite(test_client)
    forecast_batch = [
        {
            "city_id": TEST_CITY_ID,
            "temperature_deg_c": 15.5,
            "wind_speed_mps": 4.2,
            "rain_fall_total_mm": 0.5,
            "aggregation_level": "hourly",
            "forecast_generated_at_ts_utc": NOW_ISO,
            "forecast_timestamp_utc": FUTURE_1H_ISO
        },
        {
            "city_id": TEST_CITY_ID,
            "temperature_deg_c": 17.0,
            "wind_speed_mps": 5.0,
            "rain_fall_total_mm": 0.0,
            "aggregation_level": "hourly",
            "forecast_generated_at_ts_utc": NOW_ISO,
            "forecast_timestamp_utc": FUTURE_2H_ISO
        }
    ]
    response = await test_client.post("/weather/forecast", json=forecast_batch)
    assert response.status_code == 201
    rj = response.json()
    assert rj["message"] == f"Successfully ingested {len(forecast_batch)} forecast records."
    assert rj["city_id"] == TEST_CITY_ID

@pytest.mark.asyncio
async def test_post_forecast_batch_empty_failure(test_client: AsyncClient):
    """Tests failure when an empty list is sent to the batch POST endpoint."""
    response = await test_client.post("/weather/forecast", json=[])
    assert response.status_code == 400
    assert response.json()["detail"] == "Input list cannot be empty."

@pytest.mark.asyncio
async def test_post_forecast_validation_failure(test_client: AsyncClient):
    """Tests Pydantic validation failure on missing required fields."""
    response = await test_client.post("/weather/forecast", json=[
        {
            "city_id": TEST_CITY_ID,
            "temperature_deg_c": 15.5,
        }
    ])
    assert response.status_code == 422
    details = response.json()["detail"]
    assert any("wind_speed_mps" in item["loc"] for item in details)
    assert any("aggregation_level" in item["loc"] for item in details)

@pytest.mark.asyncio
async def test_get_latest_forecast_success(test_client: AsyncClient):
    """
    Tests successful retrieval of the single, *latest* forecast record 
    for a given city and aggregation level.
    """
    TEST_CITY_ID = await setup_city_prerequisite(test_client)
    old_forecast = [
        {
            "city_id": TEST_CITY_ID,
            "temperature_deg_c": 5.0,
            "wind_speed_mps": 1.0,
            "rain_fall_total_mm": 0.0,
            "aggregation_level": "daily",
            "forecast_generated_at_ts_utc": EARLIER_NOW_ISO,
            "forecast_timestamp_utc": FUTURE_1H_ISO
        }
    ]
    await test_client.post("/weather/forecast", json=old_forecast)
    latest_forecast_data = {
        "city_id": TEST_CITY_ID,
        "temperature_deg_c": 25.5,
        "wind_speed_mps": 8.0,
        "rain_fall_total_mm": 10.0,
        "aggregation_level": "daily",
        "forecast_generated_at_ts_utc": NOW_ISO,
        "forecast_timestamp_utc": FUTURE_1H_ISO
    }
    await test_client.post("/weather/forecast", json=[latest_forecast_data])
    response = await test_client.get(f"/weather/forecast/{TEST_CITY_ID}?aggregation_level=daily")
    assert response.status_code == 200
    rj = response.json()
    assert rj["forecast_generated_at_ts_utc"].startswith(NOW_ISO[:23])
    assert rj["temperature_deg_c"] == pytest.approx(latest_forecast_data["temperature_deg_c"])
    assert rj["rain_fall_total_mm"] == pytest.approx(latest_forecast_data["rain_fall_total_mm"])


@pytest.mark.asyncio
async def test_get_latest_forecast_not_found(test_client: AsyncClient):
    """Tests 404 response when a forecast is not found (e.g., wrong aggregation level)."""
    TEST_CITY_ID = await setup_city_prerequisite(test_client)
    daily_forecast = [
        {
            "city_id": TEST_CITY_ID,
            "temperature_deg_c": 10.0,
            "wind_speed_mps": 1.0,
            "rain_fall_total_mm": 0.0,
            "aggregation_level": "daily",
            "forecast_generated_at_ts_utc": NOW_ISO, 
            "forecast_timestamp_utc": FUTURE_1H_ISO
        }
    ]
    await test_client.post("/weather/forecast", json=daily_forecast)
    response = await test_client.get(f"/weather/forecast/{TEST_CITY_ID}?aggregation_level=hourly")
    assert response.status_code == 404
    assert "Forecast not found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_post_historical_success(test_client: AsyncClient):
    """Tests successful single insertion of a historical record."""
    TEST_CITY_ID = await setup_city_prerequisite(test_client)
    historical_data = {
        "city_id": TEST_CITY_ID,
        "temperature_deg_c": 12.1,
        "wind_speed_mps": 3.0,
        "rain_fall_total_mm": 2.0,
        "aggregation_level": "hourly",
        "measured_at_ts_utc": PAST_1H_ISO
    }
    response = await test_client.post("/weather/historical", json=historical_data)
    assert response.status_code == 201
    assert response.json()["message"] == "Historical data ingested successfully"
    assert response.json()["city_id"] == TEST_CITY_ID
    verification_response = await test_client.get(f"/weather/historical/{TEST_CITY_ID}?aggregation_level=hourly&limit=1")
    assert verification_response.status_code == 200
    retrieved_data = verification_response.json()[0]
    assert retrieved_data["temperature_deg_c"] == pytest.approx(historical_data["temperature_deg_c"])

@pytest.mark.asyncio
async def test_get_historical_multiple_records_ordered(test_client: AsyncClient):
    """
    Tests retrieval of multiple historical records, verifying correct ordering (DESC by timestamp).
    """
    TEST_CITY_ID = await setup_city_prerequisite(test_client)
    record_older = {
        "city_id": TEST_CITY_ID,
        "temperature_deg_c": 10.0,
        "wind_speed_mps": 1.0,
        "rain_fall_total_mm": 0.0,
        "aggregation_level": "daily",
        "measured_at_ts_utc": PAST_2H_ISO
    }
    record_newer = {
        "city_id": TEST_CITY_ID,
        "temperature_deg_c": 20.0,
        "wind_speed_mps": 5.0,
        "rain_fall_total_mm": 5.0,
        "aggregation_level": "daily",
        "measured_at_ts_utc": PAST_1H_ISO
    }
    await test_client.post("/weather/historical", json=record_older)
    await test_client.post("/weather/historical", json=record_newer)
    response = await test_client.get(f"/weather/historical/{TEST_CITY_ID}?aggregation_level=daily&limit=2")
    assert response.status_code == 200
    retrieved_data = response.json()
    assert len(retrieved_data) == 2 
    assert retrieved_data[0]["temperature_deg_c"] == pytest.approx(record_newer["temperature_deg_c"])
    assert retrieved_data[0]["measured_at_ts_utc"].startswith(PAST_1H_ISO[:23])
    assert retrieved_data[1]["temperature_deg_c"] == pytest.approx(record_older["temperature_deg_c"])
    assert retrieved_data[1]["measured_at_ts_utc"].startswith(PAST_2H_ISO[:23])


@pytest.mark.asyncio
async def test_get_historical_not_found(test_client: AsyncClient):
    """Tests 404 response when no historical data exists for a city/level."""
    await setup_city_prerequisite(test_client)
    response = await test_client.get(f"/weather/historical/{TEST_CITY_ID}?aggregation_level=hourly")
    assert response.status_code == 404
    assert "No historical data found" in response.json()["detail"]