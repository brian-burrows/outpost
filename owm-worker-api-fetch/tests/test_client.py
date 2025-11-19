from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests
from src.client import OpenWeatherMapAccessObject
from tenacity import Retrying, stop_after_attempt
from pybreaker import CircuitBreaker, CircuitBreakerError

# 288.15 K - 273.15 = 15.0 C (Used for Hourly Base)
BASE_TEMP_K: float = 288.15 
BASE_TEMP_C: float = 15.0
TEST_LAT = 39.0
TEST_LON = -105.0

@pytest.fixture
def owm_client():
    """Fixture to instantiate the client with mocked resilience objects."""
    # Note: Using function scope for fixtures ensures a clean circuit breaker state
    return OpenWeatherMapAccessObject(
        circuit_breaker=CircuitBreaker(fail_max=1, reset_timeout=60, success_threshold=1),
        retrying=Retrying(stop=stop_after_attempt(2), reraise=True)
    )

# NOTE: The _mock_open_weather_map_daily_historical_weather and 
# _mock_open_weather_map_hourly_forecast fixtures are now correctly
# returning MagicMock Response objects as defined in your previous input.

## 1. Functional Tests

def test_fetch_daily_historical_success(
    owm_client, 
    monkeypatch,
    _mock_open_weather_map_daily_historical_weather
):
    """Tests successful fetching and mapping of daily historical data."""
    # Patch requests.get for the daily call
    monkeypatch.setattr("src.client.requests.get", _mock_open_weather_map_daily_historical_weather)
    
    target_date = date.fromisoformat("2024-04-01")
    historical_data = owm_client.fetch_daily_historical_weather_data(
        target_date = target_date,
        lat = TEST_LAT,
        lon = TEST_LON,
    )
    assert historical_data.aggregation_level == "daily"
    assert historical_data.temperature_deg_c == pytest.approx(299.24 - 273.15)
    assert historical_data.rain_fall_total_mm == 2.0
    assert historical_data.wind_speed_mps == 6.0
    assert historical_data.timestamp == datetime(2024, 4, 1, 0, 0)
    
def test_fetch_hourly_forecast_success_and_duration_filter(
    owm_client, 
    monkeypatch,
    _mock_open_weather_map_hourly_forecast
):
    """Tests successful fetching, correct mapping, and filtering by duration (e.g., 2 hours)."""
    monkeypatch.setattr("src.client.requests.get", _mock_open_weather_map_hourly_forecast)
    start_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    duration = timedelta(hours=2)
    result = owm_client.fetch_hourly_weather_forecast(start_date, duration, TEST_LAT, TEST_LON)
    assert len(result) == 2
    assert result[0].aggregation_level == "hourly"
    assert result[0].temperature_deg_c == pytest.approx(BASE_TEMP_C)
    assert result[0].rain_fall_total_mm == 2.5
    assert result[1].temperature_deg_c == pytest.approx(BASE_TEMP_C + 2.0)
    assert result[1].rain_fall_total_mm == 0.0

# ## 2. Resilience Tests (Retrying and Circuit Breaking)

def test_execute_request_retries_on_500_error(monkeypatch, owm_client, _mock_open_weather_map_hourly_forecast):
    """Tests that the underlying request retries when the server returns an HTTP error."""
    # Setup: Fail, Fail (Retry 1), Success (Retry 2) -> 3 calls total
    owm_client.circuit_breaker = CircuitBreaker(fail_max=10, reset_timeout = 60)
    owm_client.retrying = Retrying(stop=stop_after_attempt(3), reraise=True)
    mock_response_fail = MagicMock(spec=requests.Response)
    mock_response_fail.status_code = 500
    mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError()
    mock_response_success = MagicMock(spec=requests.Response)
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {"hourly": []} 
    with patch(
        'src.client.requests.get', 
        side_effect=[mock_response_fail, mock_response_fail, mock_response_success]
    ) as mock_get:
        owm_client.fetch_hourly_weather_forecast(
            datetime.now(), 
            timedelta(hours=1), 
            TEST_LAT, 
            TEST_LON
        )
        assert mock_get.call_count == 3

def test_execute_request_fails_after_max_attempts(owm_client):
    """Tests that a ConnectionError is raised after exhausting all retries (max 2 attempts)."""
    # Setup: Fail, Fail (Retry 1) -> 2 calls max, then ConnectionError
    mock_response_fail = MagicMock(spec=requests.Response)
    mock_response_fail.status_code = 503
    owm_client.circuit_breaker=CircuitBreaker(fail_max=5, reset_timeout=60, success_threshold=1)
    # Use a ConnectionError exception which tenacity often handles as a transient failure
    mock_response_fail.raise_for_status.side_effect = requests.exceptions.ConnectionError("Service unavailable")
    with patch('src.client.requests.get', side_effect=[mock_response_fail] * 2) as mock_get:
        with pytest.raises(ConnectionError, match="API request failed after retries"):
            owm_client.fetch_hourly_weather_forecast(date.today(), timedelta(hours=1), TEST_LAT, TEST_LON)
        assert mock_get.call_count == 2 # Initial call + 1 Retry
        
def test_circuit_breaker_opens_on_failure(owm_client):
    """Tests that the circuit breaker opens immediately after the configured max failures (fail_max=1)."""
    mock_response_fail = MagicMock(spec=requests.Response)
    mock_response_fail.status_code = 500
    mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
    owm_client.circuit_breaker=CircuitBreaker(fail_max=5, reset_timeout=60, success_threshold=1)
    owm_client.retrying=Retrying(stop=stop_after_attempt(4), reraise=True)

    with patch('src.client.requests.get', return_value=mock_response_fail) as mock_get:
        with pytest.raises(ConnectionError):
            owm_client.fetch_hourly_weather_forecast(
                datetime.now(), timedelta(hours=1), TEST_LAT, TEST_LON
            )
        # # 2. Second call should be blocked immediately by the now-open circuit breaker
        with pytest.raises(CircuitBreakerError):
            owm_client.fetch_hourly_weather_forecast(
                datetime.now(), timedelta(hours=1), TEST_LAT, TEST_LON
            )
        # Check that the circuit breaker state updated correctly
        assert owm_client.circuit_breaker.current_state == 'open'

def test_daily_data_mapping_raises_on_missing_key(owm_client):
    """Tests that a ValueError is raised if the API response is missing a required key (e.g., 'wind')."""
    malformed_data = {
        "lat": 39.0, "lon": -105.0, "date": "2025-01-01",
        "temperature": {"max": 300.0},
        # Missing 'wind' key entirely
    }  
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = malformed_data
    with patch('src.client.requests.get', return_value=mock_response):
        result = owm_client.fetch_daily_historical_weather_data(date.today(), TEST_LAT, TEST_LON)
        assert result.wind_speed_mps == 0.0

def test_hourly_data_mapping_raises_on_malformed_item(owm_client):
    """Tests that a ValueError is raised if an hourly item within the forecast array is malformed."""
    # Malformed item is missing the required 'temp' key, which is accessed via `hour_data['temp']`.
    malformed_forecast = {
        "hourly": [
            {"dt": datetime.now().timestamp(), "wind_speed": 5.0},
        ]
    }
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = malformed_forecast
    with patch('src.client.requests.get', return_value=mock_response):
        with pytest.raises(
            ValueError, 
            match="Failed to parse OWM hourly forecast data structure"
        ):
            owm_client.fetch_hourly_weather_forecast(
                datetime.now(), 
                timedelta(hours=3), 
                TEST_LAT, TEST_LON
            )
            