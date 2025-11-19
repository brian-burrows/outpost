from datetime import date, datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse
from src.client import OpenWeatherMapAccessObject


import pytest
import requests

@pytest.fixture
def _mock_open_weather_map_daily_historical_weather() -> dict[str, Any]:
    """
    Mocks the response for the OWM 3.0 day_summary endpoint.
    
    The temperature is determined deterministically by adding the day-of-month
    from the URL to the base temperature (e.g., if date=2025-11-19, temp = BASE_TEMP + 19).
    This ensures that mocking the same date yields the same result, and different dates
    yield different results, without relying on external state.
    """
    def f(url: str, timeout):
        if "day_summary" not in url:
            raise ValueError("Mock called with incorrect URL for daily summary.")
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        try:
            target_date_str = params['date'][0]
            lat = float(params['lat'][0])
            lon = float(params['lon'][0])
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"URL missing required parameters (date, lat, lon): {e}")
        response_data = {
            "lat": lat,
            "lon": lon,
            "tz": "+00:00", # OWM should provide times in UTC
            "date": target_date,
            "units":"metric",
            "wind": {"max": {"speed": 6}, "direction": 120},
            "precipitation": {"total": 2}, # millimeters
            "cloud_cover":{"afternoon":0},
            "humidity":{"afternoon":33},
            "pressure":{"afternoon":1015},
            "temperature":{ # Updated this because we're returning metric (Celcius now), - 273.15
                "min":286.48 - 273.15,
                "max":299.24 - 273.15,
                "afternoon":296.15 - 273.15,
                "night":289.56 - 273.15,
                "evening":295.93 - 273.15,
                "morning":287.59 - 273.15
            },
        }
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        return mock_response
    return f

@pytest.fixture
def _mock_open_weather_map_hourly_forecast() -> dict[str, Any]:
    """
    Mocks the response for the OWM 3.0 onecall endpoint, specifically for hourly forecast.
    It generates a sequence of 4 hourly records using local sequence generation.
    """
    def f(url, timeout):
        BASE_TEMP_K: float = 288.15 
        if "onecall" not in url:
            raise ValueError("Mock called with incorrect URL for hourly forecast.")
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        try:
            lat = float(params['lat'][0])
            lon = float(params['lon'][0])
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"URL missing required parameters (lat, lon): {e}")
        current_time = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())
        hourly_records: list[dict[str, Any]] = []
        for i in range(48):
            hourly_records.append(
                {
                    "dt": current_time + (i * 3600),
                    "temp": BASE_TEMP_K + (i * 2.0)  - 273.15,
                    "wind_speed": 4.1 - (i * 0.2), 
                    "rain": {"1h": 2.5} if i % 2 == 0 else None, 
                    "feels_like":292.33 - 273.15,
                    "pressure":1014,
                    "humidity":91,
                    "dew_point":290.51  - 273.15,
                    "uvi":0,
                    "clouds":54,
                    "visibility":10000,
                    "wind_deg":86,
                    "wind_gust":5.88,
                    "weather":[
                        {
                        "id":803,
                        "main":"Clouds",
                        "description":"broken clouds",
                        "icon":"04n"
                        }
                    ],
                    "pop":0.15
                }
            )
        response_data = {
            "lat": lat,
            "lon": lon,
            "timezone": "America/Chicago",
            "timezone_offset":-18000,
            "hourly": hourly_records,
        }
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        return mock_response
    return f
