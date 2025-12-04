import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal

import requests
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from src.breakers import WEATHER_API_CIRCUIT_BREAKER

LOGGER = logging.getLogger(__name__)

# TODO: Update API and return more fields with the combined weather forecast
class WeatherRecord(BaseModel):
    timestamp_utc: datetime = Field(
        description="Timestamp of the measurement (historical) or forecast (future) in UTC."
    ) 
    temperature_deg_c: float
    rain_fall_total_mm: float
    data_source: Literal["HISTORICAL", "FORECAST"]

class WeatherServiceInterface(ABC):
    @abstractmethod
    def fetch_weather_data(self, city_id: int):
        pass 
    @abstractmethod
    def post_weather_classification(self, city_id: int, classification_label: str):
        pass

class WeatherServiceClient():
    WEATHER_API_BASE_URL = "http://outpost-api-weather:8000"
    @WEATHER_API_CIRCUIT_BREAKER
    @retry(
        stop=stop_after_attempt(5), 
        wait=wait_exponential_jitter(initial=1, max=30, jitter=1),
        reraise=True
    )
    def fetch_weather_data(self, city_id: int) -> list:
        """Fetches combined historical and forecast data from the /window endpoint."""
        window_url = f"{self.WEATHER_API_BASE_URL}/weather/window/{city_id}"
        LOGGER.debug(f"Fetching weather window for city {city_id}...")
        window_response = requests.get(window_url, timeout=5)
        window_response.raise_for_status()
        data: list[dict] = window_response.json()
        historical_weather_data = []
        forecast_weather_data = []
        for datum in data:
            try:
                datum = WeatherRecord(
                    timestamp_utc=datum["timestamp_utc"],
                    temperature_deg_c=datum["temperature_deg_c"],
                    rain_fall_total_mm=datum["rain_fall_total_mm"],
                    data_source=datum["data_source"],
                )
                if datum.data_source.lower() == "historical":
                    historical_weather_data.append(datum)
                elif datum.data_source.lower() == "forecast":
                    forecast_weather_data.append(datum)
                else:
                    LOGGER.warning(f"Invalid data source specified in {datum} returned from {window_url}")
            except Exception as e:
                LOGGER.error(f"Unhandled error unpacking {datum}: {e}", exc_info=True)
        return historical_weather_data, forecast_weather_data
    
    @WEATHER_API_CIRCUIT_BREAKER
    @retry(
        stop=stop_after_attempt(5), 
        wait=wait_exponential_jitter(initial=1, max=30, jitter=1),
        reraise=True
    )
    def post_weather_classification(self, city_id: int, classification_label: str) -> bool:
        """Posts the final classification back to the API."""
        classification_url = f"{self.WEATHER_API_BASE_URL}/cities/{city_id}/classification"
        classification_payload = {
            "city_id": city_id,
            "class_label": classification_label,
        }
        LOGGER.debug(f"Posting classification '{classification_label}' for city {city_id}...")
        post_response = requests.post(classification_url, json=classification_payload, timeout=5)
        post_response.raise_for_status()
        return True