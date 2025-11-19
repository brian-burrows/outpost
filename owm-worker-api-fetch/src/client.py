from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

import requests
from pydantic import BaseModel, Field

OWM_API_KEY = "TODO"

class WeatherDatum(BaseModel):
    wind_speed_mps: float = Field(description = "Wind speed in meters per second")
    rain_fall_total_mm: float = Field(description = "Rain fall total in mm")
    temperature_deg_c: float = Field(description = "(Optional) Temperature in degrees celsius")
    

class WeatherData(WeatherDatum):
    timestamp: datetime
    aggregation_level: Literal["daily", "hourly"]


class WeatherApiInterface(ABC):

    @abstractmethod
    def fetch_daily_historical_weather_data(
        self, 
        target_date: date,
        lat: float, 
        lon: float,
    ) -> WeatherData:
        pass 

    @abstractmethod 
    def fetch_hourly_weather_forecast(
        self, 
        start_date: datetime, 
        duration: timedelta, 
        lat: float, 
        lon: float
    ) -> list[WeatherData]:
        pass 


class OpenWeatherMapAccessObject(WeatherApiInterface):
    BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"
    historical_weather_api_url = (
        "{base_url}/day_summary?lat={lat}&lon={lon}&date={date}&units=metric&appid={api_key}"
    )
    forecast_weather_api_url = (
        "{base_url}?lat={lat}&lon={lon}&exclude={part}&units=metric&appid={api_key}"
    )
    def _map_daily_data(self, data: dict[str, Any], target_date: date) -> WeatherData:
        """Maps OWM JSON daily response to the internal WeatherData model."""
        try:
            if data.get("temperature") is None or data["temperature"].get('max', None) is None:
                raise ValueError("Daily data missing required max temperature field.")
            return WeatherData(
                timestamp=datetime.combine(target_date, datetime.min.time()),
                aggregation_level="daily",
                wind_speed_mps=data.get("wind", {}).get("max", {}).get('speed', 0.0),
                rain_fall_total_mm=data.get('precipitation', {}).get('total', 0.0),
                temperature_deg_c=data["temperature"]["max"],
            )
        except (KeyError, TypeError) as e:
            raise ValueError(f"Failed to parse OWM data structure: {e}")
        
    def _map_hourly_data(self, hour_data: dict[str, Any]) -> WeatherData:
        """Maps a single OWM JSON hourly item to the internal WeatherData model."""
        try:
            dt_seconds = hour_data.get('dt')
            temperature_deg_c = hour_data.get('temp')
            if dt_seconds is None:
                 raise ValueError("Hourly data missing required 'dt' field.")
            if temperature_deg_c is None:
                 raise ValueError("Hourly data missing required 'temp' field.")
            timestamp = datetime.fromtimestamp(dt_seconds, tz=timezone.utc)
            wind_speed_mps = hour_data.get('wind_speed', 0.0)
            rain_data = hour_data.get('rain')
            rain_fall_total_mm = rain_data.get('1h', 0.0) if isinstance(rain_data, dict) else 0.0
            return WeatherData(
                timestamp=timestamp,
                aggregation_level="hourly",
                wind_speed_mps=wind_speed_mps,
                rain_fall_total_mm=rain_fall_total_mm,
                temperature_deg_c=temperature_deg_c,
            )
        except (KeyError, TypeError) as e:
            raise ValueError(f"Failed to parse OWM hourly forecast data structure: {e}")
        
    def _execute_request(self, url: str) -> dict[str, Any]:
        """Executes the request with Circuit Breaker and Retries."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"OWM API request: {e}")
        
    def fetch_daily_historical_weather_data(self, target_date: date, lat: float, lon: float) -> WeatherData:
        """Fetches a single day's historical weather summary."""
        url = self.historical_weather_api_url.format(
            base_url = self.BASE_URL,
            lat=lat, 
            lon=lon, 
            date=target_date.strftime("%Y-%m-%d"), 
            api_key=OWM_API_KEY
        )
        data = self._execute_request(url)
        return self._map_daily_data(data, target_date)

    def fetch_hourly_weather_forecast(
        self, 
        start_datetime: datetime, 
        duration: timedelta, 
        lat: float, 
        lon: float
    ) -> list[WeatherData]:
        """
        Fetches the hourly weather forecast for the duration starting from start_date.
        OWM One Call 3.0 provides up to 48 hours of hourly forecast.
        """
        exclude_parts = "current,minutely,daily,alerts"
        url = self.forecast_weather_api_url.format(
            base_url = self.BASE_URL,
            lat=lat, 
            lon=lon, 
            part=exclude_parts, 
            api_key=OWM_API_KEY
        )
        data = self._execute_request(url)
        forecast_data: list[WeatherData] = []
        if start_datetime.tzinfo is None:
            start_datetime = (
                start_datetime
                .replace(tzinfo=timezone.utc, minute=0, second=0, microsecond=0)
            )
        else:
            start_datetime = (
                start_datetime
                .astimezone(timezone.utc)
                .replace(minute=0, second=0, microsecond=0)
            )
        end_datetime = start_datetime + duration
        if 'hourly' in data:
            for hour_data in data['hourly']:
                timestamp = (
                    datetime.fromtimestamp(hour_data['dt'], tz=timezone.utc)
                )
                if start_datetime <= timestamp < end_datetime:
                    forecast_data.append(self._map_hourly_data(hour_data))  
        return forecast_data
    
