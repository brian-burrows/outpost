from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from datetime import datetime
from typing import Literal, List
import logging

from ..core.database import get_read_conn, get_write_conn 

LOGGER = logging.getLogger(__name__)
router = APIRouter(
    prefix="/weather",
    tags=["weather"],
)
class WeatherData(BaseModel):
    """Base schema for weather measurements."""
    city_id: int 
    temperature_deg_c: float 
    wind_speed_mps: float 
    rain_fall_total_mm: float
    aggregation_level: Literal["daily", "hourly"]

class ForecastWeatherData(WeatherData):
    """Input model for forecast data insertion."""
    forecast_generated_at_ts_utc : datetime = Field(description="Timestamp when the forecast was generated (UTC).")
    forecast_timestamp_utc : datetime = Field(description="Future timestamp the forecast applies to (UTC).")

class HistoricalWeatherData(WeatherData):
    """Input model for historical data insertion."""
    measured_at_ts_utc : datetime = Field(description="Timestamp when the weather data was measured (UTC).")

@router.get("/forecast/{city_id}")
async def get_latest_forecast(
    city_id: int, 
    aggregation_level: Literal["daily", "hourly"] = "daily",
    conn: AsyncConnection = Depends(get_read_conn)
):
    """
    Retrieves the most recently generated weather forecast 
    for a given city and aggregation level using raw SQL.
    """
    stmt = text("""
        SELECT 
            city_id, temperature_deg_c, wind_speed_mps, rain_fall_total_mm, aggregation_level,
            forecast_generated_at_ts_utc, forecast_timestamp_utc
        FROM forecast_weather_data
        WHERE city_id = :city_id AND aggregation_level = :agg_level
        ORDER BY forecast_generated_at_ts_utc DESC, forecast_timestamp_utc ASC
        LIMIT 1
    """)
    result = await conn.execute(stmt, {
        "city_id": city_id, 
        "agg_level": aggregation_level
    })
    forecast = result.mappings().first()
    if forecast is None:
        raise HTTPException(status_code=404, detail=f"Forecast not found for city {city_id}.")
    return dict(forecast)


@router.post("/forecast")
async def create_forecast_data(
    data: List[ForecastWeatherData],
    conn: AsyncConnection = Depends(get_write_conn) 
):
    """
    Batch inserts a list of weather forecast records (e.g., a 48-hour forecast 
    for a single city) using a single raw SQL execution.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Input list cannot be empty.")
    table_name = "forecast_weather_data"
    stmt = text(f"""
        INSERT INTO {table_name} (
            city_id, temperature_deg_c, wind_speed_mps, rain_fall_total_mm, aggregation_level,
            forecast_generated_at_ts_utc, forecast_timestamp_utc
        )
        VALUES (
            :city_id, :temperature_deg_c, :wind_speed_mps, :rain_fall_total_mm, :aggregation_level,
            :forecast_generated_at_ts_utc, :forecast_timestamp_utc
        )
    """)
    params = [item.model_dump() for item in data]
    try:
        trans = await conn.begin()
        await conn.execute(stmt, params)
        await trans.commit()
    except Exception as e:
        LOGGER.error(f"Database batch insertion failed for forecast data: {e}")
        raise HTTPException(status_code=500, detail="Database batch insertion failed for forecast data.")
    city_id_ref = data[0].city_id
    return {
        "message": f"Successfully ingested {len(data)} forecast records.", 
        "city_id": city_id_ref
    }


@router.get("/historical/{city_id}")
async def get_historical_data(
    city_id: int, 
    aggregation_level: Literal["daily", "hourly"] = "daily",
    limit: int = 100,
    conn: AsyncConnection = Depends(get_read_conn)
):
    """
    Retrieves a list of recent historical weather measurements for a given city.
    """
    stmt = text("""
        SELECT 
            city_id, temperature_deg_c, wind_speed_mps, rain_fall_total_mm, aggregation_level,
            measured_at_ts_utc
        FROM historical_weather_data
        WHERE city_id = :city_id AND aggregation_level = :agg_level
        ORDER BY measured_at_ts_utc DESC
        LIMIT :limit
    """)
    result = await conn.execute(stmt, {
        "city_id": city_id, 
        "agg_level": aggregation_level,
        "limit": limit
    })
    await conn.commit()
    historical_records = [dict(record) for record in result.mappings()]
    if not historical_records:
        raise HTTPException(status_code=404, detail=f"No historical data found for city {city_id}.")
    return historical_records


@router.post("/historical")
async def create_historical_data(
    data: HistoricalWeatherData, 
    conn: AsyncConnection = Depends(get_write_conn)
):
    """Inserts a new historical weather record using raw SQL."""
    table_name = "historical_weather_data"
    stmt = text(f"""
        INSERT INTO {table_name} (
            city_id, temperature_deg_c, wind_speed_mps, rain_fall_total_mm, aggregation_level,
            measured_at_ts_utc
        )
        VALUES (
            :city_id, :temperature_deg_c, :wind_speed_mps, :rain_fall_total_mm, :aggregation_level,
            :measured_at_ts_utc
        )
    """)
    try:
        trans = await conn.begin()
        await conn.execute(stmt, data.model_dump())
        await trans.commit()
    except Exception as e:
        LOGGER.error(f"Database insertion failed for historical data: {e}")
        raise HTTPException(status_code=500, detail="Database insertion failed for historical data.")
    return {"message": "Historical data ingested successfully", "city_id": data.city_id}