import logging
from datetime import datetime
from typing import List, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from outpost.core.breakers import DB_READER_CIRCUIT_BREAKER, DB_WRITER_CIRCUIT_BREAKER
from outpost.core.database import get_read_conn, get_write_conn
from outpost.core.exceptions import async_map_postgres_exceptions_to_http
from outpost.core.retries import postgres_async_retry_factory

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

class CombinedWeatherRecord(BaseModel):
    """Schema for a record retrieved from the current_weather_data view."""
    city_name: str
    state_name: str
    timestamp_utc: datetime = Field(
        description="Timestamp of the measurement (historical) or forecast (future) in UTC."
    ) 
    temperature_deg_c: float
    rain_fall_total_mm: float
    data_source: Literal["HISTORICAL", "FORECAST"]

@router.get("/forecast/{city_id}")
@async_map_postgres_exceptions_to_http
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
            city_id,
            temperature_deg_c,
            wind_speed_mps,
            rain_fall_total_mm,
            aggregation_level,
            forecast_generated_at_ts_utc,
            forecast_timestamp_utc
        FROM
            forecast_weather_data
        WHERE
            city_id = :city_id
            AND aggregation_level = :agg_level
            AND forecast_generated_at_ts_utc = (
                SELECT
                    MAX(forecast_generated_at_ts_utc)
                FROM
                    forecast_weather_data
                WHERE
                    city_id = :city_id
                    AND aggregation_level = :agg_level
            )
        ORDER BY
            forecast_timestamp_utc ASC;
    """)
    retrying = postgres_async_retry_factory()
    async for retry in retrying:
        with retry:
            with DB_READER_CIRCUIT_BREAKER.calling():
                result = await conn.execute(stmt, {
                    "city_id": city_id, 
                    "agg_level": aggregation_level
                })
                forecast = result.mappings().first()
    if forecast:
        return dict(forecast)
    LOGGER.error(f"Forecast not found for city {city_id}")
    raise HTTPException(status_code=404, detail=f"Forecast not found for city {city_id}.")


@router.post("/forecast", status_code=status.HTTP_201_CREATED)
@async_map_postgres_exceptions_to_http
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
        ON CONFLICT (forecast_generated_at_ts_utc, city_id, forecast_timestamp_utc) DO UPDATE SET
            temperature_deg_c = EXCLUDED.temperature_deg_c,
            wind_speed_mps = EXCLUDED.wind_speed_mps,
            rain_fall_total_mm = EXCLUDED.rain_fall_total_mm;
    """)
    params = [item.model_dump() for item in data]
    city_id_ref = data[0].city_id
    async for attempt in postgres_async_retry_factory():
        with attempt:
            with DB_WRITER_CIRCUIT_BREAKER.calling():
                trans = await conn.begin()
                await conn.execute(stmt, params)
                await trans.commit()
                return {
                    "message": f"Successfully ingested {len(data)} forecast records.", 
                    "city_id": city_id_ref
                }


@router.get("/historical/{city_id}")
@async_map_postgres_exceptions_to_http
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
    historical_records = None
    async for attempt in postgres_async_retry_factory():
        with attempt:
            with DB_READER_CIRCUIT_BREAKER.calling():
                result = await conn.execute(stmt, {
                    "city_id": city_id, 
                    "agg_level": aggregation_level,
                    "limit": limit
                })
                historical_records = [dict(record) for record in result.mappings()]
    if not historical_records:
        raise HTTPException(status_code=404, detail=f"No historical data found for city {city_id}.")
    return historical_records


@router.post("/historical", status_code=status.HTTP_201_CREATED)
@async_map_postgres_exceptions_to_http
async def create_historical_data(
    data: HistoricalWeatherData, 
    conn: AsyncConnection = Depends(get_write_conn),
    status_code=status.HTTP_201_CREATED
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
        ON CONFLICT (measured_at_ts_utc, city_id) DO UPDATE SET
            temperature_deg_c = EXCLUDED.temperature_deg_c,
            wind_speed_mps = EXCLUDED.wind_speed_mps,
            rain_fall_total_mm = EXCLUDED.rain_fall_total_mm;
    """)
    async for attempt in postgres_async_retry_factory():
        with attempt:
            with DB_WRITER_CIRCUIT_BREAKER.calling():
                trans = await conn.begin()
                await conn.execute(stmt, data.model_dump())
                await trans.commit()
                return {"message": "Historical data ingested successfully", "city_id": data.city_id}

@router.get("/window/{city_id}", response_model=List[CombinedWeatherRecord])
@async_map_postgres_exceptions_to_http
async def fetch_current_weather_window(
    city_id: int,
    conn: AsyncConnection = Depends(get_read_conn)
):
    retrying = postgres_async_retry_factory()
    city_details = None
    async for retry in retrying:
        with retry:
            with DB_READER_CIRCUIT_BREAKER.calling():
                city_check_stmt = text(
                    """
                    SELECT city_name, state_name 
                    FROM cities WHERE city_id = :city_id
                    """
                )
                city_result = await conn.execute(city_check_stmt, {"city_id": city_id})
                city_details = city_result.mappings().first()
    if city_details is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                            detail=f"City with ID {city_id} not found.")
    stmt = text("""
        WITH latest_forecast_run AS (
            SELECT city_id, MAX(forecast_generated_at_ts_utc) AS latest_gen_ts
            FROM forecast_weather_data 
            WHERE city_id = :city_id
            GROUP BY city_id
        ),
        most_recent_forecast AS (
            SELECT 
                f.city_id, 
                c.city_name, 
                c.state_name, 
                f.forecast_timestamp_utc, 
                f.temperature_deg_c, 
                f.rain_fall_total_mm, 
                'FORECAST' AS data_source
            FROM forecast_weather_data f
            JOIN latest_forecast_run l 
                ON f.city_id = l.city_id AND f.forecast_generated_at_ts_utc = l.latest_gen_ts
            JOIN cities c ON f.city_id = c.city_id
        )
        -- Historical Data (Last 3 days)
        (
            SELECT 
                c.city_name, 
                c.state_name, 
                h.measured_at_ts_utc AS timestamp_utc, 
                h.temperature_deg_c, 
                h.rain_fall_total_mm, 
                'HISTORICAL' AS data_source
            FROM historical_weather_data h
            JOIN cities c ON h.city_id = c.city_id
            WHERE h.city_id = :city_id
              AND h.measured_at_ts_utc >= (NOW() - INTERVAL '3 days')
        ) 
        UNION ALL 
        -- Future Forecast Data
        (
            SELECT 
                city_name, 
                state_name, 
                forecast_timestamp_utc AS timestamp_utc, 
                temperature_deg_c, 
                rain_fall_total_mm, 
                data_source
            FROM most_recent_forecast
            WHERE forecast_timestamp_utc > NOW()
        )
        ORDER BY
            timestamp_utc ASC;
    """)
    weather_window = None
    async for attempt in postgres_async_retry_factory():
        with attempt:
            with DB_READER_CIRCUIT_BREAKER.calling():
                result = await conn.execute(stmt, {"city_id": city_id})
                weather_window = result.mappings().all()
    if not weather_window:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"No combined data found for city {city_id} in the current window."
        )
    return [CombinedWeatherRecord.model_validate(dict(record)) for record in weather_window]