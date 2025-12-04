import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

import requests
from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.deduplication import with_deduplication_caching
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask
from pybreaker import CircuitBreakerError

from src.breakers import (
    DLQ_CIRCUIT_BREAKER,
    UPSTREAM_QUEUE_CIRCUIT_BREAKER,
)
from src.client import WeatherRecord, WeatherServiceClient
from src.db import DEDUPLICATION_CACHE, REDIS_CLIENT

# TODO: import these from the environment, pass directly into constructors?
CONSUMER_GROUP = "categorization-workers"
CONSUMER_NAME = os.environ.get("WORKER_NAME", f"categorization-worker-{os.getpid()}")
CATEGORIZATION_STREAM_KEY = "owm-categorization-stream"
LOGGER = logging.getLogger(__name__)

class SimulatedError(Exception):
    pass

class WeatherCategorizationTask(BaseTask):
    city_id: int
    last_historical_timestamp: str
    forecast_generated_at_timestamp: str


class WeatherClassifierInterface(ABC):
    """Abstract base class for taking historical weather and forecast weather
    and producing some class labels.
    """
    @abstractmethod
    def classify_trail_conditions(
        self, 
        historical_weather: list[WeatherRecord], 
        forecast_weather: list[WeatherRecord]
    ):
        pass


class WeatherClassifier(WeatherClassifierInterface):
    RAIN_THRESHOLD_MM = 5.0
    HEAVY_RAIN_THRESHOLD_MM = 10.0
    MUD_DAYS = 2
    FREEZING_POINT_C = 0.0
    HEAT_ADVISORY_C = 35.0
    WIND_ADVISORY_MPS = 15.0

    def check_all_weather_for_mud(self, historical_data: list[WeatherRecord], forecast_data: list[WeatherRecord]) -> str | None:
        latest_ts = datetime.now(timezone.utc)
        rain_sum_24h_forecast = sum(d.rain_fall_total_mm for d in forecast_data[:24])
        mud_window_start = latest_ts - timedelta(days=self.MUD_DAYS)
        recent_rain_mm = 0.0
        recent_warm_days = False
        for record in historical_data:
            if record.timestamp_utc >= mud_window_start:
                recent_rain_mm += record.rain_fall_total_mm
                if record.temperature_deg_c > 10.0:
                    recent_warm_days = True
        if recent_rain_mm >= self.RAIN_THRESHOLD_MM and not recent_warm_days:
            return "TRAIL_MUD_WARNING"
        elif recent_rain_mm < self.RAIN_THRESHOLD_MM and rain_sum_24h_forecast < self.RAIN_THRESHOLD_MM:
            return "TRAIL_DRY_EXCELLENT"
        return None
    
    def check_forecast_for_heavy_precipitation(self, forecast_data: list[WeatherRecord]) -> str | None:
        rain_sum_24h_forecast = sum(d.rain_fall_total_mm for d in forecast_data[:24])
        if rain_sum_24h_forecast >= self.HEAVY_RAIN_THRESHOLD_MM:
            rain_data_24h = forecast_data[:24]
            forecast_temp_avg = (
                sum(d.temperature_deg_c for d in rain_data_24h) / len(rain_data_24h)
                if rain_data_24h
                else 0
            )
            if forecast_temp_avg < self.FREEZING_POINT_C:
                return "HEAVY_SNOW_WARNING"
            return "TRAIL_CLOSED_HEAVY_RAIN"
        return None
    
    def check_for_heavy_snowpack(self, historical_data: list[WeatherRecord], forecast_data: list[WeatherRecord]) -> str | None:
        if historical_data and forecast_data:
            historical_max_temp = max(d.temperature_deg_c for d in historical_data)
            forecast_min_temp = min(d.temperature_deg_c  for d in forecast_data[:24])
            if historical_max_temp > self.FREEZING_POINT_C and forecast_min_temp < self.FREEZING_POINT_C:
                return "SNOWPACK_ICY_CONDITIONS"
            elif historical_max_temp > 5.0 and forecast_min_temp > self.FREEZING_POINT_C:
                return "SNOWPACK_HEAVY_WET"
        return None
            
    def check_for_wind_advisory(self, forecast_data: list[WeatherRecord]) -> str | None:
        # TODO: Update API to return wind speed data in the historical/forecast period
        return None
        
    def check_for_heat_advisory(self, forecast_data: list[WeatherRecord]) -> str | None:
        max_temp_forecast = max([d.temperature_deg_c for d in forecast_data])
        if max_temp_forecast > self.HEAT_ADVISORY_C:
            return "HEAT_ADVISORY"
        return None

    def classify_trail_conditions(
        self, 
        historical_data: list[WeatherRecord], 
        forecast_data: list[WeatherRecord]
    ) -> list[str]:
        conditions = []
        for condition in [
            self.check_all_weather_for_mud(historical_data=historical_data, forecast_data=forecast_data),
            self.check_for_heat_advisory(forecast_data=forecast_data),
            self.check_for_wind_advisory(forecast_data=forecast_data),
            self.check_for_heavy_snowpack(historical_data=historical_data, forecast_data=forecast_data),
            self.check_forecast_for_heavy_precipitation(forecast_data=forecast_data),
        ]:
            if condition is not None:
                conditions.append(condition)
        return conditions



@with_deduplication_caching(DEDUPLICATION_CACHE)
def process_task(task: WeatherCategorizationTask) -> bool:
    """Executes the weather categorization in three sequential steps.

    If any step fails then the entire task fails. We raise errors to signal that failed tasks
    should be sent to the DLQ.
    """
    # TODO: Use instance methods from `client.py` to chain together the pipeline
    LOGGER.info(f"Starting classification for Task ID: {task.task_id}, City ID: {task.city_id}")
    city_id = task.city_id
    try:
        client = WeatherServiceClient()
        historical_weather_data, forecast_weather_data = client.fetch_weather_data(city_id)
        classification_label = WeatherClassifier().classify_trail_conditions(
            historical_data=historical_weather_data,
            forecast_data=forecast_weather_data
        )
        LOGGER.info(f"City {city_id} classified as: {classification_label}")
        # TODO: Update API to take a list of classification labels, database to have one-to-many relationship
        return client.post_weather_classification(city_id, ",".join(classification_label))
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"API interaction failed during task processing for city {city_id}. Error: {e}")
        raise e
    except CircuitBreakerError:
        LOGGER.warning(f"Circuit OPEN for Weather Service API. Dropping task {task.task_id}.")
        raise
    except Exception as e:
        LOGGER.fatal(f"Unhandled critical error processing task {task.task_id}: {e}", exc_info=True)
        raise e


def make_upstream_consumer_and_dlq():
    redis_client = REDIS_CLIENT
    task_queue = RedisTaskQueueRepository(
        client=redis_client,
        stream_key=CATEGORIZATION_STREAM_KEY,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        task_model=WeatherCategorizationTask,
    )
    dlq = RedisTaskQueueRepository(
        client=redis_client,
        stream_key=f"dlq:{CATEGORIZATION_STREAM_KEY}",
        task_model=WeatherCategorizationTask,
        max_stream_length=10000,
    )
    # This task consumer interacts with the distributed Deduplication cache, so we'll need some
    # safety mechanisms (e.g., retries, circuit breakers) at some point
    cat_task_consumer = TaskConsumer(executable=process_task)
    # The processing manager interacts with the upstream queue, needs networking safety mechanisms
    return TaskProcessingManager(
        task_queue=task_queue,
        dead_letter_queue=dlq,
        task_consumer=cat_task_consumer,
        queue_breaker=UPSTREAM_QUEUE_CIRCUIT_BREAKER,
        dlq_breaker=DLQ_CIRCUIT_BREAKER,
    )