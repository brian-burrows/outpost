import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import redis
import requests
from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.deduplication import (
    RedisDeduplicationCacheRepository,
    with_deduplication_caching,
)
from etl.handlers import SignalCatcher
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask
from pybreaker import CircuitBreaker, CircuitBreakerError
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

logging.basicConfig(
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
CATEGORIZATION_REDIS_HOST = os.environ.get("CATEGORIZATION_QUEUE_HOST", "redis-master")
CATEGORIZATION_REDIS_PORT = 6379
CATEGORIZATION_STREAM_KEY = "owm-categorization-stream"
CATEGORIZATION_MAX_STREAM_SIZE = 200
CATEGORIZATION_BATCH_SIZE = 10
CONSUMER_GROUP = "categorization-workers"
CONSUMER_NAME = os.environ.get("WORKER_NAME", f"categorization-worker-{os.getpid()}")
CONSUME_COUNT = 100
CONSUME_BLOCK_MS = 1000
CATEGORIZATION_MAX_RETRIES = 5
CATEGORIZATION_MAX_IDLE_MS = 600000
CATEGORIZATION_EXPIRY_TIME_MS = 2.16e7
WEATHER_API_BASE_URL = "http://outpost-api-weather:8000"
CATEGORIZATION_REDIS_CONNECTION_POOL = redis.BlockingConnectionPool(
    host=CATEGORIZATION_REDIS_HOST,
    port=CATEGORIZATION_REDIS_PORT,
    retry=Retry(ExponentialWithJitterBackoff(), 8),
    retry_on_error=[BusyLoadingError, RedisConnectionError],
    health_check_interval=3,
    socket_connect_timeout=15,
    socket_timeout=5,
    max_connections=2,
)
UPSTREAM_QUEUE_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60)
WEATHER_API_CIRCUIT_BREAKER = CircuitBreaker(fail_max=5, reset_timeout=300)
DLQ_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max=10,
    reset_timeout=60,
)
DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL),
    cache_key="weather:categorization:deduplication:cache",
)
LOGGER = logging.getLogger(__name__)


class WeatherCategorizationTask(BaseTask):
    city_id: int
    last_historical_timestamp: str
    forecast_generated_at_timestamp: str


class SimulatedError(Exception):
    pass


def _classify_weather(combined_data: list) -> str:
    historical_data = [
        {"measured_at_ts_utc": d["timestamp_utc"], **d}
        for d in combined_data
        if d["data_source"] == "HISTORICAL"
    ]
    forecast_data = [d for d in combined_data if d["data_source"] == "FORECAST"]
    # Define thresholds for inference
    RAIN_THRESHOLD_MM = 5.0
    HEAVY_RAIN_THRESHOLD_MM = 10.0
    MUD_DAYS = 2
    FREEZING_POINT_C = 0.0
    HEAT_ADVISORY_C = 35.0
    WIND_ADVISORY_MPS = 15.0
    historical_data.sort(
        key=lambda x: datetime.fromisoformat(x.get("measured_at_ts_utc")),
        reverse=True,
    )
    rain_sum_24h_forecast = sum(d.get("rain_fall_total_mm", 0.0) for d in forecast_data[:24])
    if rain_sum_24h_forecast >= HEAVY_RAIN_THRESHOLD_MM:
        forecast_temp_avg = (
            sum(d.get("temperature_deg_c", 0.0) for d in forecast_data[:24]) / 24.0
            if forecast_data
            else 0
        )
        if forecast_temp_avg < FREEZING_POINT_C:
            return "HEAVY_SNOW_WARNING"
        return "TRAIL_CLOSED_HEAVY_RAIN"
    latest_ts = datetime.now(timezone.utc)
    mud_window_start = latest_ts - timedelta(days=MUD_DAYS)
    recent_rain_mm = 0.0
    recent_warm_days = False
    for record in historical_data:
        ts = datetime.fromisoformat(record.get("measured_at_ts_utc"))
        if ts >= mud_window_start:
            recent_rain_mm += record.get("rain_fall_total_mm", 0.0)
            if record.get("temperature_deg_c", -100) > 10.0:
                recent_warm_days = True
    if recent_rain_mm >= RAIN_THRESHOLD_MM and not recent_warm_days:
        return "TRAIL_MUD_WARNING"
    elif recent_rain_mm < RAIN_THRESHOLD_MM and rain_sum_24h_forecast < RAIN_THRESHOLD_MM:
        return "TRAIL_DRY_EXCELLENT"
    if historical_data and forecast_data:
        historical_max_temp = max(d.get("temperature_deg_c", -100.0) for d in historical_data)
        forecast_min_temp = min(d.get("temperature_deg_c", 100.0) for d in forecast_data[:24])
        if historical_max_temp > FREEZING_POINT_C and forecast_min_temp < FREEZING_POINT_C:
            return "SNOWPACK_ICY_CONDITIONS"
        elif historical_max_temp > 5.0 and forecast_min_temp > FREEZING_POINT_C:
            return "SNOWPACK_HEAVY_WET"
    max_wind_forecast = (
        max(d.get("wind_speed_mps", 0.0) for d in forecast_data) if forecast_data else 0.0
    )
    if max_wind_forecast > WIND_ADVISORY_MPS:
        return "WINDY_ADVISORY"
    max_temp_historical = (
        max(d.get("temperature_deg_c", -100.0) for d in historical_data)
        if historical_data
        else -100.0
    )
    if max_temp_historical > HEAT_ADVISORY_C:
        return "HEAT_ADVISORY"
    return "NORMAL_MILD_CONDITIONS"


@WEATHER_API_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential_jitter(initial=1, max=30, jitter=1),
    reraise=True
)
def _fetch_weather_data(city_id: int) -> list:
    """Fetches combined historical and forecast data from the /window endpoint."""
    window_url = f"{WEATHER_API_BASE_URL}/weather/window/{city_id}"
    LOGGER.debug(f"Fetching weather window for city {city_id}...")
    window_response = requests.get(window_url, timeout=5)
    window_response.raise_for_status()
    return window_response.json()

@WEATHER_API_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential_jitter(initial=1, max=30, jitter=1),
    reraise=True
)
def _post_weather_classification(city_id: int, classification_label: str) -> bool:
    """Posts the final classification back to the API."""
    classification_url = f"{WEATHER_API_BASE_URL}/cities/{city_id}/classification"
    classification_payload = {
        "city_id": city_id,
        "class_label": classification_label,
    }
    LOGGER.debug(f"Posting classification '{classification_label}' for city {city_id}...")
    post_response = requests.post(classification_url, json=classification_payload, timeout=5)
    post_response.raise_for_status()
    return True


@with_deduplication_caching(DEDUPLICATION_CACHE)
def _process_task(task: WeatherCategorizationTask) -> bool:
    """Executes the weather categorization in three resilient, sequential steps.

    If any step fails then the entire task fails. We raise errors to signal that failed tasks
    should be sent to the DLQ.
    """
    LOGGER.info(f"Starting classification for Task ID: {task.task_id}, City ID: {task.city_id}")
    city_id = task.city_id
    try:
        combined_data = _fetch_weather_data(city_id)
        classification_label = _classify_weather(combined_data)
        LOGGER.info(f"City {city_id} classified as: {classification_label}")
        return _post_weather_classification(city_id, classification_label)
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"API interaction failed during task processing for city {city_id}. Error: {e}")
        raise e
    except CircuitBreakerError:
        LOGGER.warning(f"Circuit OPEN for Weather Service API. Dropping task {task.task_id}.")
        raise
    except Exception as e:
        LOGGER.fatal(f"Unhandled critical error processing task {task.task_id}: {e}", exc_info=True)
        raise e


def _make_upstream_consumer_and_dlq():
    redis_client = redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL)
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
    # This code executes locally, so we do not need rate limiting, circuit breaking
    # or retry logic on the TaskConsumer instance
    cat_task_consumer = TaskConsumer(executable=_process_task)
    return TaskProcessingManager(
        task_queue=task_queue,
        dead_letter_queue=dlq,
        task_consumer=cat_task_consumer,
        queue_breaker=UPSTREAM_QUEUE_CIRCUIT_BREAKER,
        dlq_breaker=DLQ_CIRCUIT_BREAKER,
    )

def main(catcher):
    consumer = _make_upstream_consumer_and_dlq()
    try:
        while not catcher.stop_event.wait(timeout=1):
            consumer.get_and_process_batch(
                batch_size=CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            consumer.get_and_process_stuck_tasks(
                max_idle_ms=CATEGORIZATION_MAX_IDLE_MS,
                expiry_time_ms=CATEGORIZATION_EXPIRY_TIME_MS,
                max_retries=CATEGORIZATION_MAX_RETRIES,
                batch_size=CONSUME_COUNT,
            )
    finally:
        catcher.stop_event.set()
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == "__main__":
    with SignalCatcher() as catcher:
        exit_code = 0
        try:
            exit_code = main(catcher)
        except Exception as e:
            LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
            exit_code = 1
        finally:
            catcher.stop_event.set()
            LOGGER.info("Shutdown complete.")
            sys.exit(exit_code)
