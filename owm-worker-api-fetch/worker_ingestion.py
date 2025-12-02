import logging
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone

import redis
import requests
from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.deduplication import RedisDeduplicationCacheRepository
from etl.exceptions import RateLimitExceededError
from etl.handlers import SignalCatcher
from etl.limiter import RedisDailyRateLimiterDao
from etl.persistence import SqliteTaskOutbox
from etl.producer import TaskProducerManager
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask
from pybreaker import CircuitBreaker
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

logging.basicConfig(
    encoding='utf-8', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)
REDIS_MASTER_HOST = os.environ.get("INGESTION_QUEUE_HOST", "redis-master")
REDIS_MASTER_PORT = 6379
INGESTION_STREAM_KEY = "owm-ingestion-stream"
CONSUMER_GROUP = "ingestion-workers"
CONSUMER_NAME = os.environ.get("WORKER_NAME", f"worker-{os.getpid()}") 
CONSUME_COUNT = 100
CONSUME_BLOCK_MS = 1000
INGESTION_MAX_RETRIES = 5
INGESTION_MAX_IDLE_MS = 600000
INGESTION_EXPIRY_TIME_MS = 2.16e7
DLQ_STREAM_KEY = f"dlq:{INGESTION_STREAM_KEY}"
DLQ_MAX_STREAM_SIZE = 200
PEL_CHECK_FREQUENCY_SECONDS = 60
PENDING_ENTRY_LIST_BATCH_SIZE = 10
CATEGORIZATION_STREAM_KEY = "owm-categorization-stream"
CATEGORIZATION_SQLITE_DB = "/tmp/categorization_producer_tasks.db"
CATEGORIZATION_TABLE_NAME = "categorization_tasks"
CATEGORIZATION_MAX_STREAM_SIZE = 200
CATEGORIZATION_BATCH_SIZE = 10
REDIS_SUBMIT_FREQUENCY_SECONDS = 30
OWM_MAX_DAILY_REQUESTS = 500
WEATHER_API_BASE_URL = "http://outpost-api-weather:8000"

REDIS_MASTER_CONNECTION_POOL = redis.BlockingConnectionPool(
    host= REDIS_MASTER_HOST,
    port= REDIS_MASTER_PORT,
    retry=Retry(ExponentialWithJitterBackoff(), 8),
    retry_on_error=[BusyLoadingError, RedisConnectionError],
    health_check_interval = 3,
    socket_connect_timeout = 15,
    socket_timeout = 5,
    max_connections = 2
)
REDIS_MASTER_CLIENT = redis.StrictRedis(connection_pool=REDIS_MASTER_CONNECTION_POOL)
OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
UPSTREAM_BROKER_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60 * 10)
DLQ_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60 * 10)
DOWNSTREAM_BROKER_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
WEATHER_SERVICE_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
FORECAST_DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=REDIS_MASTER_CLIENT,
    cache_key="ingestion:weather:forecast:deduplication:cache"
)
HISTORICAL_WEATHER_DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=REDIS_MASTER_CLIENT,
    cache_key="ingestion:weather:historical:deduplication:cache"
)
OWM_DAILY_RATE_LIMITER = RedisDailyRateLimiterDao(
    client=REDIS_MASTER_CLIENT,
    base_key="owm:daily:request:count:attempted",
    max_requests = 500,
)

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city_id: int
    model_config = {"frozen": True}

class WeatherCategorizationTask(BaseTask):
    city_id: int
    last_historical_timestamp: str 
    forecast_generated_at_timestamp: str

@OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER 
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _fetch_historical_data(
        task: OwmIngestionTask, 
        previous_date: date,
    ):
    historical_weather = {
        "lat": task.latitude_deg,
        "lon": task.longitude_deg,
        "tz": "+00:00", 
        "date": previous_date,
        "units":"metric",
        "wind": {"max": {"speed": 6}, "direction": 120},
        "precipitation": {"total": 2}, 
        "cloud_cover":{"afternoon":0},
        "humidity":{"afternoon":33},
        "pressure":{"afternoon":1015},
        "temperature":{ 
            "min":286.48 - 273.15,
            "max":299.24 - 273.15,
            "afternoon":296.15 - 273.15,
            "night":289.56 - 273.15,
            "evening":295.93 - 273.15,
            "morning":287.59 - 273.15
        },
    }
    measured_at_ts = datetime(
        previous_date.year, 
        previous_date.month, 
        previous_date.day, 
        tzinfo=timezone.utc
    )
    LOGGER.info(f"Successfully fetched historical data for city {task.city_id}.")
    return {
        "task_id": task.task_id,
        "city_id": task.city_id,
        "temperature_deg_c": historical_weather["temperature"]["max"],
        "wind_speed_mps": historical_weather["wind"]['max']["speed"],
        "rain_fall_total_mm": historical_weather["precipitation"]["total"],
        "aggregation_level": "daily",
        "measured_at_ts_utc": measured_at_ts.isoformat(),
    }

@OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER 
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _fetch_weather_forecast(task: OwmIngestionTask, current_hour: datetime) -> list[dict]:
    hourly_records = []
    BASE_TEMP_K: float = 288.15 
    for i in range(48):
        hourly_offset = timedelta(seconds=(i * 3600))
        hourly_records.append(
            {
                "dt": current_hour + hourly_offset,
                "temp": BASE_TEMP_K + (i * 2.0)  - 273.15,
                "wind_speed": 4.1 - (i * 0.2), 
                "rain": {"1h": 2.5}, 
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
        "lat": task.latitude_deg,
        "lon": task.longitude_deg,
        "timezone": "America/Chicago",
        "timezone_offset":-18000,
        "hourly": hourly_records,
    }
    city_id = task.city_id
    return [
        {
            "city_id": city_id, 
            "temperature_deg_c": hd["temp"],
            "wind_speed_mps": hd["wind_speed"],
            "rain_fall_total_mm": (hd.get("rain") or {}).get("1h", 0.0),
            "aggregation_level": "hourly",
            "forecast_generated_at_ts_utc" : current_hour.isoformat(),
            "forecast_timestamp_utc": hd["dt"].isoformat()
        }
        for hd in response_data["hourly"]
    ]

@WEATHER_SERVICE_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _post_historical_data(data: dict) -> bool: 
    historical_data_url = f"{WEATHER_API_BASE_URL}/weather/historical"
    historical_payload = {
        k: v for k, v in data.items() 
        if k not in ["task_id"]
    }
    response = requests.post(
        url=historical_data_url, 
        json=historical_payload,
        timeout = 5
    )
    response.raise_for_status()
    LOGGER.info(f"Successfully posted historical data for city {data['city_id']}.")
    return True

@WEATHER_SERVICE_CIRCUIT_BREAKER
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max = 30, jitter = 1),
    reraise=True
)
def _post_weather_forecast(data: list[dict]) -> bool: 
    forecast_data_url = f"{WEATHER_API_BASE_URL}/weather/forecast"
    response = requests.post(url=forecast_data_url, json=data, timeout=5)
    response.raise_for_status()
    LOGGER.info(f"Successfully posted {len(data)} forecast records for city {data[0]['city_id']}.")
    return True

def _process_task(task: OwmIngestionTask):
    # This task needs to be split into more pipeline components for
    # decoupling in a future PR. It has too many responsibilities
    # and it complicates retry/circuit breaking mechanisms
    # TODO: Update rate limiter class to INFO when incrementing and printing key
    key = OWM_DAILY_RATE_LIMITER._get_key_for_today()
    count = OWM_DAILY_RATE_LIMITER.client.get(key)
    LOGGER.info(f"The current rate limit count is {count}")
    if not (OWM_DAILY_RATE_LIMITER.allow_request() and OWM_DAILY_RATE_LIMITER.allow_request()):
        raise RateLimitExceededError("Daily rate limit exceeded for Open Weather Maps")
    current_hour = datetime.now().astimezone(timezone.utc).replace(minute=0,second=0, microsecond=0)
    previous_date = current_hour.date() - timedelta(days=1)
    historical_data = _fetch_historical_data(task, previous_date)
    _post_historical_data(historical_data)
    forecast = _fetch_weather_forecast(task, current_hour)
    _post_weather_forecast(forecast)
    LOGGER.info("Posted weather to Weather Service, forming WeatherCategorizationTask")
    downstream_task = WeatherCategorizationTask(
        task_id = task.task_id,
        city_id = task.city_id,
        last_historical_timestamp=previous_date.isoformat(),
        forecast_generated_at_timestamp=current_hour.isoformat(),
    )
    return downstream_task


def _make_upstream_consumer_and_dlq() -> TaskProcessingManager:
    # Need to find the upstream Redis Stream to consume
    task_queue = RedisTaskQueueRepository(
        client=REDIS_MASTER_CLIENT,
        stream_key = INGESTION_STREAM_KEY,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        task_model=OwmIngestionTask,
    )
    task_consumer = TaskConsumer(executable = _process_task)
    # Need to create a DLQ to send failed messages to
    dlq = RedisTaskQueueRepository(
        client=REDIS_MASTER_CLIENT,
        stream_key = f"dlq:{INGESTION_STREAM_KEY}",
        task_model=OwmIngestionTask
    )
    # Need to instatiate the Manager instance
    return TaskProcessingManager(
        task_queue = task_queue,
        task_consumer = task_consumer,
        dead_letter_queue = dlq,
        queue_breaker = UPSTREAM_BROKER_CIRCUIT_BREAKER,
        dlq_breaker = DLQ_CIRCUIT_BREAKER,
    )

def _make_downstream_producer_and_persistent_storage() -> TaskProducerManager:
    # Need to create the task queue to send weather data processing tasks to
    task_queue = RedisTaskQueueRepository(
        client = REDIS_MASTER_CLIENT,
        stream_key = CATEGORIZATION_STREAM_KEY,
        task_model=WeatherCategorizationTask,
        max_stream_length=CATEGORIZATION_MAX_STREAM_SIZE
    )
    # Need to create the persistent disk storage for the transactional outbox pattern
    storage = SqliteTaskOutbox(
        db_path = CATEGORIZATION_SQLITE_DB,
        task_type = WeatherCategorizationTask
    )
    # Need to create the manager to handle sending tasks to Redis Streams
    return TaskProducerManager(
        task_queue = task_queue, 
        storage = storage
    )

# For the producer, we need a background process to send tasks from the persistent storage to Redis
# Because we don't want to block the main thread that reads upstream tasks
@DOWNSTREAM_BROKER_CIRCUIT_BREAKER
def _flush_producer_from_disk_to_queue(
    producer: TaskProducerManager, 
    catcher: SignalCatcher, 
    batch_size: int, 
    frequency_seconds: int
):
    name = threading.current_thread().name
    LOGGER.info(f"Thread {name} starting flush loop (Frequency: {frequency_seconds}s)")
    try:
        expire_time_seconds = 86400 * 1
        while not catcher.stop_event.is_set():
            producer.clear_tasks(expire_time_seconds=expire_time_seconds)
            start_time = time.time()
            total_flushed = 0
            while True:
                num_flushed = producer.flush_tasks_to_queue(batch_size=batch_size)
                total_flushed += num_flushed
                if num_flushed == 0:
                    break 
            LOGGER.info(f"Flushed {total_flushed} tasks to the downstream queue")
            elapsed = time.time() - start_time
            sleep_duration = frequency_seconds - elapsed
            if sleep_duration > 0:
                catcher.stop_event.wait(sleep_duration)
            else:
                LOGGER.warning(
                    f"Flusher cycle took {elapsed:.2f}s, exceeding target frequency of {frequency_seconds}s. Skipping sleep."
                )      
            producer.clear_tasks(expire_time_seconds=expire_time_seconds)
    except Exception as e:
        LOGGER.fatal(f"Flusher thread failed with unhandled exception: {e}", exc_info=True)
    finally:
        producer.clear_tasks(expire_time_seconds=expire_time_seconds)
        catcher.stop_event.set()
        LOGGER.info(f"Thread {name} exiting.")

# Every now and then, we need the consumer to see if it needs to reprocess tasks
def _handle_PEL_batch(producer, consumer):
    LOGGER.debug("Fetching tasks from the upstream queue's PEL")
    pel_downstream_tasks, _ = consumer.get_and_process_stuck_tasks(
        max_idle_ms = INGESTION_MAX_IDLE_MS,
        expiry_time_ms = INGESTION_EXPIRY_TIME_MS,
        max_retries = INGESTION_MAX_RETRIES,
        batch_size = PENDING_ENTRY_LIST_BATCH_SIZE,
    )
    LOGGER.debug(f"Found {len(pel_downstream_tasks)} in the upstream queue's PEL")
    producer.produce_batch_to_disk(list(pel_downstream_tasks.values()))
    return len(pel_downstream_tasks)

def _check_pending_entries_list(
    consumer: TaskProcessingManager, 
    producer: TaskProducerManager,
    catcher: SignalCatcher, 
    frequency_seconds: int
):
    name = threading.current_thread().name
    LOGGER.info(f"Thread {name} starting PEL check (Frequency: {frequency_seconds}s)")
    try:
        while not catcher.stop_event.is_set():
            start_time = time.time()
            while True:
                num_tasks = _handle_PEL_batch(producer, consumer)
                if num_tasks == 0:
                    break
            elapsed = time.time() - start_time
            sleep_duration = frequency_seconds - elapsed
            if sleep_duration > 0:
                catcher.stop_event.wait(sleep_duration)
            else:
                LOGGER.warning(
                    f"PEL check took {elapsed:.2f}s, exceeding target frequency of {frequency_seconds}s. Skipping sleep."
                )      
    except Exception as e:
        LOGGER.fatal(f"PEL check failed with unhandled exception: {e}", exc_info=True)
    finally:
        catcher.stop_event.set()
        LOGGER.info(f"Thread {name} exiting.")

def main(catcher):
    consumer = _make_upstream_consumer_and_dlq()
    producer = _make_downstream_producer_and_persistent_storage()
    threads = [
        threading.Thread(
            group = None, 
            target=_flush_producer_from_disk_to_queue,
            name="FlusherThread",
            args=(producer, catcher, CATEGORIZATION_BATCH_SIZE, REDIS_SUBMIT_FREQUENCY_SECONDS)
        ),
        threading.Thread(
            group=None,
            target=_check_pending_entries_list,
            name="PelCheckerThread",
            args=(consumer, producer, catcher, PEL_CHECK_FREQUENCY_SECONDS)
        )
    ]
    for thread in threads:
        thread.start()
    # Main thread pulls tasks from the upstream queue and processes them, 
    # then passes new tasks downstream. The PEL is relative to the consumer, which 
    # checks periodically for stuck tasks in other workers and claims them
    try:
        while not catcher.stop_event.wait(timeout=1):
            # We'll ignore `failed_tasks` from `get_and_process_batch` and `produce_batch_to_disk`
            # by taking only the first entry, which are the successful tasks to send downstream
            LOGGER.info("Attempting to fetch upstream tasks and process them")
            downstream_tasks, _ = consumer.get_and_process_batch(
                batch_size = CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            LOGGER.debug(f"Downstream tasks {downstream_tasks}")
            if downstream_tasks:
                LOGGER.info(f"Processed batch: obtained {len(downstream_tasks)} tasks for downstream production.")
                producer.produce_batch_to_disk(list(downstream_tasks.values()))
            else:
                LOGGER.info(f"No downstream tasks to be processed: {len(downstream_tasks)}")
    finally:
        catcher.stop_event.set()
        for t in threads:
            t.join(timeout=60)
            if t.is_alive():
                LOGGER.warning(f"{t.name} did not exit in time!")
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == '__main__':
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