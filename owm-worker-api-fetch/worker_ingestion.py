import logging
import os
import random
import sys
import threading
import time
from typing import Any

import redis
from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.deduplication import (
    RedisDeduplicationCacheRepository,
    with_deduplication_caching,
)
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

logging.basicConfig(
    encoding='utf-8', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
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
INGESTION_EXPIRY_TIME_MS = 0#2.16e7
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
API_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
UPSTREAM_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60 * 10)
DOWNSTREAM_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=REDIS_MASTER_CLIENT,
    cache_key="ingestion:processing:deduplication:cache"
)

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city: str
    state: str 
    forecast_duration_hours: int = 72
    historical_duration_hours: int = 72

class WeatherCategorizationTask(BaseTask):
    city: str 
    state: str 
    historical_data: list[dict[str, Any]]
    forecast_data: list[dict[str, Any]]


@with_deduplication_caching(cache_dao = DEDUPLICATION_CACHE)
def _process_task(task: OwmIngestionTask) -> WeatherCategorizationTask:
    LOGGER.info("Simulating API call to OWM")
    time.sleep(0.01)
    if random.random() < 0.1:
        raise ValueError("Error processing weather task")
    return WeatherCategorizationTask(
        historical_data = [{"date": "2024-04-01 00:00:00", "rain_inches": 1, "wind_mps": 6}],
        forecast_data =  [{"date": "2024-04-01 01:00:00", "rain_inches": 1, "wind_mps": 6}],
        task_id=task.task_id, 
        city=task.city, 
        state=task.state, 
        latitude_deg=task.latitude_deg, 
        longitude_deg=task.longitude_deg
    )

def _make_upstream_consumer_and_dlq() -> TaskProcessingManager:
    # Need to ensure our API calls are limited to a certain amount per day
    rate_limiter = RedisDailyRateLimiterDao(
        client=REDIS_MASTER_CLIENT,
        base_key="owm:api:daily:query:count",
        max_requests=OWM_MAX_DAILY_REQUESTS
    )
    # Need to find the upstream Redis Stream to consume
    task_queue = RedisTaskQueueRepository(
        client=REDIS_MASTER_CLIENT,
        stream_key = INGESTION_STREAM_KEY,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        task_model=OwmIngestionTask,
    )
    # Need to create a circuit breaker in case the upstream queues go down
    # Need to create our interface to process tasks
    task_consumer = TaskConsumer(
        executable = _process_task,
        rate_limiter = rate_limiter,
        circuit_breaker = API_CIRCUIT_BREAKER
    )
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
        queue_breaker = UPSTREAM_CIRCUIT_BREAKER,
        dlq_breaker = UPSTREAM_CIRCUIT_BREAKER,
    )

def _make_downstream_producer_and_persistent_storage() -> TaskProducerManager:
    # Need to create the task queue to send weather data processing tasks to
    # TODO: Add circuit breaking to producer?
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
            name="categorization-queue-flush-to-redis-thread",
            args=(producer, catcher, CATEGORIZATION_BATCH_SIZE, REDIS_SUBMIT_FREQUENCY_SECONDS)
        ),
        threading.Thread(
            group=None,
            target=_check_pending_entries_list,
            name="ingestion-pending-entries-checker-thread",
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
            downstream_tasks, _ = consumer.get_and_process_batch(
                batch_size = CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            if downstream_tasks:
                LOGGER.info(f"Processed batch: obtained {len(downstream_tasks)} tasks for downstream production.")
                producer.produce_batch_to_disk(list(downstream_tasks.values()))
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