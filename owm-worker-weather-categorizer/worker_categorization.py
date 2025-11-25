import logging
import os
import random
import sys
import time
from typing import Any

import redis
from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.deduplication import (
    RedisDeduplicationCacheRepository,
    with_deduplication_caching,
)
from etl.handlers import SignalCatcher
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
CATEGORIZATION_EXPIRY_TIME_MS = 2.16e+7
CATEGORIZATION_REDIS_CONNECTION_POOL = redis.BlockingConnectionPool(
    host= CATEGORIZATION_REDIS_HOST,
    port= CATEGORIZATION_REDIS_PORT,
    retry=Retry(ExponentialWithJitterBackoff(), 8),
    retry_on_error=[BusyLoadingError, RedisConnectionError],
    health_check_interval = 3,
    socket_connect_timeout = 15,
    socket_timeout = 5,
    max_connections = 2 
)
UPSTREAM_QUEUE_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max=10, 
    reset_timeout = 60, 
)
DLQ_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max=10,
    reset_timeout= 60,
)
DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL),
    cache_key = "weather:categorization:deduplication:cache"
)
LOGGER = logging.getLogger(__name__)

class WeatherCategorizationTask(BaseTask):
    city: str 
    state: str 
    historical_data: list[dict[str, Any]]
    forecast_data: list[dict[str, Any]]

class SimulatedError(Exception):
    pass

@with_deduplication_caching(DEDUPLICATION_CACHE)
def _process_task(task: WeatherCategorizationTask) -> bool:
    """Simulates the final data sink/storage operation."""
    LOGGER.info(f"Simulating final write to database for Task ID: {task.task_id}")
    time.sleep(0.01)
    if random.random() < 0.1:
        LOGGER.error("Simulated failure during final data sink.")
        raise SimulatedError("This error simulates some weird bug in our code")
    return True

def _make_upstream_consumer_and_dlq():
    redis_client = redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL)
    task_queue = RedisTaskQueueRepository(
        client=redis_client,
        stream_key = CATEGORIZATION_STREAM_KEY,
        group_name=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        task_model=WeatherCategorizationTask
    )
    dlq = RedisTaskQueueRepository(
        client=redis_client,
        stream_key = f"dlq:{CATEGORIZATION_STREAM_KEY}",
        task_model=WeatherCategorizationTask,
        max_stream_length=10000
    )
    # This code executes locally, so we do not need rate limiting, circuit breaking
    # or retry logic on the TaskConsumer instance
    cat_task_consumer = TaskConsumer(executable=_process_task)
    return TaskProcessingManager(
        task_queue = task_queue,
        dead_letter_queue = dlq,
        task_consumer = cat_task_consumer,
        queue_breaker = UPSTREAM_QUEUE_CIRCUIT_BREAKER,
        dlq_breaker = DLQ_CIRCUIT_BREAKER
    )

def main(catcher):
    consumer = _make_upstream_consumer_and_dlq()
    try:
        while not catcher.stop_event.wait(timeout=1):
            consumer.get_and_process_batch(
                batch_size = CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            # We'll do this in the main thread, since the logic
            # is essentially identical
            consumer.get_and_process_stuck_tasks(
                max_idle_ms = CATEGORIZATION_MAX_IDLE_MS,
                expiry_time_ms = CATEGORIZATION_EXPIRY_TIME_MS,
                max_retries = CATEGORIZATION_MAX_RETRIES,
                batch_size = CONSUME_COUNT,
            )
    finally:
        catcher.stop_event.set()
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