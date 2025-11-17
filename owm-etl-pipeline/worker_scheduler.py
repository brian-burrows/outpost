import logging
import random
import sys
import time
import os # Import os to read environment variables
from uuid import uuid4

import redis
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry

# Assuming these modules exist and are available in the container
from owm.producer import TaskProducerManager
from owm.persistence import SqliteTaskOutbox
from owm.queue import RedisTaskQueueRepository
from owm.tasks import OwmIngestionTask

# --- Consolidated Redis Configuration ---
# Uses the environment variable set in docker-compose (e.g., 'redis-master')
REDIS_HOST = os.environ.get("INGESTION_QUEUE_HOST", "redis-master")
REDIS_PORT = 6379

REDIS_STREAM_KEY = "owm-ingestion-stream"
SQLITE_DB_PATH = "/tmp/owm_producer_tasks.db"

MAX_STREAM_SIZE = 100000 
TASK_SUBMIT_BATCH_SIZE = 500

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

LOCATION_DATA = [
    ("colorado-springs", "colorado", 38.83, -104.82), 
    ("denver", "colorado", 39.74, -104.99),        
    ("crested-butte", "colorado", 38.87, -106.99), 
    ("fruita", "colorado", 39.14, -108.73),         
    ("durango", "colorado", 37.27, -107.88), 
    ("moab", "utah", 38.57, -109.55), 
    ("saint-george", "utah", 37.10, -113.57),
    ("park-city", "utah", 40.65, -111.50),
    ("hurricane", "utah", 37.18, -113.40),
    ("sedona", "arizona", 34.87, -111.76), 
    ("flagstaff", "arizona", 35.20, -111.65), 
    ("prescott", "arizona", 34.55, -112.45),
    ("phoenix", "arizona", 33.45, -112.07),
    ("boulder-city", "nevada", 35.95, -114.83),
    ("las-vegas", "nevada", 36.17, -115.14),
    ("reno", "nevada", 39.53, -119.82),
]

def get_paginated_location_data(page = 0, limit = 5):
    """Simulates fetching paginated location data to be ingested."""
    start_index = limit * page
    time.sleep(random.random() * 0.1) 
    location_data = LOCATION_DATA[start_index: start_index + limit]
    return [
        OwmIngestionTask(task_id = uuid4(), city=c, state=s, latitude_deg=lat, longitude_deg=lon)
        for c, s, lat, lon in location_data
    ]

def main():
    """Executes single, one-off run task generation for all locations in the database"""
    LOGGER.info("Cron Scheduler started: Initializing producer stack.")
    redis_connection_pool = redis.BlockingConnectionPool(
        host= REDIS_HOST,
        port= REDIS_PORT,
        retry=Retry(ExponentialWithJitterBackoff(), 8),
        retry_on_error=[BusyLoadingError, RedisConnectionError],
        health_check_interval = 3,
        socket_connect_timeout = 15,
        socket_timeout = 5,
        max_connections = 2
    )
    manager = TaskProducerManager(
        task_queue=RedisTaskQueueRepository(
            client=redis.StrictRedis(connection_pool=redis_connection_pool),
            stream_key=REDIS_STREAM_KEY,
            task_model=OwmIngestionTask,
            group_name=None,
            consumer_name=None,
            max_stream_length=MAX_STREAM_SIZE
        ),
        storage= SqliteTaskOutbox(
            db_path=SQLITE_DB_PATH,
            task_type=OwmIngestionTask
        ),
    )
    start_time = time.time() 
    page = 0
    task_count = 0
    try:
        LOGGER.info("Starting pagination and task production.")
        while tasks := get_paginated_location_data(page=page, limit=5):
            manager.produce_batch_to_disk(tasks) 
            task_count += len(tasks)
            page += 1
            if len(tasks) < 5: 
                break 
        manager.flush_tasks_to_queue(batch_size=task_count)
        end_time = time.time()
        LOGGER.info(
            f"Cron job complete: Produced and Flushed {task_count} tasks "
            f"in {(end_time - start_time):.4f} seconds. Exiting gracefully."
        )
        return 0

    except Exception as e:
        LOGGER.fatal(f"Unhandled error during production run: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit_code = 0
    try:
        exit_code = main() 
    except Exception as e:
        # This handles errors outside of the main function, like connection failures
        LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
        exit_code = 1
    finally:
        LOGGER.info("Shutdown complete.")
        sys.exit(exit_code)