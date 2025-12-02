import logging
import os
import sys
import time
from uuid import uuid4

import redis
import requests
from etl.persistence import SqliteTaskOutbox
from etl.producer import TaskProducerManager
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask
from pybreaker import CircuitBreaker, CircuitBreakerError
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

REDIS_HOST = os.environ.get("INGESTION_QUEUE_HOST", "redis-master")
REDIS_PORT = 6379

REDIS_STREAM_KEY = "owm-ingestion-stream"
SQLITE_DB_PATH = "/tmp/owm_producer_tasks.db"

WEATHER_API_HOST = "outpost-api-weather"
WEATHER_API_PORT = "8000"

MAX_STREAM_SIZE = 100000 
TASK_SUBMIT_BATCH_SIZE = 500
API_LIMIT = 100

MAX_RETRIES = 5
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 20
CIRCUIT_BREAKER_RESET_TIMEOUT = 60 * 10
LOCATION_FETCH_BREAKER = CircuitBreaker(
    fail_max=CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    reset_timeout=CIRCUIT_BREAKER_RESET_TIMEOUT
)

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city_id: int
    model_config = {"frozen": True} # Makes the instance hashable for `set()`


class ApiRequestError(Exception):
    """Custom exception for HTTP errors (e.g., 404, 500) from the API."""
    pass

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential_jitter(initial=5, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException, ApiRequestError)),
    reraise=True 
)
def _fetch_page_with_retries(page: int) -> dict:
    """
    Performs the actual API call with built-in retry logic.
    """
    url = f"http://{WEATHER_API_HOST}:{WEATHER_API_PORT}/cities/?page={page}&limit={API_LIMIT}"
    response = requests.get(url)
    if not response.ok:
        LOGGER.debug(f"API request failed with status {response.status_code} for page {page}. Retrying...")
        raise ApiRequestError(f"HTTP Error: {response.status_code}")
    
    return response.json()

@LOCATION_FETCH_BREAKER
def _fetch_page(page: int) -> dict:
    """
    Fetches a page, using the retry helper. The circuit breaker counts 
    the failure of the *entire retry sequence* as one failure.
    """
    return _fetch_page_with_retries(page)

def get_location_data(page: int = 0, locations: set[tuple] = None) -> set['OwmIngestionTask']:
    """
    Recursively fetches all city location data from the API with pagination,
    retries, and circuit breaking.
    """
    if locations is None:
        # Use a set to automatically handle duplicates and ensure safe collection
        locations = set()
    page_data = None
    try:
        page_data = _fetch_page(page)
    except CircuitBreakerError:
        LOGGER.error(
            f"Circuit Breaker is OPEN. Stopping fetch for page {page} and all subsequent pages."
        )
        return locations
    except (requests.exceptions.RequestException, ApiRequestError) as e:
        LOGGER.error(
            f"Failed to fetch page {page} after {MAX_RETRIES} attempts. Error: {e}"
        )
        return locations
    for city in page_data.get("cities", []):
        location_tuple = OwmIngestionTask(
            task_id=uuid4(),
            city_id=city["city_id"],
            latitude_deg=city["latitude_deg"],
            longitude_deg=city["longitude_deg"]
        )
        locations.add(location_tuple)
    total_count = page_data.get("total_count", 0)
    limit = page_data.get("limit", API_LIMIT) 
    if len(locations) < total_count and len(locations) % limit == 0:
        return get_location_data(page=page + 1, locations=locations)
    LOGGER.info(f"Finished fetching all {len(locations)} cities.")
    return locations


# LOCATION_DATA = [
#     ("colorado-springs", "colorado", 38.83, -104.82), 
#     ("denver", "colorado", 39.74, -104.99),        
#     ("crested-butte", "colorado", 38.87, -106.99), 
#     ("fruita", "colorado", 39.14, -108.73),         
#     ("durango", "colorado", 37.27, -107.88), 
#     ("moab", "utah", 38.57, -109.55), 
#     ("saint-george", "utah", 37.10, -113.57),
#     ("park-city", "utah", 40.65, -111.50),
#     ("hurricane", "utah", 37.18, -113.40),
#     ("sedona", "arizona", 34.87, -111.76), 
#     ("flagstaff", "arizona", 35.20, -111.65), 
#     ("prescott", "arizona", 34.55, -112.45),
#     ("phoenix", "arizona", 33.45, -112.07),
#     ("boulder-city", "nevada", 35.95, -114.83),
#     ("las-vegas", "nevada", 36.17, -115.14),
#     ("reno", "nevada", 39.53, -119.82),
# ]

# def get_paginated_location_data(page = 0, limit = 5):
#     """Simulates fetching paginated location data to be ingested."""
#     start_index = limit * page
#     time.sleep(random.random() * 0.1) 
#     location_data = LOCATION_DATA[start_index: start_index + limit]
#     return [
#         OwmIngestionTask(task_id = uuid4(), city=c, state=s, latitude_deg=lat, longitude_deg=lon)
#         for c, s, lat, lon in location_data
#     ]

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
    expire_time_seconds = 86400 * 2
    manager.clear_tasks(expire_time_seconds=expire_time_seconds)
    start_time = time.time() 
    try:
        LOGGER.info("Starting pagination and task production.")
        tasks = list(get_location_data())
        manager.produce_batch_to_disk(tasks) 
        total_num_flushed = 0
        while True:
            num_flushed = manager.flush_tasks_to_queue(batch_size=TASK_SUBMIT_BATCH_SIZE)
            total_num_flushed += num_flushed
            if num_flushed == 0:
                break
        end_time = time.time()
        LOGGER.info(
            f"Cron job complete: Produced and Flushed {total_num_flushed} tasks "
            f"in {(end_time - start_time):.4f} seconds. Exiting gracefully."
        )
        manager.clear_tasks(expire_time_seconds=expire_time_seconds)
        return 0
    except Exception as e:
        LOGGER.fatal(f"Unhandled error during production run: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    exit_code = 0
    try:
        exit_code = main() 
    except Exception as e:
        LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
        exit_code = 1
    finally:
        LOGGER.info("Shutdown complete.")
        sys.exit(exit_code)