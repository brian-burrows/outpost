import logging
from uuid import uuid4

import redis
import requests
from etl.persistence import SqliteTaskOutbox
from etl.producer import TaskProducerManager
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask
from pybreaker import CircuitBreakerError
from src.breakers import LOCATION_FETCH_BREAKER
from src.db import REDIS_CONNECTION_POOL
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 5
API_LIMIT = 100
WEATHER_API_HOST = "outpost-api-weather"
WEATHER_API_PORT = "8000"
REDIS_STREAM_KEY = "owm-ingestion-stream"
SQLITE_DB_PATH = "/tmp/owm_producer_tasks.db"
MAX_STREAM_SIZE = 100000 

class ApiRequestError(Exception):
    """Custom exception for HTTP errors (e.g., 404, 500) from the API."""
    pass

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city_id: int
    model_config = {"frozen": True} # Makes the instance hashable for `set()`


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

PRODUCER_MANAGER = TaskProducerManager(
    task_queue=RedisTaskQueueRepository(
        client=redis.StrictRedis(connection_pool=REDIS_CONNECTION_POOL),
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