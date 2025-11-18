import time
from logging import DEBUG, basicConfig, getLogger
from unittest.mock import patch
from uuid import uuid4

import docker
import pytest
from pydantic import Field
from redis import StrictRedis

from etl.persistence import SqliteTaskOutbox
from etl.queue import RedisTaskQueueRepository
from etl.tasks import BaseTask

basicConfig(
    encoding='utf-8', 
    level=DEBUG , 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

# TODO: Map to env variables
TEST_REDIS_HOST = "localhost"
TEST_REDIS_PORT = 6379
CONTAINER_NAME = "test-redis-container"
REDIS_IMAGE = "redis:7.0-alpine"

@pytest.fixture(scope="session", autouse=True)
def docker_redis_service():
    """
    Manages the lifecycle of a dedicated Redis container for the test session.
    Includes preemptive cleanup to handle orphaned containers from previous failed runs.
    """
    client = docker.from_env()
    container = None
    try:
        existing_container = client.containers.get(CONTAINER_NAME)
        logger.debug(f"Found existing container '{CONTAINER_NAME}'. Forcing removal...")
        existing_container.remove(force=True)
    except docker.errors.NotFound:
        pass
    except Exception as e:
        logger.warning(f"Error during preemptive container cleanup: {e}")
    try:
        logger.debug(f"\nStarting Redis container '{CONTAINER_NAME}'...")
        container = client.containers.run(
            image=REDIS_IMAGE,
            name=CONTAINER_NAME,
            detach=True,
            ports={"6379/tcp": TEST_REDIS_PORT}, 
            command='redis-server --appendonly yes'
        )
        redis_conn = StrictRedis(host=TEST_REDIS_HOST, port=TEST_REDIS_PORT)
        max_attempts = 15
        for _ in range(max_attempts):
            try:
                if redis_conn.ping():
                    logger.debug("Redis is ready and responding!")
                    break
            except Exception:
                time.sleep(1)
        else:
            raise ConnectionError("Redis container failed to start or respond after timeout.")
        yield
        
    finally:
        if container:
            logger.debug(f"\nStopping and removing container '{CONTAINER_NAME}'...")
            try:
                container.stop()
                container.remove(v=True, force=True)
            except docker.errors.NotFound:
                pass 
        logger.debug("Redis container torn down.")

@pytest.fixture(scope="function")
def redis_client():
    """Provides a clean, connected Redis client for each individual test function.
    
    Other connections may be made to the same Redis host (e.g., to create streams), 
    but this one can be used to verify properties about the Redis instance within each test.
    """
    # Decode_responses MUST be false for deserialization to work
    client = StrictRedis(host=TEST_REDIS_HOST, port=TEST_REDIS_PORT, decode_responses=False)
    client.flushdb()
    yield client
    client.flushdb()


class Task(BaseTask):
    location_id: int = Field(..., description="ID of the location to process.")
    api_name: str = Field(..., description="Name of the API to call.")

@pytest.fixture
def stream_key() -> str:
    """Fixture for a unique stream key per test function."""
    return f"test-stream-{int(time.time() * 1000)}"

@pytest.fixture
def repo(redis_client: StrictRedis, stream_key: str) -> RedisTaskQueueRepository:
    """Provides an initialized repository instance for consumer operations."""
    return RedisTaskQueueRepository(
        client=redis_client,
        stream_key=stream_key,
        task_model=Task,
        group_name="TEST_GROUP",
        consumer_name="TEST_CONSUMER_1",
    )

@pytest.fixture
def sample_task_factory():
    """Provides a function (factory) to create a list of sample tasks of a specified size."""
    def _factory(num_tasks: int = 3):
        return [
            Task(task_id=uuid4(), location_id=i, api_name=f"owm-{i}")
            for i in range(101, 101 + num_tasks)
        ]
    return _factory

@pytest.fixture
def test_task_model():
    """Provides the Pydantic model used for persistence testing."""
    return Task

@pytest.fixture
def mock_time():
    """Mocks the time function to return a fixed, deterministic time value."""
    fixed_time = 1700000000.0
    with patch('etl.persistence._get_current_time', return_value=fixed_time) as mock:
        yield mock

@pytest.fixture
def sqlite_outbox(test_task_model, mock_time, tmp_path):
    """
    Provides an SqliteTaskOutbox instance connected to a file-based SQLite database
    in a temporary directory for robust testing.
    """
    db_file = tmp_path / "test_outbox.db"
    outbox = SqliteTaskOutbox(db_path=str(db_file), task_type=test_task_model)
    yield outbox
