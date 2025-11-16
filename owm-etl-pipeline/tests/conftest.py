import os
import subprocess
import time

import docker
import pytest
from redis import StrictRedis
from logging import getLogger, basicConfig, DEBUG

basicConfig(
    encoding='utf-8', 
    level=DEBUG , 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

TEST_REDIS_HOST = os.environ["INGESTION_REDIS_HOST"]
TEST_REDIS_PORT = os.environ["INGESTION_REDIS_PORT"]
CONTAINER_NAME = os.environ["REDIS_CONTAINER_NAME"]
REDIS_IMAGE = os.environ["REDIS_IMAGE"]

@pytest.fixture(scope="session", autouse=True)
def docker_redis_service():
    """Manages the lifecycle of a dedicated Redis container for the test session."""
    client = docker.from_env()
    container = None
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
    client = StrictRedis(host=TEST_REDIS_HOST, port=TEST_REDIS_PORT, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()