import os

import redis
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry

REDIS_HOST = os.environ.get("INGESTION_QUEUE_HOST", "redis-master")
REDIS_PORT = 6379

REDIS_CONNECTION_POOL = redis.BlockingConnectionPool(
    host= REDIS_HOST,
    port= REDIS_PORT,
    retry=Retry(ExponentialWithJitterBackoff(), 8),
    retry_on_error=[BusyLoadingError, RedisConnectionError],
    health_check_interval = 3,
    socket_connect_timeout = 15,
    socket_timeout = 5,
    max_connections = 2
)