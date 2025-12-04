import os

import redis
from etl.deduplication import (
    RedisDeduplicationCacheRepository,
)
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry

# TODO: Centralize magic numbers into environment variables, when it makes sense
CATEGORIZATION_REDIS_CONNECTION_POOL = redis.BlockingConnectionPool(
    host=os.environ.get("CATEGORIZATION_QUEUE_HOST", "redis-master"),
    port=6379,
    retry=Retry(ExponentialWithJitterBackoff(), 8),
    retry_on_error=[BusyLoadingError, RedisConnectionError],
    health_check_interval=3,
    socket_connect_timeout=15,
    socket_timeout=5,
    max_connections=2,
)
DEDUPLICATION_CACHE = RedisDeduplicationCacheRepository(
    client=redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL),
    cache_key="weather:categorization:deduplication:cache",
)
REDIS_CLIENT = redis.StrictRedis(connection_pool=CATEGORIZATION_REDIS_CONNECTION_POOL)