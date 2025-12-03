import logging
import os

import redis
from etl.deduplication import RedisDeduplicationCacheRepository
from etl.limiter import RedisDailyRateLimiterDao
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.retry import Retry

logging.basicConfig(
    encoding='utf-8', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)
REDIS_MASTER_HOST = os.environ.get("INGESTION_QUEUE_HOST", "redis-master")
REDIS_MASTER_PORT = 6379
CATEGORIZATION_SQLITE_DB = "/tmp/categorization_producer_tasks.db"
CATEGORIZATION_TABLE_NAME = "categorization_tasks"

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
