import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, TypeVar
from uuid import UUID

import redis
from redis.exceptions import ConnectionError as RedisConnectionError

from etl.exceptions import DuplicateTaskError

T = TypeVar('T') 
F = TypeVar('F', bound=Callable[..., Any])

LOGGER = logging.getLogger(__name__)


class DeduplicationCacheInterface(ABC):
    @abstractmethod
    def is_processed(self, task_id: UUID) -> bool:
        """Checks if a task_id has been recorded as processed.
        
        Parameters
        ----------
        task_id : UUID
            The task ID used to identify the task in the Cache.

        Returns
        -------
        bool : `True` if `task_id` is in the cache already.
        """
        pass

    @abstractmethod
    def record_processed(self, task_id: UUID) -> int:
        """Records a task_id as successfully processed.
        
        Parameters
        ----------
        task_id : UUID
            The task ID used to identify the task in the Cache.

        Returns
        -------
        int : 1 if the operation was successful.

        """
        pass
    
    @abstractmethod
    def discard(self, task_id: UUID) -> int:
        """Removes a task_id from the processed list (e.g., if the transaction failed).
        
        Parameters
        ----------
        task_id : UUID
            The task ID used to identify the task in the Cache.

        Returns
        -------
        int : 1 if the operation was successful.

        """
        pass

class RedisDeduplicationCacheRepository(DeduplicationCacheInterface):
    def __init__(self, client: redis.StrictRedis, cache_key: str):
        """Create a Redis deduplication cache repository.
        
        Parameters
        ----------
        client : StrictRedis
            A redis client that is connected to the target database.
        cache_key : str 
            The cache key representing the Redis set that will be used for
            deduplication of tasks.

        """
        self.client = client
        self.cache_key = cache_key
        pass 

    def is_processed(self, task_id: UUID) -> bool:
        """Checks if the `task_id` is stored in Redis under `cache_key`."""
        try:
            return bool(self.client.sismember(self.cache_key, str(task_id)))
        except RedisConnectionError as e:
            LOGGER.error("Failed to check set membership in redis set: {e}")
            raise e
        except Exception as e:
            LOGGER.error("Unhandled exception checking set membership: {e}")
            raise e

    def record_processed(self, task_id: UUID) -> int:
        """Add `task_id` to the Redis set under `cache_key`. Returns 1 if successful."""
        try:
            return self.client.sadd(self.cache_key, str(task_id))
        except RedisConnectionError as e:
            LOGGER.error(f"Failed to add item {task_id} to the redis set: {e}")
            raise e
        except Exception as e:
            LOGGER.error("Unhandled exception adding record to Redis set: {e}")
            raise e

    def discard(self, task_id: UUID) -> int:
        """Remove `task_id` from Redis under `cache_key`. Returns 1 if successful."""
        try:
            return self.client.srem(self.cache_key, str(task_id))
        except RedisConnectionError as e:
            LOGGER.error(f"Failed to remove item from the redis set due to connection error: {e}")
            raise e
        except Exception as e:
            LOGGER.error(f"Unhandled exception removing record from Redis set: {e}")
            raise e
        
def with_deduplication_caching(cache_dao: DeduplicationCacheInterface) -> Callable[[F], F]:
    """Decorator to skip execution if a task_id is found in the DAO."""
    def decorator(func: F) -> F:
        def wrapper(task: T) -> Any:
            task_id = task.task_id
            if cache_dao.is_processed(task_id):
                raise DuplicateTaskError(f"Task ID {task_id} has already been processed")
            try:
                response = func(task)
                cache_dao.record_processed(task_id)
                return response
            except Exception as e:
                cache_dao.discard(task_id)
                raise e
        return wrapper
    return decorator