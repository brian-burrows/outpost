import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, TypeVar
from owm.exceptions import RateLimitExceededError

from redis import StrictRedis
from redis.exceptions import ConnectionError as RedisConnectionError

T = TypeVar('T') 
F = TypeVar('F', bound=Callable[..., Any])

LOGGER = logging.getLogger(__name__)

class RateLimiterInterface(ABC):
    @abstractmethod
    def allow_request(self) -> bool:
        """Attempts to consume one unit of allowance. Returns True if allowed, False otherwise."""
        pass

class RedisDailyRateLimiterDao(RateLimiterInterface):
    """Implements a distributed fixed-window daily rate limiter using Redis's atomic INCR command."""
    def __init__(
        self, 
        client: StrictRedis,
        base_key: str, 
        max_requests: int,
    ):
        self.client = client
        self.max_requests = max_requests
        self.base_key = base_key
        # TTL: 48 hours in seconds (to cover potential edge cases around midnight)
        self.two_days_in_seconds = 2 * 86400 

    def _get_key_for_today(self) -> str:
        """Generates a key specific to the current UTC date."""
        today_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        return f"{self.base_key}:{today_utc}"

    def allow_request(self) -> bool:
        key = self._get_key_for_today()
        try:
            count = self.client.incr(key)
            LOGGER.debug(f"For Key {key}, the current request count is {count}")
            if count == 1:
                self.client.expire(key, self.two_days_in_seconds)
            return count <= self.max_requests
        except RedisConnectionError as e:
            LOGGER.critical(f"Redis rate limiter connection failed; {e}. DENYING API CALL.")
            return False
        except Exception as e:
            LOGGER.error(f"Unexpected error during rate limiting: {e}. DENYING API CALL.")
            return False
        
class DummyRateLimiter(RateLimiterInterface):
    def allow_request(self):
        return True
    
def with_rate_limiting(rate_limiter: 'RateLimiterInterface') -> Callable[[F], F]:
    """Decorator to enforce a rate limit before executing the target function."""
    def decorator(func: F) -> F:
        def wrapper(task: T) -> Any:
            if not rate_limiter.allow_request():
                LOGGER.warning(
                    f"Rate limit exceeded for task ID {task.task_id}. Raising error for PEL retry."
                )
                raise RateLimitExceededError(f"Rate limit exceeded for task ID {task.task_id}.")
            return func(task)
        return wrapper
    return decorator