import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, TypeVar

from redis import StrictRedis
from redis.exceptions import ConnectionError as RedisConnectionError

T = TypeVar('T') 
F = TypeVar('F', bound=Callable[..., Any])

LOGGER = logging.getLogger(__name__)

class RateLimiterInterface(ABC):
    """Interface for consuming an allowance unit in a rate-limiting system."""
    @abstractmethod
    def allow_request(self) -> bool:
        """Attempts to consume one unit of allowance.

        Returns
        -------
        bool
            True if the request is allowed (within the limit), False otherwise.
            
        """
        pass

class RedisDailyRateLimiterDao(RateLimiterInterface):
    """Implements a distributed fixed-window daily rate limiter using Redis's atomic INCR command."""
    def __init__(
        self, 
        client: StrictRedis,
        base_key: str, 
        max_requests: int,
    ):
        """Initializes the fixed-window rate limiter DAO.

        Parameters
        ----------
        client : StrictRedis
            The connected Redis client instance.
        base_key : str
            The base key used for the rate limit counter (e.g., 'service:api:limit').
        max_requests : int
            The maximum number of requests allowed per 24-hour window.
        """
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
        """Attempts to consume one unit of allowance using Redis's atomic INCR.
        
        If the counter is initialized (count == 1), it sets the TTL to 48 hours.

        Returns
        -------
        bool
            True if the request count is less than or equal to `max_requests`, False otherwise.

        """
        key = self._get_key_for_today()
        try:
            count = self.client.incr(key)
            LOGGER.info(f"For Key {key}, the current request count is {count}")
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
        """Dummy implementation that always allows the request.
        
        Returns
        -------
        bool
            Always True.

        """
        return True