from outpost.core.exceptions import POSTGRES_TRANSIENT_DB_ERRORS
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)


def postgres_async_retry_factory(
    num_retries: int = 4, 
    max_wait_seconds: int = 8, 
    jitter_seconds: int = 1,
    initial_wait_seconds: int = 1
) -> AsyncRetrying:
    return AsyncRetrying(
        stop=stop_after_attempt(num_retries), 
        wait=wait_exponential_jitter(initial=initial_wait_seconds, max=max_wait_seconds, jitter=jitter_seconds),
        retry=retry_if_exception_type(POSTGRES_TRANSIENT_DB_ERRORS),
        reraise=True 
    )