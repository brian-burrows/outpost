import pytest
from tenacity import AsyncRetrying
from outpost.core.retries import postgres_async_retry_factory
import logging

LOGGER = logging.getLogger()
class MockTransientError(Exception):
    pass

@pytest.mark.asyncio
async def test_factory_returns_async_retrying_instance():
    """Verifies that the factory returns an instance of AsyncRetrying."""
    retryer = postgres_async_retry_factory()
    assert isinstance(retryer, AsyncRetrying)

@pytest.mark.asyncio
async def test_factory_configures_correct_stop_attempt():
    """Verifies that the retryer stops after the specified number of attempts."""
    num_retries = 1
    retryer = postgres_async_retry_factory(num_retries=num_retries, max_wait_seconds=1)
    attempts_made = 0
    async def failing_call():
        nonlocal attempts_made
        attempts_made += 1
        raise ConnectionError("Simulated transient failure")
    with pytest.raises(ConnectionError):
        async for attempt in retryer:
            with attempt:
                await failing_call()
    assert attempts_made == num_retries

@pytest.mark.asyncio
async def test_factory_retries_on_transient_error():
    """Verifies that a known transient error triggers a retry."""
    retryer = postgres_async_retry_factory(num_retries=2, max_wait_seconds=1)
    attempts_made = 0
    async def failing_then_succeeding_call():
        nonlocal attempts_made
        attempts_made += 1
        if attempts_made < 2:
            raise ConnectionError("Transient failure")
        return "Success"
    result = None
    async for attempt in retryer:
        with attempt:
            result = await failing_then_succeeding_call()
    assert result == "Success"
    assert attempts_made == 2

@pytest.mark.asyncio
async def test_factory_does_not_retry_on_unknown_error():
    """Verifies that a generic exception is immediately re-raised."""
    retryer = postgres_async_retry_factory(num_retries=4, max_wait_seconds=1) 
    attempts_made = 0
    async def failing_call():
        nonlocal attempts_made
        attempts_made += 1
        raise ValueError("Non-transient error")
    with pytest.raises(ValueError) as excinfo:
        async for attempt in retryer:
            with attempt:
                await failing_call()
    assert "Non-transient error" in str(excinfo.value)
    assert attempts_made == 1

@pytest.mark.asyncio
async def test_factory_reraise_on_failure():
    """Verifies that reraise=True is set, making RetryError contain the original exception."""
    retryer = postgres_async_retry_factory(num_retries=1)
    async def failing_call():
        raise MockTransientError("Reraise Test")
    with pytest.raises(MockTransientError):
        async for attempt in retryer:
            with attempt:
                await failing_call()

@pytest.mark.asyncio
async def test_factory_does_not_retry_on_success():
    """Verifies that a generic exception is immediately re-raised."""
    retryer = postgres_async_retry_factory(num_retries=4, max_wait_seconds=1) 
    attempts_made = 0
    async def success_call():
        nonlocal attempts_made
        attempts_made += 1
        return "Hello World"
    async for attempt in retryer:
        with attempt:
            await success_call()
    assert attempts_made == 1