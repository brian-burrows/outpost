import asyncio
import logging

import pytest
from pybreaker import CircuitBreaker, CircuitBreakerError

from outpost.core.breakers import DB_READER_CIRCUIT_BREAKER, DB_WRITER_CIRCUIT_BREAKER
from outpost.core.exceptions import POSTGRES_PERSISTENT_DB_ERRORS

LOGGER = logging.getLogger()

print(POSTGRES_PERSISTENT_DB_ERRORS)

EXPECTED_CONFIG = {
    'fail_max': 20,
    'reset_timeout': 120,
    'success_threshold': 1,
}

class TestBreakerFailure(BaseException):
    """Used specifically for testing that the breaker fails."""
    pass

@pytest.mark.parametrize(
    "breaker_name, breaker_instance",
    [
        ("DB_READER_CIRCUIT_BREAKER", DB_READER_CIRCUIT_BREAKER),
        ("DB_WRITER_CIRCUIT_BREAKER", DB_WRITER_CIRCUIT_BREAKER),
    ],
)
def test_circuit_breaker_configuration(breaker_name, breaker_instance):
    """
    Tests that the DB_READER and DB_WRITER CircuitBreaker objects
    are initialized with the correct parameters.
    """
    assert isinstance(breaker_instance, CircuitBreaker), f"{breaker_name} is not a CircuitBreaker instance."
    assert breaker_instance.fail_max == EXPECTED_CONFIG['fail_max'], \
        f"{breaker_name} fail_max mismatch."
    assert breaker_instance.reset_timeout == EXPECTED_CONFIG['reset_timeout'], \
        f"{breaker_name} reset_timeout mismatch."
    assert breaker_instance.success_threshold == EXPECTED_CONFIG['success_threshold'], \
        f"{breaker_name} success_threshold mismatch."
    assert breaker_instance.current_state == 'closed', \
        f"{breaker_name} initial state is incorrect."
    def fail():
        raise TestBreakerFailure("Simulated system outage failure")
    for _ in range(breaker_instance.fail_max):
        try:
            print("Before call:", breaker_instance.fail_counter, breaker_instance.success_counter)
            breaker_instance.call(fail)
        except TestBreakerFailure as e:
            print("Caught:", e, "is_system_error?", breaker_instance.is_system_error(e))
        except CircuitBreakerError:
            print("Breaker open")
    assert breaker_instance.current_state == 'open', \
        f"{breaker_name} failed to open after {breaker_instance.fail_max} failures."
    with pytest.raises(CircuitBreakerError):
        breaker_instance.call(lambda: "Should not execute")

@pytest.mark.parametrize(
    "breaker_instance, exception_class",
    [
        (DB_READER_CIRCUIT_BREAKER, exc)
        for exc in POSTGRES_PERSISTENT_DB_ERRORS
    ] + [
        (DB_WRITER_CIRCUIT_BREAKER, exc)
        for exc in POSTGRES_PERSISTENT_DB_ERRORS
    ]
)
def test_excluded_exceptions_are_skipped(breaker_instance, exception_class):
    """
    Ensure that exceptions listed in POSTGRES_PERSISTENT_DB_ERRORS
    do not count as system errors and do not open the breaker.
    """
    def raise_exc():
        raise exception_class("Simulated persistent DB exception")
    for _ in range(breaker_instance.fail_max * 2):
        try:
            breaker_instance.call(raise_exc)
        except exception_class:
            assert not breaker_instance.is_system_error(exception_class("Test")), \
                f"{exception_class.__name__} should be excluded from system errors"
        except Exception:
            pytest.fail("Unexpected exception raised")
    assert breaker_instance.current_state == "closed", \
        f"Breaker opened on excluded exception {exception_class.__name__}"

class TestAsyncBreakerException(Exception):
    pass

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "breaker_instance",
    [DB_READER_CIRCUIT_BREAKER, DB_WRITER_CIRCUIT_BREAKER]
)
async def test_async_circuit_breaker_skip_excluded_exceptions(breaker_instance):
    """
    Ensure that async functions raising excluded exceptions do NOT trip the breaker.
    """
    async def raise_excluded(exc_class):
        raise exc_class("Simulated persistent DB exception")
    for exc_class in POSTGRES_PERSISTENT_DB_ERRORS:
        for _ in range(5):
            try:
                with breaker_instance.calling():
                    await raise_excluded(exc_class)
            except exc_class:
                assert not breaker_instance.is_system_error(exc_class("test")), \
                    f"{exc_class.__name__} should be excluded from system errors"
            except Exception as e:
                pytest.fail(f"Unexpected exception raised for {exc_class.__name__}, {e}")
    assert breaker_instance.current_state == "closed", \
        "Breaker opened for excluded exceptions"

class TestAsyncBreakerFailure(Exception):
    """Used specifically for testing that the breaker opens on system errors."""
    pass

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "breaker_instance",
    [DB_READER_CIRCUIT_BREAKER, DB_WRITER_CIRCUIT_BREAKER]
)
async def test_async_circuit_breaker_trips_on_system_errors(breaker_instance):
    """
    Ensure that async functions raising non-excluded exceptions DO trip the breaker.
    """
    async def raise_system_error():
        raise TestAsyncBreakerFailure("Simulated system failure")
    for _ in range(breaker_instance.fail_max):
        try:
            with breaker_instance.calling():
                await raise_system_error()
        except (TestAsyncBreakerFailure, CircuitBreakerError) as e:
            assert breaker_instance.is_system_error(e), \
                "System error should be counted by the breaker"
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")
    assert breaker_instance.current_state == "open", \
        "Breaker did not open after system errors"
    with pytest.raises(CircuitBreakerError):
        with breaker_instance.calling():
            await raise_system_error()