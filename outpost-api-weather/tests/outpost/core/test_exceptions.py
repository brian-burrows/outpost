from unittest.mock import MagicMock, patch

import pytest
from asyncpg.exceptions import (
    PostgresSyntaxError,
    TooManyConnectionsError,
    UniqueViolationError,
)
from fastapi import status
from fastapi.exceptions import HTTPException
from pybreaker import CircuitBreakerError

from outpost.core.exceptions import (
    LOGGER,
    async_map_postgres_exceptions_to_http,
)


async def mock_endpoint(a: int, b: str, conn: MagicMock):
    """A mock function to be wrapped by the decorator."""
    if a == 100:
        return "Success"
    elif a == 200:
        raise ValueError("Unknown Error")
    raise ValueError("Should not happen")


@pytest.mark.asyncio
@patch.object(LOGGER, 'warning')
@patch.object(LOGGER, 'error')
async def test_successful_execution(mock_log_error, mock_log_warning):
    """Tests that the function returns the result correctly without exceptions."""
    wrapped_func = async_map_postgres_exceptions_to_http(mock_endpoint)
    # Test arguments to check functools.wraps works correctly
    result = await wrapped_func(a=100, b="test", conn=MagicMock())
    assert result == "Success"
    mock_log_error.assert_not_called()
    mock_log_warning.assert_not_called()

@pytest.mark.asyncio
@patch.object(LOGGER, 'warning')
async def test_persistent_db_error_maps_to_400(mock_log_warning):
    """Tests that a persistent error (e.g., UniqueViolationError) maps to 400."""
    async def failing_endpoint():
        raise UniqueViolationError()
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint)
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "data violates database constraints" in excinfo.value.detail
    mock_log_warning.assert_called_once()


@pytest.mark.asyncio
@patch.object(LOGGER, 'error')
async def test_fatal_sql_error_maps_to_500(mock_log_error):
    """Tests that a fatal SQL/DB error (e.g., PostgresSyntaxError) maps to 500."""
    async def failing_endpoint():
        raise PostgresSyntaxError("SELECT FROM * bad") 
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint) 
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "critical database configuration or code error occurred" in excinfo.value.detail
    mock_log_error.assert_called_once()

@pytest.mark.asyncio
@patch.object(LOGGER, 'error')
async def test_failed_transient_retry_maps_to_503(mock_log_error):
    """Tests that a final transient error (after retries fail) maps to 503."""
    async def failing_endpoint():
        # This simulates the final exception re-raised by tenacity
        raise TooManyConnectionsError()
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint)
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "Database service outage after retry failures" in excinfo.value.detail
    # Should log the final failure in the generic 'except Exception' block
    mock_log_error.assert_called_once() 


@pytest.mark.asyncio
async def test_circuit_breaker_error_maps_to_503():
    """Tests that a CircuitBreakerError maps directly to 503."""
    async def failing_endpoint():
        raise CircuitBreakerError("Circuit Open")
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint)
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "service is currently unavailable (circuit open)" in excinfo.value.detail

@pytest.mark.asyncio
@patch.object(LOGGER, 'error')
async def test_uncaught_exception_maps_to_500(mock_log_error):
    """Tests that a completely uncaught exception maps to a generic 500."""
    async def failing_endpoint():
        # Simulates an unhandled generic Python exception
        raise IndexError("List out of bounds")
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint)
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "An unknown internal database error occurred" in excinfo.value.detail
    mock_log_error.assert_called_once()


@pytest.mark.asyncio
async def test_http_exception_is_passed_through():
    """Tests that an HTTPException raised by the endpoint is NOT caught or mapped."""
    expected_exception = HTTPException(status_code=404, detail="Not Found")
    async def failing_endpoint():
        raise expected_exception
    wrapped_func = async_map_postgres_exceptions_to_http(failing_endpoint)
    with pytest.raises(HTTPException) as excinfo:
        await wrapped_func()
    assert excinfo.value is expected_exception
    assert excinfo.value.status_code == 404