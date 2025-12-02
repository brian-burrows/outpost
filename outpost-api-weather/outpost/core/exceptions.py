import functools
import logging

from asyncpg.exceptions import (
    DeadlockDetectedError,
    ForeignKeyViolationError,
    IntegrityConstraintViolationError,
    NotNullViolationError,
    PostgresSyntaxError,
    SerializationError,
    TooManyConnectionsError,
    TransactionRollbackError,
    UndefinedTableError,
    UniqueViolationError,
)
from fastapi import status
from fastapi.exceptions import HTTPException
from pybreaker import CircuitBreakerError

LOGGER = logging.getLogger(__name__)

POSTGRES_TRANSIENT_DB_ERRORS = (
    ConnectionError,
    TimeoutError,
    TooManyConnectionsError,
    SerializationError,
    DeadlockDetectedError,
    TransactionRollbackError,
)

POSTGRES_PERSISTENT_DB_ERRORS = (
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
    IntegrityConstraintViolationError
)

def async_map_postgres_exceptions_to_http(func):
    """Wraps a FastAPI endpoint to handle common sqlite/postgres database errors.
    
    Passes through HTTPExceptions that are raised directly.

    """
    # functools.wraps is necessary to preserve metadata about `func` so that
    # FastAPI dependency injection and Pydantic mapping can be preserved
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
        except HTTPException:
            raise
        except CircuitBreakerError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database write service is currently unavailable (circuit open)."
            )
        except POSTGRES_PERSISTENT_DB_ERRORS as e:
            LOGGER.warning(f"Data Integrity Violation in /forecast: {e.__class__.__name__}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Input data violates database constraints. Error: {e.__class__.__name__}"
            )
            
        except (PostgresSyntaxError, UndefinedTableError) as e:
            LOGGER.error(f"Fatal unrecoverable SQL/DB error: {e.__class__.__name__} - {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="A critical database configuration or code error occurred."
            )
        except Exception as e:
            if isinstance(e, POSTGRES_TRANSIENT_DB_ERRORS):
                LOGGER.error(f"PostgreSQL failed after all retries (Transient Error): {e.__class__.__name__}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Database service outage after retry failures."
                )
            else:
                LOGGER.error(f"Final uncaught database exception: {e.__class__.__name__} - {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="An unknown internal database error occurred."
                )
        return result
    return wrapper