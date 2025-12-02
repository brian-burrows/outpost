from pybreaker import CircuitBreaker
from outpost.core.exceptions import POSTGRES_PERSISTENT_DB_ERRORS

def make_excluder(*exceptions):
    """Fix CircuitBreaker for exceptions that are Callables and not Types
    
    In PyBreaker, exclude:
        An optional iterable of Exception classes (or callables). 
        Exceptions listed here are not considered system errors. 
        If a callable is provided, it should take a single exception instance
        and return True if it should be excluded, or False otherwise.

    Some error types (see `POSTGRES_PERSISTENT_DB_ERRORS`) are NOT Types but ARE callables,
    which creates a situation where ALL exceptions are excluded. So, we map them
    to actual callables that return booleans, rather than 

    See CircuitBreaker.is_system_error for more details.

    """
    def excluder(exc):
        return isinstance(exc, exceptions)
    return excluder

DB_READER_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max = 20, 
    reset_timeout = 120, 
    success_threshold = 1, 
    exclude=[make_excluder(*POSTGRES_PERSISTENT_DB_ERRORS)],
)

DB_WRITER_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max = 20,
    reset_timeout=120,
    success_threshold=1,
    exclude=[make_excluder(*POSTGRES_PERSISTENT_DB_ERRORS)],
)