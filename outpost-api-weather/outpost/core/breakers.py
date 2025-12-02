from pybreaker import CircuitBreaker

DB_READER_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max = 20, 
    reset_timeout = 120, 
    success_threshold = 1, 
)

DB_WRITER_CIRCUIT_BREAKER = CircuitBreaker(
    fail_max = 20,
    reset_timeout=120,
    success_threshold=1,
)