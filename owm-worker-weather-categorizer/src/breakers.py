from pybreaker import CircuitBreaker

# TODO: Need to specify specific exceptions that should trigger circuit breaking

UPSTREAM_QUEUE_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60)
WEATHER_API_CIRCUIT_BREAKER = CircuitBreaker(fail_max=5, reset_timeout=300)
DLQ_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60)
