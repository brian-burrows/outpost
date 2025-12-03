from pybreaker import CircuitBreaker

OPEN_WEATHER_MAPS_API_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
UPSTREAM_BROKER_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60 * 10)
DLQ_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout=60 * 10)
DOWNSTREAM_BROKER_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)
WEATHER_SERVICE_CIRCUIT_BREAKER = CircuitBreaker(fail_max=10, reset_timeout = 60 * 10)

