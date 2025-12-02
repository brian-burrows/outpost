import pytest
from pybreaker import CircuitBreaker, CircuitBreakerError
from outpost.core.breakers import DB_READER_CIRCUIT_BREAKER, DB_WRITER_CIRCUIT_BREAKER

EXPECTED_CONFIG = {
    'fail_max': 20,
    'reset_timeout': 120,
    'success_threshold': 1,
}

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
        raise ValueError("Simulated failure")
    for _ in range(breaker_instance.fail_max):
        try:
            breaker_instance.call(fail)
        except (ValueError, CircuitBreakerError):
            pass
    assert breaker_instance.current_state == 'open', \
        f"{breaker_name} failed to open after {breaker_instance.fail_max} failures."
    with pytest.raises(CircuitBreakerError):
        breaker_instance.call(lambda: "Should not execute")