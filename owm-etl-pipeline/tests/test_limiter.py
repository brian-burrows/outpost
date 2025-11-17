from owm.limiter import RedisDailyRateLimiterDao
from unittest.mock import patch, MagicMock

def test_max_fail_on_rate_limit(redis_client):
    limiter = RedisDailyRateLimiterDao(
        client = redis_client, 
        base_key = "my:rate:limiter",
        max_requests = 2
    )
    for _, expected in zip(range(3), [True, True, False]):
        assert limiter.allow_request() == expected

def test_limit_resets_after_day_boundary(redis_client):
    limiter = RedisDailyRateLimiterDao(
        client=redis_client, 
        base_key="test:key", 
        max_requests=2
    )
    with patch('owm.limiter.datetime.datetime') as mock_dt:
        day1_mock_dt_object = MagicMock()
        day1_mock_dt_object.strftime.return_value = "2025-11-16"
        mock_dt.now.return_value = day1_mock_dt_object
        assert limiter.allow_request() is True  # Request 1 (Day 1)
        assert limiter.allow_request() is True  # Request 2 (Day 1)
        assert limiter.allow_request() is False # Request 3 (Blocked)
        day2_mock_dt_object = MagicMock()
        day2_mock_dt_object.strftime.return_value = "2025-11-17"
        mock_dt.now.return_value = day2_mock_dt_object
        assert limiter.allow_request() is True  # Request 1 (Day 2)