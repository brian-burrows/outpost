from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)
from uuid import uuid4

import pytest
from worker_ingestion import (
    OwmIngestionTask,
    _fetch_historical_data,
    _fetch_weather_forecast,
)


@pytest.fixture
def task():
    return OwmIngestionTask(
        task_id = uuid4(),
        latitude_deg = 0,
        longitude_deg = 0,
        city_id = 1
    )

def test_mock_historical_works(task):
    previous_date = date.today() - timedelta(days=1)
    historical_data = _fetch_historical_data(task, previous_date)
    assert isinstance(historical_data, dict)
    assert historical_data['city_id'] == 1
    assert 'measured_at_ts_utc' in historical_data


def test_mock_forecast_works(task):
    current_hour = (
        datetime
        .now()
        .astimezone(timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
    )
    forecast_data = _fetch_weather_forecast(task, current_hour)
    assert isinstance(forecast_data, list)
    assert len(forecast_data) == 48
    assert 'forecast_timestamp_utc' in forecast_data[0]
    assert forecast_data[0]['city_id'] == 1