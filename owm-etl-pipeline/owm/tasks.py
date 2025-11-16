from typing import Any, TypeVar
from uuid import UUID
from pydantic import BaseModel

class BaseTask(BaseModel):
    task_id: UUID

class OwmIngestionTask(BaseTask):
    latitude_deg: float 
    longitude_deg: float
    city: str
    state: str 
    forecast_duration_hours: int = 72
    historical_duration_hours: int = 72

class WeatherCategorizationTask(BaseTask):
    city: str 
    state: str 
    historical_data: list[dict[str, Any]]
    forecast_data: list[dict[str, Any]]

# Used for type hinting generic tasks
TaskType = TypeVar("TaskType", bound = BaseTask)