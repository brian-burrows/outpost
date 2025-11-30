from typing import TypeVar
from uuid import UUID
from pydantic import BaseModel

class BaseTask(BaseModel):
    task_id: UUID

# Used for type hinting generic tasks
TaskType = TypeVar("TaskType", bound = BaseTask)