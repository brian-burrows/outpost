import logging
import sqlite3
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, List, Type

from owm.tasks import UUID, TaskType

LOGGER = logging.getLogger(__name__)

def _adapt_uuid(val):
    return str(val)

sqlite3.register_adapter(uuid.UUID, _adapt_uuid)

def _get_current_time():
    return time.time()

class TaskOutboxInterface(ABC):
    """
    Interface for local, persistent storage to track task state.
    """
    def __init__(self, db_connection: Any, task_type: Type[TaskType]):
        self.db_connection = db_connection 
        self.task_type = task_type
        self.STATUS_COLUMN = 'status' 

    @abstractmethod 
    def create_task_table(self) -> None:
        """Creates the necessary database table if it doesn't exist."""
        pass

    @abstractmethod
    def insert_tasks(self, tasks: List[TaskType]) -> None:
        """Inserts tasks into local storage with an initial 'PENDING' state."""
        pass

    @abstractmethod
    def select_pending_tasks(self, limit: int) -> List[TaskType]:
        """Selects tasks ready for processing, i.e., in 'PENDING' state."""
        pass
    
    @abstractmethod
    def update_tasks_status(self, task_ids: List[UUID]) -> None:
        """Updates the status of tasks after a successful operation to 'QUEUED'."""
        pass
        
    @abstractmethod
    def delete_completed_tasks(self) -> None:
        """Removes tasks that have been fully processed and are no longer needed."""
        pass

    @abstractmethod 
    def delete_old_tasks(self, expire_time_seconds: int):
        """Removes tasks that have expired."""
        pass