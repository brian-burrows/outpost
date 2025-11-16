from abc import ABC, abstractmethod
from owm.tasks import TaskType
import redis
import json
from typing import Type, Any
import logging 

LOGGER = logging.getLogger(__name__)

class TaskQueueRepositoryInterface(ABC):
    @abstractmethod
    def dequeue_tasks(self, count: int, block_ms: int | None = None) -> list[tuple[str, TaskType]]:
        """Fetches new tasks from the queue"""
        pass
    
    @abstractmethod
    def enqueue_tasks(self, task: list[TaskType]) -> list[Any]:
        pass

    @abstractmethod
    def recover_stuck_tasks(self, max_idle_ms: int, max_retries: int, batch_size: int) -> tuple[
        list[tuple[str, TaskType]],
        list[tuple[str, TaskType]]
    ]:
        """Scans for and attempts to reclaim or flag stuck/failed tasks."""
        pass

    @abstractmethod
    def acknowledge_tasks(self, message_ids: list[str]) -> int:
        """Confirms processing of tasks, removing them from the pending list."""
        pass