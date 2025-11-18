import logging
from typing import List
from uuid import UUID

from redis.exceptions import ConnectionError as RedisConnectionError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from etl.persistence import TaskOutboxInterface
from etl.queue import TaskQueueRepositoryInterface
from etl.tasks import TaskType

LOGGER = logging.getLogger(__name__)

class TaskProducerManager:
    """ Encapsulates functionality for a transactional outbox pattern.

    The transactional outbox pattern writes messages to disk in the main thread.
    A separate process or thread flushes messages to consumers (or a queue).

    Some transactional outbox patterns offer guarantees about the order of delivery.
    This implementation provides no such guarantees.

    More information can be found here:
    """
    def __init__(
        self, 
        task_queue: TaskQueueRepositoryInterface,
        storage: TaskOutboxInterface,
    ):
        self.task_queue = task_queue
        self.storage = storage

    @retry(
        wait=wait_random_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(2),
        reraise=True
    )
    def produce_batch_to_disk(self, tasks: List[TaskType]) -> List[UUID]:
        """
        Main thread API: Saves new tasks locally with status 'PENDING'.
        The background flusher thread handles the enqueueing later.
        Returns the Task UUIDs that were saved to disk.
        """
        if not tasks:
            return []
        task_ids = [task.task_id for task in tasks]
        try:
            self.storage.insert_tasks(tasks)
            return task_ids
        except Exception as e:
            LOGGER.error(f"Failed to persist tasks locally: {e}")
            raise
    
    @retry(
        retry=retry_if_exception_type(RedisConnectionError),  # todo: Make this generic
        wait=wait_random_exponential(multiplier=1, min=1, max=60), 
        stop=stop_after_attempt(10), 
        reraise=True
    )
    def _attempt_flush(self, tasks):
        submitted_task_ids = self.task_queue.enqueue_tasks(tasks)
        self.storage.update_tasks_status(submitted_task_ids)
        return len(tasks)

    def flush_tasks_to_queue(self, batch_size: int = 100) -> int:
        """ Flush tasks from persistent storage to the message queue"""
        tasks = self.storage.select_pending_tasks(batch_size)
        if not tasks:
            return 0
        try:
            return self._attempt_flush(tasks)
        except Exception as e:
            # If queueing fails, the task remains locally in the "PENDING" state 
            # and will be retried on the next execution of this method.
            LOGGER.error(f"Failed to flush tasks to queue. They remain 'PENDING' for next retry: {e}")
            raise e

    def clear_tasks(self, expire_time_seconds: int) -> None:
        self.storage.delete_old_tasks(expire_time_seconds=expire_time_seconds)
        self.storage.delete_completed_tasks()