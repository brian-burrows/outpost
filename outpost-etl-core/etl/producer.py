import logging
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

    @retry( # TODO: make retry logic configurable following example in consumer.py
        wait=wait_random_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(2),
        reraise=True
    )
    def produce_batch_to_disk(self, tasks: list[TaskType]) -> list[UUID]:
        """Writes a list of `TaskType` instances to the underlying storage provided.

        Parameters
        ----------
        tasks : list[TaskType]
            A list of tasks to write to disk.
        
        Returns
        -------
        A list of task UUIDs (task.task_ids) that were successfully written to the data storage.

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
        retry=retry_if_exception_type(RedisConnectionError),
        wait=wait_random_exponential(multiplier=1, min=1, max=60), 
        stop=stop_after_attempt(10), 
        reraise=True
    )
    def _attempt_flush(self, tasks: list[TaskType]) -> int:
        """Attempts to enqueue tasks to the task queue, and then remove them from persistant storage.
        
        Parameters
        ----------
        tasks : list[TaskType]
            A list of tasks to send to the target task queue, and subsequently remove from storage.

        Returns
        -------
        int
            The number of tasks sent to the message queue. Assumes the `task_queue` handling 
            and storage deletion are all or nothing operations (independently).

        """
        submitted_task_ids = self.task_queue.enqueue_tasks(tasks)
        self.storage.update_tasks_status(submitted_task_ids)
        return len(tasks)

    def flush_tasks_to_queue(self, batch_size: int = 100) -> int:
        """Flush tasks from persistent storage to the message queue.
        
        Parameters
        ----------
        batch_size : int 
            The maximum number of tasks to pull from storage.

        Returns
        -------
        int
            The number of tasks flushed to the queue.

        """
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
        """Remove completed and expired tasks from underlying storage.
        
        Parameters
        ----------
        expire_time_seconds : int 
            The minimum age (in seconds) of a completed task before it is eligible for deletion.

        """
        self.storage.delete_old_tasks(expire_time_seconds=expire_time_seconds)
        self.storage.delete_completed_tasks()