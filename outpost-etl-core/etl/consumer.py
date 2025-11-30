"""Contains core logic for defining Consumers of message queues.


"""
import logging
from abc import ABC, abstractmethod
from contextlib import nullcontext
from typing import Any, Callable

from pybreaker import CircuitBreaker, CircuitBreakerError
from tenacity import (
    Retrying,
    retry,
    retry_unless_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from etl.exceptions import DuplicateTaskError, RateLimitExceededError
from etl.limiter import DummyRateLimiter, RateLimiterInterface
from etl.queue import TaskQueueRepositoryInterface
from etl.tasks import TaskType

# Create a Retrying instance that does nothing, avoiding `None` handling
BASE_RETRYING = Retrying(
    retry=retry_unless_exception_type(
        (RateLimitExceededError, CircuitBreakerError, DuplicateTaskError)
    ),
    stop=stop_after_attempt(1), 
    reraise=True,
)


class TaskProcessingFunctionInterface(ABC):
    @abstractmethod
    def execute(self, task: TaskType) -> Any:
        """Processes a single `task` instance of type `TaskType`"""
        pass 

    @abstractmethod
    def execute_many(self, tasks: list[TaskType]) -> list[Any]:
        """Processes a list of `task` instances of type `TaskType`"""
        pass

class TaskConsumer(TaskProcessingFunctionInterface):
    def __init__(
        self, 
        executable: Callable[[TaskType], Any],
        rate_limiter: RateLimiterInterface = DummyRateLimiter(),
        circuit_breaker: CircuitBreaker | None = None,
        retrying: Retrying = BASE_RETRYING,
    ):
        """Encapsulates logic for safely processing tasks.
        
        Parameters
        ----------
        executable : Callable[[TaskType], Any]
            A function that processes a specific `TaskType` instance.
        rate_limiter : RateLimiterInterface
            A class instance that implementes the RateLimiterInterface.
        circuit_breaker: CircuitBreaker | None
            A class instance that implements the CircuitBreaker interface as defined
            by the 'pybreaker' package.
        retrying : Retrying
            A class instance that implements the Retrying interface as defined by 
            the 'tenacity' package. Reraise must be set to `True`, otherwise an
            exception will be raised.

        """
        if retrying.reraise is False:
            raise ValueError(
                "'TaskConsumer' class requires `reraise` property in `retrying` is set to `True`"
            )
        self.executable = executable
        self.rate_limiter = rate_limiter 
        self.circuit_breaker = circuit_breaker
        self.retrying = retrying

    def execute(self, task: TaskType) -> Any:
        """Evaluates the instances `executable` with instance-specific resource handling.
        
        This function will attempt to execute `self.executable` on the provided `task` instance.
        Upon failure, it will retry based on the specified retry policy. The following will be
        treated as individual failures (i.e., will trigger a retry, unless configured otherwise):
            - Rate limit exceeded, based on the supplied rate limiting policy.
            - Circuit breaker has tripped, based on the supplied circuit breaker.
            - Any exceptions raised by `executable`
        The retry behavior, including the specific exceptions that result in a retry, 
        is determined by the configured retrying policy.

        """
        breaker = nullcontext() if self.circuit_breaker is None else self.circuit_breaker.calling()
        for attempt in self.retrying:
            with attempt:
                if not self.rate_limiter.allow_request():
                    raise RateLimitExceededError()
                with breaker:
                    return self.executable(task)
    
    def execute_many(self, tasks: list[TaskType]) -> list[Any]:
        """Evaluates the instances `executable` with instance-specific resource handling.
        
        This function will attempt to execute `self.executable` on the provided `task` instances.
        Upon failure, it will retry based on the specified retry policy. The following will be
        treated as individual failures (i.e., will trigger a retry, unless configured otherwise):
            - Rate limit exceeded, based on the supplied rate limiting policy.
            - Circuit breaker has tripped, based on the supplied circuit breaker.
            - Any exceptions raised by `executable`
        The retry behavior, including the specific exceptions that result in a retry.
        If any task fails, the entire result will be discarded.

        """
        return [self.execute(task) for task in tasks]


LOGGER = logging.getLogger(__name__)

class TaskProcessingManagerInterface(ABC):
    """Provides an interface for message consumption from a message queue."""
    @abstractmethod
    def get_and_process_batch(self, batch_size: int, block_ms: int) -> tuple[
        dict[str, Any],
        list[TaskType]
    ]:
        """Provides blocking, batched message queue consumption.
        
        Parameters
        ----------
        batch_size : int 
            The maximum number of messages to deque in a single request.
        block_ms : int
            Number of milliseconds to block waiting for a batch of messages.
        
        Returns
        -------
        dict[str, Any] : 
            A Dictionary containing the results of successfully processed messages.
            The key is the task ID as provided by the processed task.
            The value is the result of the message processing pipeline.
        list[TaskType] :
            A list of messages that failed to be processed correctly.

        """
        pass

    @abstractmethod
    def get_and_process_stuck_tasks(
        self, 
        max_idle_ms: int, 
        expiry_time_ms: int, 
        max_retries: int, 
        batch_size: int
    ) -> tuple[
        dict[str, Any],
        list[TaskType]
    ]:
        """Provides blocking, batched, message queue recovery of stuck tasks.

        Parameters
        ----------
        max_idle_ms: int
            The minimum amount of time between claiming a message and labeling that message as stuck.
        expiry_time_ms: int
            The maximum amount of time between message creation and message processing, before
            a task is simply discarded.
        max_retries: int
            The maximum number of times a stuck message can be reclaimed before it is classified
            as a poison pill.
        batch_size: int
            The number of claimed messages to fetch from the task queue.

        Returns
        -------
        dict[str, Any] : 
            A Dictionary containing the results of successfully processed messages.
            The key is the task ID as provided by the processed Task.
            The value is the result of the message processing pipeline.
        list[TaskType] :
            A list of messages that failed to be processed correctly.

        Side Effect
        -----------
        Tasks that exceed the `expiry_time_ms` or `max_retries` are viewed as discarded, and do 
        not appear in the `returns` statement. It is up to the concrete implementation of this
        interface to decide on any side effects that occur.

        """
        pass

class TaskProcessingManager(TaskProcessingManagerInterface):
    """Manages task consumption and processing from a queue using a local target function.

    This class provides mechanisms to fetch tasks from an internal task queue,
    execute a given function on the tasks, and remove them from the task queue
    on success. It also provides an optional dead-letter-queue mechanism
    for fast-failure of tasks when unhandled exceptions occur, or a task has
    been retried too many times.

    Due to internal retry mechanisms to handle intermittant connection errors,
    it may be desirable to make the `target` function idempotent, have
    deduplication mechanisms, and handle rate limiting. This class handles
    two exceptions of the `target` function: `RateLimitExceededError` and
    `DuplicateTaskError`. 
    
    Upon a `RateLimitExceededError`, no task acknowledgement will be attempted,
    leaving the task in the `task_queue`. It is the responsibility of `target`
    to implement a 'Circuit Breaker' pattern to avoid accessive calls to the
    rate limited service. The function should raise a `CircuitBreakerError`
    whenever the circuit is broken.

    A `DuplicateTaskError` will remove the item from the queue, making the
    assumption that it has already been processed successfully.

    """
    def __init__(
        self, 
        task_queue: TaskQueueRepositoryInterface, 
        task_consumer: TaskConsumer,
        dead_letter_queue: TaskQueueRepositoryInterface | None = None,
        queue_breaker: CircuitBreaker | None = None,
        dlq_breaker: CircuitBreaker | None = None,
    ):
        self.task_queue = task_queue
        self.dead_letter_queue = dead_letter_queue
        self.task_consumer = task_consumer
        self.queue_breaker = queue_breaker
        self.dlq_breaker = dlq_breaker
        if dead_letter_queue:
            assert task_queue.task_model is dead_letter_queue.task_model
    
    @retry(
        wait = wait_random_exponential(multiplier=1, min=1, max=8),
        stop = stop_after_attempt(2),
        reraise=True
    )
    def _acknowledge_tasks(self, messages_to_ack: list[str]) -> None:
        """Acknowledge that tasks in `self.task_queue` are completed.
        
        Failure cascade:
            If acknowledgement fails too many times, it will circuit break. 
            Tasks remain in queue and will be retried.

        Parameters
        ----------
        messages_to_ack : list[str]
            A list of message IDs (specified by `self.task_queue`) to be acknowledged
            and removed from the task queue.

        """
        if messages_to_ack:
            breaker = nullcontext() if self.queue_breaker is None else self.queue_breaker.calling()
            with breaker:
                self.task_queue.acknowledge_tasks(messages_to_ack)
        
    @retry(
        wait = wait_random_exponential(multiplier=1, min=1, max=8),
        stop = stop_after_attempt(2),
        reraise=True
    )
    def _send_tasks_to_dlq_and_acknowledge(
        self, 
        tasks_to_dlq: list[TaskType],
        message_ids_to_acknowledge: list[str],
    ) -> None:
        """Acknowledge tasks after sending them to the DLQ.

        If no DLQ is configured, tasks will be simple acknowledged and dropped.
        
        Failure cascade:
        1. If DLQ fails too many times, it will circuit break. We'll never acknowledge the tasks. Safe.
        2. If acknowledgement fails too many times, it will circuit break.
            Duplicate entries may be sent to the DLQ.
            Tasks will remain in `task_queue` in some fashion, and may be reprocessed.

        Parameters
        ----------
        tasks_to_dlq : list[TaskType]
            A list of tasks to send to the dead letter queue.
        message_ids_to_acknowledge : list[str]
            The set of message IDs, associated with `tasks_to_dlq`, to acknowledge.

        """
        if len(tasks_to_dlq) != len(message_ids_to_acknowledge):
            msg = "Number of tasks to DLQ is not equal to the number of messages to acknowledge"
            LOGGER.fatal(msg)
            raise ValueError(msg)
        N = len(tasks_to_dlq)
        if tasks_to_dlq:
            breaker = nullcontext() if self.dlq_breaker is None else self.dlq_breaker.calling()
            with breaker:
                if self.dead_letter_queue:
                    self.dead_letter_queue.enqueue_tasks(tasks_to_dlq)
                    LOGGER.error(f"Sent {N} failed messages to DLQ")
                    self._acknowledge_tasks(message_ids_to_acknowledge)
                    LOGGER.info(f"Acknowledged {N} failed messaged")
                else:
                    LOGGER.info(f"{N} messaged failed, but no DLQ configured to send them to. Dropping them.")
                    self._acknowledge_tasks(message_ids_to_acknowledge)
                    LOGGER.info(f"Dropped {N} failed messaged")
                        

    def _process_tasks(self, tasks: list[tuple[str, TaskType]]) -> tuple[
        dict[str, Any],
        list[TaskType]
    ]:
        """Attempt to process tasks using `self.target`.
        
        Failure cascade:
            1. If `self.target` raises RateLimitExceededError, tasks will not be acknowledged.
               It is the responsibility of `target` to circuit break to avoid excessive retries.
            2. If `self.target` raises `DuplicateTaskError`, tasks acknowledgement will be attempted.

        Parameters
        ----------
        tasks : list[tuple[str, TaskType]]
            A list of tuples containing `(msg_id, TaskType)`, where `msg_id` is the internal message ID
            needed to acknowledge tasks in the selected `task_queue` for the corresponding task.
        
        Returns
        -------
        dict[str, Any] : 
            A list of successfully processed task results, keyed by `TaskType.task_id`.
        list[TaskType] : 
            A list of task instances that failed to process.

        Side Effects:
            - Successful tasks will be acknowledged in a batch.
            - Failed tasks will be acknowledged and sent to the DLQ in a batch.

        """
        successful_responses: dict[str, Any] = {}
        failed_tasks: list[TaskType] = []
        failed_message_ids: list[str] = []
        messages_to_ack: list[str] = []
        for msg_id, task in tasks:
            try:
                try:
                    response = self.task_consumer.execute(task)
                    successful_responses[task.task_id] = response
                    messages_to_ack.append(msg_id)
                    LOGGER.info(f"Task {msg_id} was successfully processed")
                except CircuitBreakerError:
                    LOGGER.debug(f"Task {msg_id} failed due to Circuit Breaker trip.")
            except RateLimitExceededError:
                LOGGER.error(
                    f"Task {msg_id} was not processed due to rate limits."
                )
                continue
            except DuplicateTaskError:
                LOGGER.info(f"Task {msg_id} is a duplicate task, and was not processed. Acknowledging.")
                messages_to_ack.append(msg_id)
                continue
            except Exception as e:
                failed_tasks.append(task)
                failed_message_ids.append(msg_id)
                LOGGER.error(f"Task {msg_id} failed with unhandled exception: {e}, sending to DLQ")
                continue
        self._acknowledge_tasks(messages_to_ack)
        self._send_tasks_to_dlq_and_acknowledge(
            tasks_to_dlq=failed_tasks,
            message_ids_to_acknowledge=failed_message_ids
        )
        return successful_responses, failed_tasks
    
    def get_and_process_batch(self, batch_size: int, block_ms: int = 5000) -> tuple[
        dict[str, Any],
        list[TaskType]
    ]:
        """Provides blocking, batched message queue consumption.
        
        Tasks will be dequeued from `self.task_queue`, and processed using the supplied
        Task Consumer instance. Failed tasks will be automatically sent to a DLQ and
        acknowledged. Successful tasks will be acknowledged.

        Parameters
        ----------
        batch_size : int 
            The maximum number of messages to deque in a single request.
        block_ms : int
            Number of milliseconds to block waiting for a batch of messages.

        Returns
        -------
        dict[str, Any] : 
            A Dictionary containing the results of successfully processed messages.
            The key is the task ID as provided by the processed task.
            The value is the result of the message processing pipeline.
        list[TaskType] :
            A list of messages that failed to be processed correctly.

        """
        return self._process_tasks(
            self.task_queue.dequeue_tasks(count=batch_size, block_ms = block_ms)
        )

    def get_and_process_stuck_tasks(self, max_idle_ms: int, expiry_time_ms: int, max_retries: int, batch_size: int) -> tuple[
        dict[str, Any],
        list[TaskType]
    ]:
        """Provides blocking, batched, message queue recovery of stuck tasks.

        Parameters
        ----------
        max_idle_ms: int
            The minimum amount of time between claiming a message and labeling that message as stuck.
        expiry_time_ms: int
            The maximum amount of time between message creation and message processing, before
            a task is simply discarded.
        max_retries: int
            The maximum number of times a stuck message can be reclaimed before it is classified
            as a poison pill.
        batch_size: int
            The number of claimed messages to fetch from the task queue.

        Returns
        -------
        dict[str, Any] : 
            A Dictionary containing the results of successfully processed messages.
            The key is the task ID as provided by the processed task.
            The value is the result of the message processing pipeline.
        list[TaskType] :
            A list of messages that failed to be processed correctly.

        Side Effect
        -----------
        Side effect: expired and poison pill messages are acknowledged and sent to a dead letter queue.

        """
        tasks_to_retry, tasks_to_discard = self.task_queue.recover_stuck_tasks(
            max_idle_ms=max_idle_ms, 
            expiry_time_ms=expiry_time_ms,
            max_retries=max_retries, 
            batch_size=batch_size
        )
        task_ids_to_discard = []
        _tasks_to_discard = []
        for task_id, task in tasks_to_discard:
            task_ids_to_discard.append(task_id)
            _tasks_to_discard.append(task)
        LOGGER.info(f"Discarding {task_ids_to_discard} by sending them to DLQ and acknoweldging them")
        self._send_tasks_to_dlq_and_acknowledge(tasks_to_dlq = _tasks_to_discard, message_ids_to_acknowledge=task_ids_to_discard)
        LOGGER.info(f"Retrying {tasks_to_retry}")
        return self._process_tasks(tasks_to_retry)