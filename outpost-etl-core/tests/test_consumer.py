import logging
import time
from unittest.mock import MagicMock

import pybreaker
import pytest
from tenacity import Retrying, stop_after_attempt

from etl.consumer import TaskConsumer, TaskProcessingManager
from etl.exceptions import DuplicateTaskError, RateLimitExceededError
from etl.queue import RedisTaskQueueRepository, TaskQueueRepositoryInterface

logger = logging.getLogger(__name__)

@pytest.fixture
def mock_executable():
    """Mock function that the TaskConsumer will run."""
    mock = MagicMock()
    mock.return_value = "Task Processed"
    return mock

@pytest.fixture
def mock_consumer(mock_executable):
    """
    Mock TaskConsumer, but the manager needs a full instance to call its execute method.
    We'll use a real TaskConsumer initialized with a mock executable.
    """
    # Use a minimal Retrying policy for fast, deterministic tests
    fast_retrying = Retrying(stop=stop_after_attempt(1), reraise=True)
    consumer = TaskConsumer(
        executable=mock_executable, 
        retrying=fast_retrying
    )
    return consumer

@pytest.fixture
def mock_task_queue(test_task_model):
    """Mock for TaskQueueRepositoryInterface instance."""
    mock = MagicMock(spec=TaskQueueRepositoryInterface)
    mock.task_model = test_task_model
    mock.acknowledge_tasks = MagicMock()
    return mock

@pytest.fixture
def mock_dlq(test_task_model):
    """Mock for the Dead Letter Queue instance."""
    mock = MagicMock(spec=TaskQueueRepositoryInterface)
    mock.task_model = test_task_model
    mock.enqueue_tasks = MagicMock()
    return mock

@pytest.fixture
def manager_with_dlq(mock_consumer, mock_task_queue, mock_dlq):
    """Provides a manager instance with a configured DLQ."""
    manager = TaskProcessingManager(
        task_queue=mock_task_queue,
        task_consumer=mock_consumer,
        dead_letter_queue=mock_dlq,
    )
    manager._acknowledge_tasks = MagicMock()
    manager._send_tasks_to_dlq_and_acknowledge = MagicMock()
    return manager


def test_get_and_process_batch_flow_control(
        manager_with_dlq,
        sample_task_factory, 
        mock_consumer, 
        mock_task_queue
    ):
    """
    Tests that the TaskProcessingManager correctly delegates tasks and handles
    all possible flow-control exceptions (Success, Duplicate, RateLimit, Breaker, Unhandled/DLQ).
    """
    # GIVEN 4 tasks
    task_list = sample_task_factory(num_tasks=4)
    # Task queues return an internal ID for the tasks
    tasks_with_ids = [(f"msg-{i}", task_list[i]) for i in range(4)]
    # Configure the queue to return the tasks
    mock_task_queue.dequeue_tasks.return_value = tasks_with_ids
    manager_with_dlq.task_consumer.execute = MagicMock()
    # Configure the TaskConsumer to raise a specific sequence of exceptions
    # The manager catches these exceptions propagated by the consumer.
    mock_consumer.execute.side_effect = [
        DuplicateTaskError("Task is already done"), # 1. Acknowledge/Drop
        "Hello World",                              # 2. Success/Acknowledge
        ValueError("Unhandled Exception"),          # 3. DLQ/Drop
        RateLimitExceededError("Too many requests"),# 4. Re-queue
        pybreaker.CircuitBreakerError("Tripped"),   # 5. Re-queue (if a 5th task were processed)
    ]
    responses, failed_tasks = manager_with_dlq.get_and_process_batch(4)
    assert mock_consumer.execute.call_count == 4
    print(responses)
    assert responses == {task_list[1].task_id: "Hello World"}
    # Only the ValueError (msg-2) should be treated as failed/DLQ candidate
    assert len(failed_tasks) == 1
    assert failed_tasks[0] == task_list[2] # Task corresponding to msg-2
    # Expected: msg-0 (Duplicate) and msg-1 (Success)
    manager_with_dlq._acknowledge_tasks.assert_called_once_with(["msg-0", "msg-1"])
    # Expected: msg-2 (ValueError)
    manager_with_dlq._send_tasks_to_dlq_and_acknowledge.assert_called_once()
    dlq_call = manager_with_dlq._send_tasks_to_dlq_and_acknowledge.call_args[1]
    assert dlq_call['tasks_to_dlq'] == [task_list[2]]
    assert dlq_call['message_ids_to_acknowledge'] == ["msg-2"]
    # Expected: msg-3 (RateLimitExceededError) should NOT be acknowledged or DLQ'd.
    assert "msg-3" not in manager_with_dlq._acknowledge_tasks.call_args[0][0]
    assert "msg-3" not in dlq_call['message_ids_to_acknowledge']
    # Verification of internal calls to DLQ (sanity check)
    assert manager_with_dlq.dead_letter_queue.enqueue_tasks.call_count == 0 # We mocked _send_tasks...
    assert mock_task_queue.acknowledge_tasks.call_count == 0 # We mocked _acknowledge_tasks

def test_get_and_process_stuck_messages_discard_by_expiry(
    redis_client, 
    sample_task_factory, 
    test_task_model
):
    """
    Tests that tasks which have been idle longer than expiry_time_ms 
    are correctly flagged for DLQ (discarded) and acknowledged, 
    resulting in an empty PEL.
    """
    
    STREAM_KEY = "test_stuck_expiry_stream"
    GROUP_NAME = "TEST_GROUP_EXPIRE"
    CONSUMER_NAME = "TEST_CONSUMER_EXPIRE_1"
    TASK_COUNT = 10
    task_queue = RedisTaskQueueRepository(
        client=redis_client,
        stream_key=STREAM_KEY,
        task_model=test_task_model,
        group_name=GROUP_NAME,
        consumer_name=CONSUMER_NAME,
    )
    # Use a dummy consumer/executable since we are testing queue management, not execution
    def successful_func(task):
        pass

    consumer = TaskConsumer(executable=successful_func)
    manager = TaskProcessingManager(
        task_queue=task_queue,
        task_consumer=consumer,
    )
    # Create Stuck Messages in the PEL
    tasks = sample_task_factory(num_tasks=TASK_COUNT)
    task_queue.enqueue_tasks(tasks) # Add to stream
    dequeued_tasks = task_queue.dequeue_tasks(TASK_COUNT, block_ms=100) # Claim by user
    assert len(dequeued_tasks) == TASK_COUNT, "Tasks should be successfully dequeued"
    # We want the task's idle time to exceed the expiry time.
    idle_delay_seconds = 0.1 
    time.sleep(idle_delay_seconds) 
    # Configuration:
    # expiry_time_ms: 100ms (Much less than 500ms sleep, so all tasks should be DISCARDED)
    # max_idle_ms: High value (Ensures we hit expiry first, not just basic reclaim)
    # max_retries: 10 (Ensures we hit expiry before max retries, though expiry is primary trigger)
    max_idle_ms = 5000 
    expiry_time_ms = 100
    max_retries = 10
    batch_size = 10
    # Run the manager to process the stuck tasks
    retried_tasks, failed_tasks = manager.get_and_process_stuck_tasks(
        max_idle_ms=max_idle_ms, 
        expiry_time_ms=expiry_time_ms,
        max_retries=max_retries,
        batch_size=batch_size
    )
    print(f"TASKS RETRIED = {retried_tasks}")
    # A. Verify the tasks were correctly categorized by the manager
    # All 10 tasks should have been flagged for DLQ/discarded because idle_delay_seconds > expiry_time_ms
    assert len(retried_tasks) == 0, "No tasks should be retried (claimed) as they are expired."
    assert len(failed_tasks) == 0, "No tasks should have been retried (and failed) as they are expired."
    # B. Verify that the acknowledgement successfully cleared the PEL
    pel_info = redis_client.xpending(STREAM_KEY, GROUP_NAME)
    assert pel_info['pending'] == 0, "The Pending Entry List (PEL) should be empty after processing expired tasks."


def test_get_and_process_stuck_messages(
    redis_client, 
    sample_task_factory, 
    test_task_model
):
    """
    Tests that tasks which have been idle longer than expiry_time_ms 
    are correctly flagged for DLQ (discarded) and acknowledged, 
    resulting in an empty PEL.
    """
    STREAM_KEY = "test_stuck_retry_stream"
    GROUP_NAME = "TEST_GROUP_EXPIRE"
    CONSUMER_NAME = "TEST_CONSUMER_EXPIRE_1"
    TASK_COUNT = 10
    task_queue = RedisTaskQueueRepository(
        client=redis_client,
        stream_key=STREAM_KEY,
        task_model=test_task_model,
        group_name=GROUP_NAME,
        consumer_name=CONSUMER_NAME,
    )
    # Use a dummy consumer/executable since we are testing queue management, not execution
    def successful_func(task):
        pass

    consumer = TaskConsumer(executable=successful_func)
    manager = TaskProcessingManager(
        task_queue=task_queue,
        task_consumer=consumer,
    )
    # Create Stuck Messages in the PEL
    tasks = sample_task_factory(num_tasks=TASK_COUNT)
    task_queue.enqueue_tasks(tasks) # Add to stream
    dequeued_tasks = task_queue.dequeue_tasks(TASK_COUNT, block_ms=100) # Claim by user
    assert len(dequeued_tasks) == TASK_COUNT, "Tasks should be successfully dequeued"
    # We want the task's idle time to exceed the expiry time.
    idle_delay_seconds = 0.1
    time.sleep(idle_delay_seconds) 
    # Configuration:
    # we want a small idle time to trigger xclaim and retry
    max_idle_ms = 100
    expiry_time_ms = 50000
    max_retries = 10
    batch_size = 10
    # Run the manager to process the stuck tasks
    retried_tasks, failed_tasks = manager.get_and_process_stuck_tasks(
        max_idle_ms=max_idle_ms, 
        expiry_time_ms=expiry_time_ms,
        max_retries=max_retries,
        batch_size=batch_size
    )
    print(f"TASKS RETRIED = {retried_tasks}")
    # A. Verify the tasks were correctly categorized by the manager
    # All 10 tasks should have been flagged for DLQ/discarded because idle_delay_seconds > expiry_time_ms
    assert len(retried_tasks) == 10, "No tasks should be retried (claimed) as they are expired."
    assert len(failed_tasks) == 0, "No tasks should have been retried (and failed) as they are expired."
    # B. Verify that the acknowledgement successfully cleared the PEL
    pel_info = redis_client.xpending(STREAM_KEY, GROUP_NAME)
    assert pel_info['pending'] == 0, "The Pending Entry List (PEL) should be empty after processing expired tasks."



def test_get_and_process_stuck_messages_with_max_retries(
    redis_client, 
    sample_task_factory, 
    test_task_model
):
    """
    Tests that tasks which have been idle longer than expiry_time_ms 
    are correctly flagged for DLQ (discarded) and acknowledged, 
    resulting in an empty PEL.
    """
    STREAM_KEY = "test_stuck_retry_stream"
    GROUP_NAME = "TEST_GROUP_EXPIRE"
    CONSUMER_NAME = "TEST_CONSUMER_EXPIRE_1"
    TASK_COUNT = 10
    task_queue = RedisTaskQueueRepository(
        client=redis_client,
        stream_key=STREAM_KEY,
        task_model=test_task_model,
        group_name=GROUP_NAME,
        consumer_name=CONSUMER_NAME,
    )
    # Use a dummy consumer/executable since we are testing queue management, not execution
    def successful_func(task):
        pass

    consumer = TaskConsumer(executable=successful_func)
    manager = TaskProcessingManager(
        task_queue=task_queue,
        task_consumer=consumer,
    )
    # Create Stuck Messages in the PEL
    tasks = sample_task_factory(num_tasks=TASK_COUNT)
    task_queue.enqueue_tasks(tasks) # Add to stream
    dequeued_tasks = task_queue.dequeue_tasks(TASK_COUNT, block_ms=100) # Claim by user
    assert len(dequeued_tasks) == TASK_COUNT, "Tasks should be successfully dequeued"
    # We want the task's idle time to exceed the expiry time.
    idle_delay_seconds = 0.1
    time.sleep(idle_delay_seconds) 
    # Configuration:
    # we want a small idle time to trigger xclaim and retry
    max_idle_ms = 1
    expiry_time_ms = 50000
    max_retries = 1
    batch_size = 10
    # Run the manager to process the stuck tasks
    for i in range(3):
        manager.task_queue.recover_stuck_tasks(
            max_idle_ms=max_idle_ms, 
            expiry_time_ms=expiry_time_ms,
            max_retries=max_retries,
            batch_size=batch_size
        )
    retried_tasks, failed_tasks = manager.get_and_process_stuck_tasks(
        max_idle_ms=max_idle_ms, 
        expiry_time_ms=expiry_time_ms,
        max_retries=max_retries,
        batch_size=batch_size
    )
    print(f"TASKS RETRIED = {retried_tasks}")
    # A. Verify the tasks were correctly categorized by the manager
    # All 10 tasks should have been flagged for DLQ/discarded because idle_delay_seconds > expiry_time_ms
    assert len(retried_tasks) == 0, "No tasks should be retried (claimed) as they are expired."
    assert len(failed_tasks) == 0, "No tasks should have been retried (and failed) as they are expired."
    # B. Verify that the acknowledgement successfully cleared the PEL
    pel_info = redis_client.xpending(STREAM_KEY, GROUP_NAME)
    assert pel_info['pending'] == 0, "The Pending Entry List (PEL) should be empty after processing expired tasks."