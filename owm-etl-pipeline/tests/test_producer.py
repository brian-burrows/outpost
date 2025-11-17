import logging
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from owm.producer import TaskProducerManager

LOGGER = logging.getLogger('owm.producer')

@pytest.fixture
def mock_task_queue():
    """Mock for TaskQueueRepositoryInterface."""
    mock = MagicMock()
    mock.enqueue_tasks.return_value = [uuid4(), uuid4()]
    return mock

@pytest.fixture
def mock_storage():
    """Mock for TaskOutboxInterface."""
    mock = MagicMock()
    mock.select_pending_tasks.return_value = []
    return mock

@pytest.fixture
def manager(mock_task_queue, mock_storage):
    """Provides an instance of TaskProducerManager."""
    return TaskProducerManager(
        task_queue=mock_task_queue,
        storage=mock_storage
    )

def test_produce_batch_to_disk_success(manager, mock_storage, sample_task_factory):
    sample_tasks = sample_task_factory()
    task_ids = manager.produce_batch_to_disk(sample_tasks)
    assert len(task_ids) == 3
    assert all(isinstance(uid, UUID) for uid in task_ids)
    mock_storage.insert_tasks.assert_called_once_with(sample_tasks)

def test_produce_batch_to_disk_empty_list(manager, mock_storage):
    task_ids = manager.produce_batch_to_disk([])
    assert task_ids == []
    mock_storage.insert_tasks.assert_not_called()

def test_produce_batch_to_disk_storage_failure(manager, mock_storage, sample_task_factory):
    sample_tasks = sample_task_factory()
    mock_storage.insert_tasks.side_effect = Exception("DB error")
    
    with pytest.raises(Exception, match="DB error"):
        manager.produce_batch_to_disk(sample_tasks)
    assert mock_storage.insert_tasks.call_count == 2

def test_flush_tasks_to_queue_no_tasks(manager, mock_storage, mock_task_queue):
    mock_storage.select_pending_tasks.return_value = []
    count = manager.flush_tasks_to_queue()
    assert count == 0
    mock_storage.select_pending_tasks.assert_called_once_with(100)
    mock_task_queue.enqueue_tasks.assert_not_called()

def test_flush_tasks_to_queue_success(manager, mock_storage, mock_task_queue, sample_task_factory):
    sample_tasks = sample_task_factory()
    # Configure mocks to simulate a successful run
    submitted_ids = [t.task_id for t in sample_tasks]
    mock_storage.select_pending_tasks.return_value = sample_tasks
    mock_task_queue.enqueue_tasks.return_value = submitted_ids
    count = manager.flush_tasks_to_queue(batch_size=5)
    assert count == 3
    mock_storage.select_pending_tasks.assert_called_once_with(5)
    mock_task_queue.enqueue_tasks.assert_called_once_with(sample_tasks)
    mock_storage.update_tasks_status.assert_called_once_with(submitted_ids)

def test_flush_tasks_to_queue_queue_failure(manager, mock_storage, mock_task_queue, sample_task_factory, caplog):
    sample_tasks = sample_task_factory()
    mock_storage.select_pending_tasks.return_value = sample_tasks
    mock_task_queue.enqueue_tasks.side_effect = Exception("Queue network error")
    with caplog.at_level(logging.ERROR), pytest.raises(Exception):
        manager.flush_tasks_to_queue(batch_size=10)
    assert "Failed to flush tasks to queue." in caplog.text
    mock_storage.select_pending_tasks.assert_called_once_with(10)
    mock_task_queue.enqueue_tasks.assert_called_once_with(sample_tasks)
    mock_storage.update_tasks_status.assert_not_called()


def test_clear_tasks(manager, mock_storage):
    expire_time = 3600
    manager.clear_tasks(expire_time_seconds=expire_time)
    mock_storage.delete_old_tasks.assert_called_once_with(expire_time_seconds=expire_time)
    mock_storage.delete_completed_tasks.assert_called_once()

def test_produce_batch_to_disk_retries_and_fails(manager, mock_storage, sample_task_factory):
    sample_tasks = sample_task_factory()
    mock_storage.insert_tasks.side_effect = [
        Exception("DB error 1"), 
        Exception("DB error 2")
    ]
    with pytest.raises(Exception, match="DB error 2"):
        manager.produce_batch_to_disk(sample_tasks)
    assert mock_storage.insert_tasks.call_count == 2

def test_produce_batch_to_disk_recovers_on_retry(manager, mock_storage, sample_task_factory):
    sample_tasks = sample_task_factory()
    mock_storage.insert_tasks.side_effect = [
        Exception("Transient DB error"), 
        None
    ]
    task_ids = manager.produce_batch_to_disk(sample_tasks)
    # It should have called insert_tasks twice (failure + success)
    assert mock_storage.insert_tasks.call_count == 2
    assert len(task_ids) == 3

@patch('owm.producer.wait_random_exponential', return_value=MagicMock(side_effect=lambda *args, **kwargs: 0))
@patch('time.sleep', return_value=None) # <-- This is the key change!
def test_flush_retries_max_attempts_on_redis_error(
    mock_sleep,
    mock_wait_func,
    manager, 
    mock_storage, 
    mock_task_queue, 
    sample_task_factory, 
    caplog
):
    """Verifies flush_tasks_to_queue attempts the max number of times quickly."""
    sample_tasks = sample_task_factory()
    mock_storage.select_pending_tasks.return_value = sample_tasks
    mock_task_queue.enqueue_tasks.side_effect = [RedisConnectionError("Redis down")] * 10 
    with caplog.at_level(logging.ERROR), pytest.raises(Exception):
        manager.flush_tasks_to_queue(batch_size=10)
    assert "Failed to flush tasks to queue." in caplog.text
    # Assert that enqueue_tasks was called exactly 10 times (initial + 9 retries)
    assert mock_task_queue.enqueue_tasks.call_count == 10
    # Status must not be updated since all queue attempts failed
    mock_storage.update_tasks_status.assert_not_called()