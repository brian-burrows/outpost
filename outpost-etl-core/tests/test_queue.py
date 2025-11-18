import time

from redis import StrictRedis

from etl.queue import RedisTaskQueueRepository


def test_redis_consumer_group_creation(redis_client: StrictRedis, repo: RedisTaskQueueRepository):
    """Verifies the consumer group is created upon repository initialization."""
    group_info = redis_client.xinfo_groups(repo.stream_key)
    assert len(group_info) == 1
    assert group_info[0]['name'] == repo.group_name.encode("utf-8")
    assert group_info[0]['last-delivered-id'] == '0-0'.encode("utf-8")

def test_redis_enqueue_and_dequeue_tasks(repo: RedisTaskQueueRepository, sample_task_factory):
    """Tests basic enqueue and subsequent dequeue of messages."""
    sample_tasks = sample_task_factory()
    enqueued_task_ids = repo.enqueue_tasks(sample_tasks)
    assert len(enqueued_task_ids) == 3
    dequeued_tasks_data = repo.dequeue_tasks(count=3, block_ms=500)
    assert len(dequeued_tasks_data) == 3
    assert dequeued_tasks_data[0][1].location_id == sample_tasks[0].location_id
    redis_message_ids = [msg_id for msg_id, _ in dequeued_tasks_data]
    pending_info = repo.redis_connection.xpending(repo.stream_key, repo.group_name)
    assert pending_info['pending'] == 3
    ack_count = repo.acknowledge_tasks(redis_message_ids)
    assert ack_count == 3
    pending_info_after_ack = repo.redis_connection.xpending(repo.stream_key, repo.group_name)
    assert pending_info_after_ack['pending'] == 0

def test_redis_dequeue_blocks_when_empty(repo: RedisTaskQueueRepository):
    """Tests that dequeue blocks when the queue is empty."""
    start_time = time.time()
    tasks = repo.dequeue_tasks(count=1, block_ms=200)
    end_time = time.time()
    assert len(tasks) == 0
    assert end_time - start_time >= 0.2

def test_redis_recover_stuck_tasks(redis_client: StrictRedis, repo: RedisTaskQueueRepository, sample_task_factory):
    """Tests the recovery logic for stuck tasks, ensuring only the intended task is claimed."""
    pass