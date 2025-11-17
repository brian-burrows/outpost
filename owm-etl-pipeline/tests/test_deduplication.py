from uuid import uuid4

from owm.deduplication import RedisDeduplicationCacheRepository, with_deduplication_caching
from owm.exceptions import DuplicateTaskError
import pytest


def test_deduplication_cache_class(redis_client):
    dedup = RedisDeduplicationCacheRepository(
        client=redis_client,
        cache_key='deduplication-cache'
    )
    task_id = uuid4()
    assert dedup.is_processed(task_id) is False 
    dedup.record_processed(task_id)
    assert dedup.is_processed(task_id) is True 
    dedup.discard(task_id)
    assert dedup.is_processed(task_id) is False

def test_deduplication_cache_decorator(redis_client, sample_task_factory):
    sample_tasks = sample_task_factory()
    dedup = RedisDeduplicationCacheRepository(
        client = redis_client,
        cache_key="my-deduplication-cache"
    )
    @with_deduplication_caching(dedup)
    def my_func(task):
        pass
    for task in sample_tasks:
        task_id = task.task_id
        assert dedup.is_processed(task_id) is False 
        my_func(task)
        assert dedup.is_processed(task_id) is True 
        with pytest.raises(DuplicateTaskError): # It must raise an error
            my_func(task)
        dedup.discard(task_id)
        assert dedup.is_processed(task_id) is False
        assert my_func(task) is None # Make sure it no longer raises an error
