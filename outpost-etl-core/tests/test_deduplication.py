from uuid import uuid4

from etl.deduplication import RedisDeduplicationCacheRepository


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
