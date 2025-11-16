import sqlite3
import uuid

from owm.persistence import SqliteTaskOutbox  # Import the class being tested

def get_task_id_set(tasks: list) -> set[uuid.UUID]:
        return {t.task_id for t in tasks}

def test_create_table_exists(sqlite_outbox: SqliteTaskOutbox):
    """Verifies that the task table is created successfully upon initialization."""
    conn = None
    try:
        conn = sqlite3.connect(sqlite_outbox.db_connection)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{sqlite_outbox.table_name}'")
        assert cursor.fetchone() is not None, "Task table was not created."
    finally:
        if conn:
            conn.close()

def test_insert_and_select_pending_tasks(sqlite_outbox: SqliteTaskOutbox, sample_tasks: list):
    """Tests inserting tasks and then selecting them in the PENDING state."""
    sqlite_outbox.insert_tasks(sample_tasks)
    pending_tasks = sqlite_outbox.select_pending_tasks(limit=10)
    assert len(pending_tasks) == 3
    expected_ids = get_task_id_set(sample_tasks)
    retrieved_ids = get_task_id_set(pending_tasks)
    assert retrieved_ids == expected_ids

def test_select_pending_tasks_limit(sqlite_outbox: SqliteTaskOutbox, sample_tasks: list):
    """Tests the 'limit' functionality of select_pending_tasks."""
    sqlite_outbox.insert_tasks(sample_tasks)
    pending_tasks = sqlite_outbox.select_pending_tasks(limit=2)
    assert len(pending_tasks) == 2
    assert get_task_id_set(pending_tasks).issubset(set(get_task_id_set(sample_tasks)))
    

def test_update_tasks_status(sqlite_outbox: SqliteTaskOutbox, sample_tasks: list):
    """Tests updating the status of tasks."""
    sqlite_outbox.insert_tasks(sample_tasks)
    task_ids_to_update = [sample_tasks[0].task_id, sample_tasks[2].task_id]
    sqlite_outbox.update_tasks_status(task_ids_to_update)
    pending_tasks = sqlite_outbox.select_pending_tasks(limit=10)
    assert len(pending_tasks) == 1
    assert pending_tasks[0].task_id == sample_tasks[1].task_id

def test_delete_completed_tasks(sqlite_outbox: SqliteTaskOutbox, sample_tasks: list):
    """Tests removing tasks that are in the 'QUEUED' status."""
    sqlite_outbox.insert_tasks(sample_tasks)
    task_ids_to_update = [sample_tasks[0].task_id, sample_tasks[2].task_id]
    sqlite_outbox.update_tasks_status(task_ids_to_update)
    sqlite_outbox.delete_completed_tasks()
    pending_tasks = sqlite_outbox.select_pending_tasks(limit=10)
    assert len(pending_tasks) == 1
    conn = None
    try:
        conn = sqlite3.connect(sqlite_outbox.db_connection)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {sqlite_outbox.table_name}")
        total_rows = cursor.fetchone()[0]
        assert total_rows == 1
    finally:
        if conn:
            conn.close()


def test_delete_old_tasks(sqlite_outbox: SqliteTaskOutbox, sample_tasks: list, mock_time):
    """Tests deletion based on insertion timestamp and expiry time."""
    sqlite_outbox.insert_tasks(sample_tasks)
    new_time = 1700000010.0
    mock_time.return_value = new_time
    sqlite_outbox.delete_old_tasks(expire_time_seconds=20)
    tasks_left = sqlite_outbox.select_pending_tasks(limit=10)
    assert len(tasks_left) == 3 
    sqlite_outbox.delete_old_tasks(expire_time_seconds=5)
    tasks_left_after_delete = sqlite_outbox.select_pending_tasks(limit=10)
    assert len(tasks_left_after_delete) == 0