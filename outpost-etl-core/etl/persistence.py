import logging
import sqlite3
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, List, Type

from etl.tasks import UUID, TaskType

LOGGER = logging.getLogger(__name__)

def _adapt_uuid(val):
    return str(val)

sqlite3.register_adapter(uuid.UUID, _adapt_uuid)

def _get_current_time():
    return time.time()

class TaskOutboxInterface(ABC):
    """
    Interface for local, persistent storage to track task state.
    """
    def __init__(self, db_connection: Any, task_type: Type[TaskType]):
        self.db_connection = db_connection 
        self.task_type = task_type
        self.STATUS_COLUMN = 'status' 

    @abstractmethod 
    def create_task_table(self) -> None:
        """Creates the necessary database table if it doesn't exist."""
        pass

    @abstractmethod
    def insert_tasks(self, tasks: List[TaskType]) -> None:
        """Inserts tasks into local storage with an initial 'PENDING' state."""
        pass

    @abstractmethod
    def select_pending_tasks(self, limit: int) -> List[TaskType]:
        """Selects tasks ready for processing, i.e., in 'PENDING' state."""
        pass
    
    @abstractmethod
    def update_tasks_status(self, task_ids: List[UUID]) -> None:
        """Updates the status of tasks after a successful operation to 'QUEUED'."""
        pass
        
    @abstractmethod
    def delete_completed_tasks(self) -> None:
        """Removes tasks that have been fully processed and are no longer needed."""
        pass

    @abstractmethod 
    def delete_old_tasks(self, expire_time_seconds: int):
        """Removes tasks that have expired."""
        pass


class SqliteTaskOutbox(TaskOutboxInterface):
    DATA_COLUMN = "data"
    TIMESTAMP_COLUMN = "inserted_at"
    STATUS_COLUMN = "status"
    def __init__(self, db_path: str, task_type: Type[TaskType]):
        # TODO: Make this support pagination correctly
        super().__init__(db_connection=db_path, task_type=task_type)
        self.table_name = self.task_type.__name__
        self.lock = threading.Lock()
        self.create_task_table()

    def create_task_table(self) -> None:
        """
        Creates a simplified table with columns for ID, JSON data, Status, and Timestamp.
        """
        conn = None
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_connection)
                cursor = conn.cursor()
                sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        task_id TEXT PRIMARY KEY,
                        {self.DATA_COLUMN} TEXT NOT NULL,  -- 💡 Stores the model_dump_json string
                        {self.TIMESTAMP_COLUMN} REAL NOT NULL, -- 💡 Stores insertion time (seconds since epoch)
                        {self.STATUS_COLUMN} TEXT NOT NULL DEFAULT 'PENDING'
                    );
                """
                LOGGER.debug(f"Creating table with query {sql}")
                cursor.execute(sql)
                conn.commit()
                LOGGER.info(f"Ensured table '{self.table_name}' exists in SQLite.")
            except Exception as e:
                LOGGER.error(f"Error creating table {self.table_name}: {e}")
                raise
            finally:
                if conn:
                    conn.close()
    
    def insert_tasks(self, tasks: List[TaskType]) -> None:
        """Inserts tasks as JSON strings along with an insertion timestamp."""
        LOGGER.debug(f"Attempting to insert {len(tasks)} tasks into {self.table_name}")
        conn = None
        current_time = _get_current_time()
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_connection)
                cursor = conn.cursor()
                columns_to_insert = [
                    'task_id', 
                    self.DATA_COLUMN, 
                    self.TIMESTAMP_COLUMN
                ]
                columns_str = ", ".join(columns_to_insert)
                placeholders = ", ".join(["?"] * len(columns_to_insert))
                sql = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"
                rows_to_insert = [
                    (task.task_id, task.model_dump_json(), current_time)
                    for task in tasks
                ]   
                cursor.executemany(sql, rows_to_insert)
                conn.commit()
                LOGGER.debug(f"Inserted {len(tasks)} tasks into {self.table_name}.")
            except Exception as e:
                LOGGER.error(f"Error inserting tasks into {self.table_name}: {e}")
                raise
            finally:
                if conn:
                    conn.close()

    def select_pending_tasks(self, limit: int) -> List[TaskType]:
        """Selects tasks ready for processing and reconstructs the Pydantic model from JSON."""
        conn = None
        with self.lock:
            selected_tasks = []
            try:
                conn = sqlite3.connect(self.db_connection)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                # Select only the JSON data column and task_id for reconstruction
                sql = f"""
                    SELECT task_id, {self.DATA_COLUMN} 
                    FROM {self.table_name} 
                    WHERE {self.STATUS_COLUMN} = ? 
                    ORDER BY {self.TIMESTAMP_COLUMN} ASC, task_id ASC
                    LIMIT ?
                """
                LOGGER.debug(f"Selecting tasks with query {sql}")
                cursor.execute(sql, ("PENDING", limit))
                for row in cursor.fetchall():
                    json_data = row[self.DATA_COLUMN]
                    task_instance = self.task_type.model_validate_json(json_data) 
                    selected_tasks.append(task_instance)
                LOGGER.debug(f"Selected {len(selected_tasks)} tasks with status 'PENDING' from {self.table_name}.")
                return selected_tasks
            except Exception as e:
                LOGGER.error(f"Error selecting tasks from {self.table_name}: {e}")
                return []
            finally:
                if conn:
                    conn.close()

    def update_tasks_status(self, task_ids: List[UUID]) -> None:
        """Updates the status to 'QUEUED' for tasks after a successful operation."""
        conn = None
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_connection)
                cursor = conn.cursor()
                placeholders = ", ".join(["?"] * len(task_ids))
                sql = f"""
                    UPDATE {self.table_name} 
                    SET {self.STATUS_COLUMN} = ? 
                    WHERE task_id IN ({placeholders})
                """
                LOGGER.debug(f"Updating tasks with query {sql}")
                new_status = "QUEUED"
                params = (new_status,) + tuple(uid for uid in task_ids)
                cursor.execute(sql, params)
                conn.commit()
                LOGGER.debug(f"Updated {cursor.rowcount} tasks to status '{new_status}' in {self.table_name}.")
            except Exception as e:
                LOGGER.error(f"Error updating task status in {self.table_name}: {e}")
                raise
            finally:
                if conn:
                    conn.close()
    
    def delete_completed_tasks(self) -> None:
        """Removes tasks that have been fully processed and are no longer needed."""
        LOGGER.debug(f"Deleting all completed tasks from {self.table_name}")
        conn = None
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_connection)
                cursor = conn.cursor()
                sql = f"DELETE FROM {self.table_name} WHERE status = 'QUEUED'"
                LOGGER.debug(f"Deleting tasks with query {sql}")
                cursor.execute(sql)
                conn.commit()
                LOGGER.debug(f"Deleted {cursor.rowcount} tasks from {self.table_name}.")
            except Exception as e:
                LOGGER.error(f"Error deleting completed tasks from {self.table_name}: {e}")
                raise
            finally:
                if conn:
                    conn.close()

    def delete_old_tasks(self, expire_time_seconds: int):
        LOGGER.info(f"Deleting tasks older than {expire_time_seconds}")
        conn = None
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_connection)
                cursor = conn.cursor()
                sql = f"DELETE FROM {self.table_name} WHERE {self.TIMESTAMP_COLUMN} < ?"
                LOGGER.debug(f"Deleting tasks with query {sql}")
                cutoff_time = _get_current_time() - expire_time_seconds
                cursor.execute(sql, (cutoff_time, ))
                conn.commit()
                LOGGER.debug(f"Deleted {cursor.rowcount} tasks from {self.table_name}.")
            except Exception as e:
                LOGGER.error(f"Error deleting completed tasks from {self.table_name}: {e}")
                raise
            finally:
                if conn:
                    conn.close()