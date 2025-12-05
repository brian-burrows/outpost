import json
import logging
from abc import ABC, abstractmethod
from typing import Type, TypeVar
from uuid import UUID

import redis
from pydantic import BaseModel, Field

from etl.tasks import BaseTask

TaskType = TypeVar("TaskType", bound=BaseTask)

LOGGER = logging.getLogger(__name__)

class QueuedTask(BaseModel):
    queued_task_id: str = Field(
        ..., 
        description="The unique message ID assigned by the queue (e.g., Redis Stream ID)."
    )
    time_since_queued_seconds: float = Field(
        ...,
        description="The total time, in seconds, elapsed since the task was originally added to the queue."
    )
    time_since_last_delivered: float = Field(
        ...,
        description="The time, in seconds, elapsed since this task was last claimed by a worker (idle time)."
    )
    number_of_times_delivered: int = Field(
        ...,
        description="The total count of how many times this task has been claimed by a worker."
    )
    task: TaskType = Field(
        ...,
        description="The encapsulated task payload data."
    )


class TaskQueueRepositoryInterface(ABC):
    @abstractmethod
    def dequeue_tasks(self) -> list[QueuedTask]:
        """Fetches new tasks from the queue.

        Returns
        -------
        list[QueuedTask]
            A list of queued tasks
        
        """
        pass
    
    @abstractmethod
    def enqueue_tasks(self, task: list[TaskType]) -> list[QueuedTask]:
        """Enqueue a list of tasks to the message queue.
        
        Parameters
        ----------
        A list of `TaskType` instances to enqueue to the message queue.

        Returns
        -------
        A list of QueuedTask instances containing the task and associated
        queue metadata.

        """
        pass

    @abstractmethod
    def recover_claimed_tasks(self) -> list[QueuedTask]:
        """Scans for tasks that have been claimed by a worker, but not acknowledged.

        Provides a way for one worker to collaborate with another by reclaiming tasks
        if the worker dies. It is up to the application code to decide on how to handle
        in-progress tasks.

        Returns
        -------
        list[QueuedTask]
            A list of queued tasks that have been claimed by a worker, but not acknowledged
            
        """
        pass

    @abstractmethod
    def acknowledge_tasks(self, tasks: list[QueuedTask]) -> int:
        """Confirms processing of tasks, removing them from the pending list.
        
        Parameters
        ----------
        tasks : list[QueuedTask]
            A list of `QueuedTask` instances to acknowledge as completed. Tasks
            should be removed from the queue that they were claimed from.

        Returns
        -------
        int 
            The number of items successfully acknowledged in the task queue.

        """
        pass


class RedisTaskQueueRepository(TaskQueueRepositoryInterface):
    def __init__(
        self,
        client: redis.StrictRedis,
        stream_key: str,
        task_model: Type[TaskType],
        group_name: str | None = None,
        consumer_name: str | None = None,
        max_stream_length: int | None = None
    ):
        """A concrete implementation of TaskQueueRepositoryInterface using Redis.
        
        client : StrictRedis
            A Redis client instance that contains the target stream key.
        stream_key : str 
            The Redis Stream key that will store the list of tasks.
        task_model : TaskType
            The expected `TaskType` model that will be stored in the Redis Stream.
            It will be stored as a serialized JSON string under the key b'data'.
        group_name : str | None
            The name of the Redis Consumer Group that this worker belongs to.
            If the consumer group does not exist, it will be created upon
            instantiation of this class. Messages that exist in the Redis Stream
            prior to the creation of the Consumer Group will never be processed.
        consumer_name : str | None 
            The name of the consumer within the specified Consumer Group.
        max_stream_length : int | None
            The maximum length of the Redis stream before entries will be truncated.

        """
        LOGGER.info(f"Connected to client {client.info}")
        self.redis_connection = client
        self.stream_key = stream_key 
        self.task_model = task_model 
        self.group_name = group_name 
        self.consumer_name = consumer_name
        self.max_stream_length = max_stream_length
        if group_name and consumer_name:
            self._ensure_consumer_group_exists()

    def _ensure_consumer_group_exists(self):
        try:
            # Create group starting at '$' (new messages only), mkstream=True creates stream if missing
            self.redis_connection.xgroup_create(
                name=self.stream_key, 
                groupname=self.group_name, 
                id='$',  # Enforces the docstring that messages enqueued prior to `xgroup_create` are never fetched
                mkstream=True
            )
            LOGGER.info(f"Consumer group '{self.group_name}' created for stream '{self.stream_key}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                LOGGER.debug(f"Consumer group '{self.group_name}' already exists")
            else:
                LOGGER.error(f"Error creating consumer group: {e}")
                raise

    def _deserialize_response(self, response: list[tuple[bytes, list[tuple[bytes, bytes]]]]) -> list[tuple[str, TaskType]]:
        """Deserialize a response object from Redis.

        Tasks are assumed to be produced by a producer in the following manner:
            Key: b'data'
            Value: byte-encoded JSON string using utf-8

        Where the Serialized JSON string must have been generated using `Pydantic.BaseModel.model_dumps`
        associated with the `task_type` class used to instantiate the `RedisStreamConsumer`.

        The message ID must be a Redis-generated timestamp.

        Parameters
        ----------
        response : list[tuple[bytes, list[tuple[bytes, bytes]]]
            Redis Stream responses are provided as:
                `[[stream_key, [[message_id, fields], [message_id, fields], ...]]]`
            where `message_id` and `fields` are byte-encoded strings. In this class,
            `fields` are byte-encoded JSON used to instantiate a `TaskType` instance.
            
        Returns
        -------
        List[Tuple[str, TaskType]]
            Returns a list of messages. Each message has a string-represented timestamp (message_id)
            which was auto-generated by Redis at the time the message was produced. The first
            entry in the tuple is this message ID. The second entry is a Pydantic model
            instantiated with the data contained in the message.

        """
        tasks_with_ids = []
        if response and response[0][1]:
            for message_id, fields in response[0][1]:
                LOGGER.debug(f"Processing message ID {message_id} with fields {fields}")
                task_data = json.loads(fields.get(b'data', b'{}'))
                LOGGER.debug(f"Extracted {task_data} from message ID {message_id}")
                if task_data:
                    task = self.task_model(**task_data) 
                    tasks_with_ids.append((message_id.decode('utf-8'), task))
                else:
                    LOGGER.critical(f"Unable to process the data from message ID {message_id}")
        return tasks_with_ids
    
    def _serialize_task(self, task: TaskType) -> tuple[str, bytes]:
        return (task.task_id, task.model_dump_json())
    
    def enqueue_tasks(self, tasks : list[TaskType]) -> list[UUID]:
        """A method to enqueue tasks into the Redis Stream.

        Messages are enqueued using a Pipeline to minimize networking.
        
        Parameters
        ----------
        tasks : list[TaskType]
            A list of tasks to enqueue.

        Returns
        -------
        list[UUID] : A list of task IDs that were successfully submitted to the queue.

        """
        LOGGER.debug(f"Attempting to enqueue {len(tasks)} tasks to queue {self.stream_key}")
        tasks = list(map(self._serialize_task, tasks))
        LOGGER.debug("Successfully serialized tasks")
        submitted_task_ids = []
        pipe = self.redis_connection.pipeline()
        for task_id, task_bytes in tasks:
            pipe.xadd(
                name=self.stream_key,
                fields={"data": task_bytes},
                maxlen=self.max_stream_length
            )
            submitted_task_ids.append(task_id)
        response = pipe.execute()
        LOGGER.debug(f"Successfully executed pipeline to send {len(tasks)} to Redis {self.stream_key}")
        LOGGER.debug(f"Received {response} from Redis after submitting tasks")
        return submitted_task_ids
    
    def dequeue_tasks(self, count: int, block_ms: int | None = None) -> list[tuple[str, TaskType]]:
        """Fetches only new tasks (for the specified consumer group) from the queue.
        
        Parameters
        ----------
        count : int 
            The maximum number of tasks to fetch from the Redis Stream for this consumer group.
        block_ms : int
            The maximum number of milliseconds to block the thread waiting on new tasks.

        Returns
        -------
        list[tuple[str, TaskType]]]
            A list of (msg_id, task) tuples as fetched from the Redis Stream.
            Tasks will have been claimed by this worker, and moved to the 
            Pending Entries List in the Redis Stream.

        """
        LOGGER.info(f"Attempting to fetch {count} tasks from {self.stream_key}")
        response = self.redis_connection.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_key: '>'}, # > specifies new tasks only for the consumer group
            count=count,
            block=block_ms,
        )
        LOGGER.debug(f"Received {response} after trying to dequeue {count} tasks")
        tasks = self._deserialize_response(response)
        LOGGER.info(f"Found and claimed {len(tasks)} tasks from {self.stream_key}")
        return tasks

    def acknowledge_tasks(self, message_ids: list[str]) -> int:
        """Confirms processing of tasks, removing them from the pending entries list.
        
        Parameters
        ----------
        message_ids : list[str]
            A list of message IDs, auto-generated from Redis, that identify tasks in the
            message queues.

        """
        if message_ids:
            LOGGER.info(f"Acknowledging {len(message_ids)} messages from {self.stream_key}")
            return self.redis_connection.xack(self.stream_key, self.group_name, *message_ids)  
        return 0

    def recover_stuck_tasks(self) -> tuple[
        list[tuple[str, TaskType]], 
        list[tuple[str, TaskType]]
    ]:
        """Fetch items from the Pending Entries list and classify them based on Redis metadata.

        Fetches identifiers from the Pending Entries List, and then subsequently pulls the task
        from the Redis Stream using available metadata. Classifies messages as stuck, poison pills,
        expired, or in progress.

        Parameters
        ----------
        max_idle_ms : int 
            The maximum amount of time a worker can check out a task before another worker flags it as
            stuck and subsequently reclaims it.
        expiry_time_ms : int
            The maximum amount of time after message creation that a task can exist before it is flagged
            as expired. Handling (and acknowledging) the expired task must be done by the calling process.
        max_retries : int 
            The maximum number of times a message can be reclaimed from the PEL before it is flagged as 
            a poison pill message. Handling (and acknowledging) the expired task must be done by the calling 
            process.
        batch_size : int
            The maximum number of messages that can be fetched and classified in a single pass.
            Currently non-functional.

        """
        # TODO: We need some state to track pagination properly, so that batch reads are done correctly
        LOGGER.info(f"Attempting to fetch {batch_size} tasks from {self.stream_key} PEL")
        pel_entries = self.redis_connection.xpending_range(
            self.stream_key,
            self.group_name,
            min='-', # -, + will fetch ALL items from the pending entries list
            max='+',
            count=batch_size,    
        )
        message_ids_to_dlq: list[str] = []
        message_ids_to_claim: list[str] = []
        for entry in pel_entries: 
            LOGGER.debug(f"Extracted entry from PEL: {entry}")
            message_id = entry['message_id'].decode('utf-8') 
            delivery_count = entry['times_delivered']
            idle_time_ms = entry['time_since_delivered']
            if idle_time_ms > expiry_time_ms:
                # If a message is too old, always retire it OR 
                LOGGER.info(f"Flagging {entry} for DLQ because {idle_time_ms} > {expiry_time_ms}")
                message_ids_to_dlq.append(message_id)
            elif delivery_count > max_retries:
                # If a message has been retried too many times, retire it
                LOGGER.info(f"Flagging {entry} for DLQ because of retry limit")
                message_ids_to_dlq.append(message_id)
            elif idle_time_ms > max_idle_ms:
                # If a message is stale, but still relevant, retry it
                LOGGER.info(f"Claiming stale message {entry}, will attempt a retry")
                message_ids_to_claim.append(message_id)
        dlq_tasks_data = []
        if message_ids_to_dlq:
            pipe = self.redis_connection.pipeline()
            for message_id in message_ids_to_dlq:
                pipe.xrange(name=self.stream_key, min=message_id, max=message_id, count=1)
            dlq_message_batches = pipe.execute()
            dlq_messages = []
            for message_list in dlq_message_batches:
                if message_list:
                    dlq_messages.append(message_list[0])
            dlq_response_to_process = [(self.stream_key.encode(), dlq_messages)]
            dlq_tasks_data = self._deserialize_response(dlq_response_to_process)
            LOGGER.info(f"Fetched {len(dlq_tasks_data)} DLQ messages.")
        claimed_tasks_data = []
        if message_ids_to_claim:
            claimed_messages = self.redis_connection.xclaim(
                name=self.stream_key,
                groupname=self.group_name,
                consumername=self.consumer_name,
                min_idle_time=max_idle_ms,
                message_ids=message_ids_to_claim,
            )
            tasks_response_to_process = [(self.stream_key.encode(), claimed_messages)]
            claimed_tasks_data = self._deserialize_response(tasks_response_to_process)
            LOGGER.info(f"Claimed {len(claimed_tasks_data)} stuck messages.")
        return claimed_tasks_data, dlq_tasks_data
