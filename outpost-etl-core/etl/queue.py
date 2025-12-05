import json
import logging
import time
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
        max_stream_length: int | None = None,
        dequeue_blocking_time_seconds: int = 1,
        dequeue_batch_size: int = 1,
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
        dequeue_blocking_time_seconds : int 
            The number of seconds to block the thread while waiting for new tasks.
        dequeue_batch_size : int 
            The number of messages to fetch from Redis in a single read.

        """
        LOGGER.info(f"Connected to client {client.info}")
        self.redis_connection = client
        self.stream_key = stream_key 
        self.task_model = task_model 
        self.group_name = group_name 
        self.consumer_name = consumer_name
        self.max_stream_length = max_stream_length
        self.dequeue_blocking_time_seconds = dequeue_blocking_time_seconds
        self.dequeue_batch_size = dequeue_batch_size
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

        Deserializes a raw Redis stream response (XREAD/XCLAIM/XPENDING) 
        into a list of (message_id, TaskType) tuples.

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
    
    def enqueue_tasks(self, tasks: list[TaskType]) -> list[QueuedTask]:
        """A method to enqueue tasks into the Redis Stream.

        Messages are enqueued using a Pipeline to minimize networking.
        
        Parameters
        ----------
        tasks : list[TaskType]
            A list of tasks to enqueue.

        Returns
        -------
        list[QueuedTask] :
            A list of `QueuedTask` instances representing the successfully submitted tasks.

        """
        LOGGER.debug(f"Attempting to enqueue {len(tasks)} tasks to queue {self.stream_key}")
        # Prepare data and track original tasks for zipping with Redis reseponse later
        tasks_to_submit = []
        original_tasks_map = {}
        for task in tasks:
            task_id, task_bytes = self._serialize_task(task)
            tasks_to_submit.append((task_id, task_bytes))
            original_tasks_map[task_id] = task
        pipe = self.redis_connection.pipeline()
        for task_id, task_bytes in tasks_to_submit:
            pipe.xadd(
                name=self.stream_key,
                fields={"data": task_bytes},
                maxlen=self.max_stream_length
            )
        # response is a list of Redis Stream IDs (strings) corresponding to each XADD command
        response = pipe.execute()
        LOGGER.info(f"Successfully executed pipeline to send {len(tasks)} to Redis {self.stream_key}")
        LOGGER.debug(f"Received Redis Stream IDs: {response}")
        queued_tasks = []
        # Redis Pipeline guarantees order matches between XADD order and Response order
        for idx, redis_message_id in enumerate(response):
            task_id, _ = tasks_to_submit[idx]
            original_task = original_tasks_map[task_id]
            # Since we just enqueued it, time metrics are zero/minimal
            queued_task = QueuedTask(
                queued_task_id=redis_message_id,
                time_since_queued_seconds=0.0, 
                time_since_last_delivered=0.0,
                number_of_times_delivered=0, # Hasn't been delivered yet
                task=original_task,
            )
            queued_tasks.append(queued_task)
        return queued_tasks
    
    def _get_redis_server_time_ms(self):
        # Clocks might not be synchorized across servers, so we need to fetch the Redis server time
        # in order to map the message ID timestamp to the message age.
        # We won't fall back to the local server time, since that might create unwanted an hard-to find
        # timing bugs. The Redis server must be the single source of truth for age calculations
        redis_time = self.redis_connection.time()
        # The TIME command returns the current server time as a two items lists: 
        # a Unix timestamp and the amount of microseconds already elapsed in the current second. 
        return (int(redis_time[0]) * 1000) + (int(redis_time[1]) // 1000)

    
    def dequeue_tasks(self) -> list[QueuedTask]:
        """Fetches only new tasks (for the specified consumer group) from the queue.
        
        Parameters
        ----------
        count : int 
            The maximum number of tasks to fetch from the Redis Stream for this consumer group.
        block_ms : int
            The maximum number of milliseconds to block the thread waiting on new tasks.

        Returns
        -------
        list[QueuedTask]
            A list of `QueuedTask` instances as fetched from the Redis Stream.
            Tasks will have been claimed by this worker, and moved to the 
            Pending Entries List in the Redis Stream.

        """
        count = self.dequeue_batch_size
        LOGGER.info(f"Attempting to fetch {count} tasks from {self.stream_key}")
        # We need to calculate ages from Redis message ID timestamps, but clocks won't be sync'd with 
        # this server's local clock. We cannot fetch the time and xreadgroup in a single atomic step
        # so this two stage approach get's us close.
        redis_server_time_ms = self._get_redis_server_time_ms()
        response = self.redis_connection.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_key: '>'}, # > specifies new tasks only for the consumer group
            count=count,
            block=self.dequeue_blocking_time_seconds * 1000, # Needs in milliseconds
        )
        deserialized_response = self._deserialize_response(response)
        queued_tasks = []
        LOGGER.info(f"Found and claimed {len(deserialized_response)} tasks from {self.stream_key}")
        for message_id, task_object in deserialized_response:
            time_queued_milliseconds = int(message_id.split("-")[0])
            if redis_server_time_ms >= time_queued_milliseconds:
                time_since_queued_seconds = (redis_server_time_ms - time_queued_milliseconds) / 1000.0
            else:
                LOGGER.warning(f"Found a task {message_id} from Redis that is newer than the current server time")
                time_since_queued_seconds = 0.0
            queued_task = QueuedTask(
                queued_task_id=message_id, 
                time_since_queued_seconds=time_since_queued_seconds,
                number_of_times_delivered=1,
                time_since_last_delivered=0.0,
                task=task_object,
            )
            queued_tasks.append(queued_task)
        return queued_tasks

    def acknowledge_tasks(self, tasks: list[QueuedTask]) -> int:
        """Confirms processing of tasks, removing them from the pending entries list.
        
        This method sends the XACK command to Redis to move the messages from the 
        consumer group's Pending Entry List (PEL) to the final acknowledged state.

        Parameters
        ----------
        tasks : list[QueuedTask]
            A list of `QueuedTask` instances whose processing is confirmed. The 
            `queued_task_id` from each object is used for acknowledgement.

        Returns
        -------
        int 
            The number of items successfully acknowledged in the task queue (returned by XACK).

        """
        message_ids = [t.queued_task_id for t in tasks]
        if message_ids:
            LOGGER.info(f"Acknowledging {len(message_ids)} messages from {self.stream_key}")
            # Use XACK to remove the messages from the PEL
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
