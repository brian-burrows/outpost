# ☁️ OWM Ingestion Producer (Cron Job)

This component is the first stage of an ETL pipeline to handle weather data,
operating as a periodic **cron job** responsible for generating and enqueuing tasks.
Its primary purpose is to ensure that every configured location is submitted to the ingestion queue reliably, triggering the data fetching process.

It uses a **Transactional Outbox Pattern** to guarantee **at-least-once delivery** of tasks to the queue,
even if the cron job fails during the submission phase.

---

## Architecture and Execution

The Producer runs as a single, one-off execution job, typically scheduled hourly or daily via a container orchestration system (like Kubernetes CronJob or a similar scheduler).

1.  **Generate**: The script iterates through a simulated location database (or a static list) and generates a batch of `OwmIngestionTask` objects.
2.  **Persist (Outbox)**: Each generated batch is immediately persisted locally to the SQLite database (`/tmp/owm_producer_tasks.db`), serving as the outbox.
3.  **Flush (Commit)**: Once all tasks have been successfully written to the local Outbox, they are submitted to the target **Redis Stream** (`owm-ingestion-stream`) in a large batch. Failure modes:
    - The producer can fail before the tasks are written to disk. The cron job will not repeat, and tasks will be missed.
    - The producer can fail after writing to disk, but before tasks are transferred to Redis. They will be resubmitted upon restart.
    - The producer can fail after tasks are transferred to Redis, but before they are deleted from the disk store. They will be resubmitted upon restart.
    - The producer can fail after the tasks are deleted from the disk store. Safe.
4.  **Exit**: The cron job exits successfully only after the flush operation is complete.

---

## Technology Stack & Resilience Patterns

| Feature                  | Technology/Pattern                   | Purpose                                                                                                                                               |
| :----------------------- | :----------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transactional Outbox** | SQLite Database (`SqliteTaskOutbox`) | Guarantees that task records are durably saved locally before being attempted in the remote Redis queue. Prevents task loss on mid-run failures.      |
| **Messaging/Queueing**   | Redis Streams (`outpost-etl-core`)   | The high-throughput, ordered stream used as the central ingestion queue for downstream workers.                                                       |
| **Retry Handling**       | Redis Connection Pool                | Uses **ExponentialWithJitterBackoff** for up to 8 retries on connection or loading errors, ensuring resilience against transient network instability. |
| **Task Model**           | `BaseTask` (`outpost-etl-core`)      | Provides Pydantic-based validation and serialization/deserialization for queue payloads.                                                              |

---

## Configuration & Operational Limits ⚙️

The cron job's performance is optimized for a single, fast burst submission using the following parameters:

| Component             | Parameter                   | Value                        | Purpose                                                                                                             |
| :-------------------- | :-------------------------- | :--------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **Redis Stream**      | $MAX\_STREAM\_SIZE$         | 100000                       | The maximum length of the ingestion stream before Redis starts trimming old entries.                                |
| **Producer Batching** | $TASK\_SUBMIT\_BATCH\_SIZE$ | 500                          | The maximum number of tasks submitted to the Redis queue in a single atomic batch during the final flush operation. |
| **Outbox Storage**    | Database Path               | `/tmp/owm_producer_tasks.db` | Local persistent storage for the Transactional Outbox.                                                              |
| **Connection Pool**   | Max Connections             | 2                            | Small, dedicated pool for the single-threaded cron job.                                                             |

---

## Interface

This application creates tasks that are formatted in the following manner:

## Future work

- Implement retry logic on writes, rather than just on initial connection.
- Implement circuit breaking on external interfaces
