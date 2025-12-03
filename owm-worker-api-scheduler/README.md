# OWM Ingestion Producer (Cron Job)

This component is the **initial stage** of the weather data ETL pipeline, running as a single, one-off **cron job** to generate and enqueue tasks. It ensures that all required city locations are reliably submitted to the ingestion queue, triggering the data fetching process in downstream workers.

It uses a **Transactional Outbox Pattern** to guarantee **at-least-once delivery** of tasks to the queue.

---

## Architecture and Execution

The Producer runs as a single job with three core steps: **Extract**, **Persist**, and **Load**.

1.  **Extract (API Fetching)**:

    - Recursively fetches all city location data from the internal **Weather Service API** (`http://outpost-api-weather:8000`) using **pagination** (`/cities/?page={page}&limit={API_LIMIT}`).
    - This extraction process is protected by **Retry Logic** (5 attempts with exponential backoff) for transient HTTP errors and a **Circuit Breaker** to prevent overwhelming the API service.
    - For each fetched city, an **OwmIngestionTask** is created.

2.  **Persist (Transactional Outbox)**:

    - All generated tasks are written to the local **SQLite Outbox** (`/tmp/owm_producer_tasks.db`). This ensures durability and task retention in case of a crash before final submission.

3.  **Load (Flush/Commit)**:
    - Tasks are read from the Outbox and submitted to the target **Redis Stream** (`owm-ingestion-stream`) in large batches of **500** (`TASK_SUBMIT_BATCH_SIZE`).
    - The job exits successfully only after all tasks have been flushed from the Outbox to Redis and the local outbox records have been cleared.

---

## Resilience and Technology Stack

| Feature                  | Technology/Pattern                    | Purpose                                                                                                                                               |
| :----------------------- | :------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transactional Outbox** | SQLite Database (`SqliteTaskOutbox`)  | Guarantees task records are durably saved locally before being attempted in the remote Redis queue, preventing task loss on mid-run failures.         |
| **API Resilience**       | Circuit Breaker (`pybreaker`)         | Isolates the internal API service (`WEATHER_API_HOST`) if it experiences 20 failures within a 10-minute period, preventing a cascading failure.       |
| **API Retry Logic**      | `@retry` (`tenacity`)                 | Handles transient errors (connection issues, HTTP 4xx/5xx) during API calls with 5 attempts using exponential jitter backoff.                         |
| **Queueing**             | Redis Streams (`TaskProducerManager`) | The high-throughput, ordered stream used as the central ingestion queue for downstream workers.                                                       |
| **Redis Resilience**     | Connection Pool Retry                 | Uses **ExponentialWithJitterBackoff** for up to 8 retries on connection or loading errors, ensuring resilience against transient network instability. |

---

## Configuration & Operational Limits

| Component             | Parameter                   | Value                                                  | Purpose                                                                                                             |
| :-------------------- | :-------------------------- | :----------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **API Paging Limit**  | $API\_LIMIT$                | 100                                                    | The number of city records fetched per API request.                                                                 |
| **Redis Stream**      | $MAX\_STREAM\_SIZE$         | 100000                                                 | The maximum length of the ingestion stream before Redis starts trimming old entries.                                |
| **Producer Batching** | $TASK\_SUBMIT\_BATCH\_SIZE$ | 500                                                    | The maximum number of tasks submitted to the Redis queue in a single atomic batch during the final flush operation. |
| **Circuit Breaker**   | Fail Max / Reset Timeout    | 20 failures / $600 \text{ seconds}$ ($10 \text{ min}$) | Tolerance before opening / duration before attempting a reset.                                                      |
| **Outbox Storage**    | Database Path               | `/tmp/owm_producer_tasks.db`                           | Local persistent storage for the Transactional Outbox.                                                              |

---

## Interface: OwmIngestionTask

This application creates and enqueues tasks that are formatted as:

| Field           | Type  | Purpose                         |
| :-------------- | :---- | :------------------------------ |
| `task_id`       | UUID  | Unique identifier for the task. |
| `latitude_deg`  | float | Latitude of the location.       |
| `longitude_deg` | float | Longitude of the location.      |
| `city_id`       | int   | Unique city ID.                 |
