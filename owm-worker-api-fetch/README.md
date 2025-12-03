# OWM Worker Weather Categorization

This Python application is a multithreaded worker designed for an Extract, Transform, Load (ETL) pipeline. Its primary functions are:

1.  **Consume** tasks (city coordinates) from an upstream Redis Stream.
2.  **Fetch** historical and forecast weather data (simulated from an external API).
3.  **Post** the processed data to an internal **Weather Service**.
4.  **Produce** a new task for categorization to a downstream Redis Stream using a **Transactional Outbox** pattern.

---

## Architecture

The application uses a **Main Thread** for primary task consumption and two background **Helper Threads** for system reliability and performance:

| Thread                 | Responsibility                                                                                                                                                                                  |
| :--------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Main Thread**        | Consumes upstream **OwmIngestionTask** tasks, executes weather data fetching and posting, generates the downstream **WeatherCategorizationTask**, and writes it to the local **SQLite Outbox**. |
| **Flusher Thread**     | Periodically reads tasks from the local **SQLite Outbox** and flushes them to the downstream Redis Stream.                                                                                      |
| **PEL Checker Thread** | Periodically monitors the upstream Redis Stream's **Pending Entries List (PEL)** to claim and reprocess tasks that appear stuck in other workers, ensuring **at-least-once-delivery**.          |

---

## Technology Stack & Safety Mechanisms

The worker is built with robust reliability features:

- **Task Queues:** **Redis Streams** are used for both upstream consumption (using **Consumer Groups** for competing consumers) and downstream production (using a transactional outbox).
- **Transactional Outbox:** The **SQLiteTaskOutbox** provides **at-least-once-delivery** assurance for downstream messages by persisting them to disk before submitting them to the downstream Redis Stream.
- **Circuit Breaking:** **`pybreaker`** protects against cascading failures by wrapping high-risk external dependencies (OWM API, Weather Service, Redis Queues).
- **Rate Limiting:** A Redis-backed **RedisDailyRateLimiterDao** enforces a daily request limit to the (simulated) OWM API.
- **Error Handling:** Failed tasks are sent to a **Dead Letter Queue (DLQ)**. Retries on transient errors are handled via **`tenacity`**.

---

## Configuration & Operational Limits

| Component              | Parameter                                              | Value                                                  | Purpose                                                              |
| :--------------------- | :----------------------------------------------------- | :----------------------------------------------------- | :------------------------------------------------------------------- |
| **Rate Limiter**       | $OWM\_MAX\_DAILY\_REQUESTS$                            | 500                                                    | Distributed limit to stay within the simulated API free-tier.        |
| **Consumer**           | $CONSUME\_COUNT$ / $CONSUME\_BLOCK\_MS$                | $100 \text{ tasks} / 1000 \text{ms}$                   | Max tasks to read per poll / wait time for a blocking read.          |
| **PEL Checker**        | $PEL\_CHECK\_FREQUENCY\_SECONDS$                       | $60 \text{ seconds}$                                   | Frequency to check the PEL for stuck tasks.                          |
| **PEL Task Threshold** | $INGESTION\_MAX\_IDLE\_MS$ / $INGESTION\_MAX\_RETRIES$ | $600000 \text{ms}$ ($10 \text{ min}$) / 5              | Max idle time or retries before a task is claimed as stuck.          |
| **Flusher**            | $REDIS\_SUBMIT\_FREQUENCY\_SECONDS$                    | $30 \text{ seconds}$                                   | Frequency to flush the SQLite Outbox to the downstream Redis Stream. |
| **Circuit Breakers**   | Fail Max / Reset Timeout                               | 10 failures / $600 \text{ seconds}$ ($10 \text{ min}$) | Tolerance before opening / duration before attempting a reset.       |
| **Outbox Storage**     | Database Path                                          | `/tmp/categorization_producer_tasks.db`                | Local persistent storage for the Transactional Outbox.               |

---

## Task Data Models & Interfaces

### Upstream Queue: OwmIngestionTask

The worker **consumes** tasks from the **`owm-ingestion-stream`** Redis Stream.

| Field           | Type  | Purpose                         |
| :-------------- | :---- | :------------------------------ |
| `task_id`       | UUID  | Unique identifier for the task. |
| `latitude_deg`  | float | Latitude of the location.       |
| `longitude_deg` | float | Longitude of the location.      |
| `city_id`       | int   | Unique city ID.                 |

### Third-Party API Interaction (Simulated)

The worker performs two primary (simulated) calls for a given location:

1.  **Historical Data:** Fetches _daily_ historical data for the **previous day**.
2.  **Forecast Data:** Fetches _hourly_ forecast data, typically covering the next **48 hours** from the current hour.

Both successful historical and forecast records are then **POSTed** to the internal **Weather Service** at the base URL: **`http://outpost-api-weather:8000`**.

### Downstream Queue: WeatherCategorizationTask

Upon successful processing and posting to the Weather Service, the worker **produces** a task to the **`owm-categorization-stream`** Redis Stream.

| Field                             | Type           | Purpose                                                    |
| :-------------------------------- | :------------- | :--------------------------------------------------------- |
| `task_id`                         | UUID           | Task identifier, inherited from the upstream task.         |
| `city_id`                         | int            | Unique city ID.                                            |
| `last_historical_timestamp`       | str (ISO 8601) | Date of the last historical data fetched.                  |
| `forecast_generated_at_timestamp` | str (ISO 8601) | Timestamp of when the forecast data was fetched/generated. |
