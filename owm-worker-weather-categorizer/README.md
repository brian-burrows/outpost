# OWM Categorization Worker

This Python application functions as the **Transform** stage of the ETL pipeline. It consumes raw weather data (historical and forecast), applies a machine learning model to categorize the conditions (e.g., "high avalanche risk," "optimal hiking conditions"), and writes the final, classified results directly to the production database.

It is built as a highly resilient competing consumer using Redis Streams and various reliability patterns.

## Architecture and Responsibilities

The worker operates as a single-threaded process (the main thread) that manages all core responsibilities, including its own persistence and resilience checks.

- **Main Thread (Consumer & Watchdog):**

  - **Consume:** Continuously polls the upstream `owm-categorization-stream` using a Redis Consumer Group for new weather datasets.

  - **Transform:** Deserializes the data and passes it to the ML classification layer for categorization.

  - **Load:** Writes the final, categorized results to the Production Relational Database.

  - **Watchdog:** Regularly watches the upstream Redis Stream's Pending Entries List (PEL) to claim and reprocess tasks that have become stuck or timed out in a failed worker instance.

## Technology Stack & Resilience Patterns

| Feature                 | Technology/Pattern                       | Purpose                                                                                                                                                       |
| ----------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Messaging/Queueing**  | Redis Streams, `outpost-etl-core`        | Provides at-least-once delivery semantics and persistent queueing for task consumption.                                                                       |
| **Deduplication**       | Redis Set w/ TTL                         | Prevents the same task (identified by its UUID) from being processed and written multiple times, safeguarding the production database from duplicate entries. |
| **Fault Tolerance**     | `pybreaker` (Circuit Breakers)           | Protects the upstream Redis queue and the Dead Letter Queue (DLQ) from cascade failures by temporarily blocking calls after too many consecutive failures.    |
| **Retry Handling**      | `tenacity` (Exponential Backoff, Jitter) | Gracefully retries operations (like ML inference or DB writes) that fail due to transient errors, preventing overwhelming the dependency.                     |
| **Stuck Task Recovery** | Redis PEL Watchdog (Internal)            | Recovers tasks that were being processed when a worker failed, ensuring they are not lost and meet the at-least-once guarantee.                               |
| **Final Sink**          | Production RDBMS (Simulated)             | The authoritative final destination for all categorized weather data.                                                                                         |

## Safety Mechanisms

The worker is designed with robust safety mechanisms to ensure data integrity and system stability:

- **Connection Retry Logic:** The underlying Redis connection pool includes aggressive retry logic (`ExponentialWithJitterBackoff`) to handle transient network issues.

- **Dedicated Circuit Breakers:**

  - One breaker protects the **Upstream Queue** read operations.

  - A second breaker protects write operations to the **Dead Letter Queue (DLQ)**, preventing a DLQ failure from halting the entire consumption process.

- **Dead Letter Queue (DLQ):** Tasks that fail processing due to unhandled exceptions (e.g., internal ML model errors, data corruption) are immediately routed to the `dlq:owm-categorization-stream` for manual inspection and reprocessing.

- **At-Least-Once Delivery:** Ensured by using Redis Streams Consumer Groups and explicitly managing the Pending Entries List (PEL). A task is only acknowledged as processed upon successful completion and final sink write.

- **Deduplication Cache:** The `@with_deduplication_caching` decorator prevents the processing function from being executed if the task's UUID is already present in the cache, minimizing redundant ML inference and database writes.

## Configuration & Operational Limits

The worker's performance and resilience are tuned using the following key parameters, all sourced from environment variables or internal configuration constants:

| Component                   | Parameter                               | Value                                 | Purpose                                                                                        |
| --------------------------- | --------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Consumer (Main Loop)**    | $CONSUME\_COUNT$ / $CONSUME\_BLOCK\_MS$ | $100 \text{ tasks} / 1000 \text{ms}$  | Max tasks to read per poll / wait time for a blocking read.                                    |
| **PEL Watchdog (Internal)** | $CATEGORIZATION\_MAX\_IDLE\_MS$         | $600000 \text{ms}$ ($10 \text{ min}$) | Max idle time before a task is claimed as stuck and reprocessed.                               |
| **PEL Task Threshold**      | $CATEGORIZATION\_MAX\_RETRIES$          | 5                                     | Max number of times a task will be processed before it is sent to the DLQ.                     |
| **Circuit Breakers**        | Fail Max / Reset Timeout                | 10 failures / $60 \text{ seconds}$    | Tolerance before opening / duration before attempting a closed reset.                          |
| **Connection Pool**         | Max Connections                         | 2                                     | Small, dedicated pool for the single-threaded worker to minimize overhead.                     |
| **Stream Management**       | $CATEGORIZATION\_MAX\_STREAM\_SIZE$     | 200                                   | Hard limit on the upstream stream size (not strictly enforced by Redis but used for trimming). |

## Interface with External Services

### Upstream Queue

The worker consumes tasks from the Redis stream named `owm-categorization-stream`. These tasks are produced by the OWM Ingestion Worker and contain the aggregated weather data.

**Task Format (`WeatherCategorizationTask`):**

```python
task_id: UUID
city: str
state: str
historical_data: list[dict[str, Any]]  # Coarse-grained historical observations
forecast_data: list[dict[str, Any]]    # Hourly forecast data
```

### ML Categorization Service

Currently, this behavior is mocked.
Future plans exist to serve ML models from a distributed data store.

### Final Sink: Production Relational Database

Curretly, this behavior is not implemented.
Future plans exist to serve classifications to the production RDBMS system.
Upon successful categorization, the resulting data structure is expected to be:

```sql
TABLE classified_weather (
    task_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    city VARCHAR(100),
    state VARCHAR(100),
    weather_category VARCHAR(50) NOT NULL, -- e.g., 'OPTIMAL_HIKING', 'AVALANCHE_RISK'
    ml_confidence_score DECIMAL(5, 4) );
```

## Environment Variables (Required for Deployment)

The worker's runtime behavior and external connectivity are configured using the following environment variables:

| Variable                    | Default Value                 | Purpose                                                                                                |
| --------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------ |
| `CATEGORIZATION_QUEUE_HOST` | `redis-master`                | Hostname for the primary Redis instance (master) used for all streams and caches.                      |
| `WORKER_NAME`               | `categorization-worker-{pid}` | Unique identifier for this worker instance within the Redis Consumer Group (`categorization-workers`). |
| `ML_MODEL_STORE_URL`        | TODO                          | The network address (URL/path) where the classification model weights are loaded from.                 |
| `PROD_DB_CONNECTION_STRING` | TODO                          | The connection string for the authoritative Production Relational Database.                            |
