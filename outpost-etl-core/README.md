# Outpost ETL Core: Interfaces and Concrete Implementations

The **Outpost ETL Core** package relies on abstract interfaces (contracts) and concrete classes (implementations) to allow users to compose specific **delivery semantics** (e.g., At-Least-Once, Exactly-Once).

The entire system architecture is built on **Dependency Injection**, ensuring the core orchestrator logic is isolated from infrastructure choices.

---

## I. Interfaces (The Contracts)

These abstract base classes (ABCs) define the required behavior for pipeline components.
Users can implement these interfaces to integrate any external service (e.g., Kafka, PostgreSQL, or a custom internal queue).

### A. Persistence and State Interfaces

| Interface                         | Purpose                                                                                                              | Essential Methods                                                                               |
| :-------------------------------- | :------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------- |
| **`TaskOutboxInterface`**         | Defines persistent storage for tasks waiting to be queued. It is the core contract for the **Transactional Outbox**. | `insert_tasks()`, `select_pending_tasks()`, `update_tasks_status()`, `delete_completed_tasks()` |
| **`DeduplicationCacheInterface`** | Defines the contract for deduplicating tasks during processing.                                                      | `is_processed(task_id)`, `record_processed(task_id)`                                            |

### B. Messaging and Control Interfaces

| Interface                          | Purpose                                                                                                      | Essential Methods                                                                    |
| :--------------------------------- | :----------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------- |
| **`TaskQueueRepositoryInterface`** | Defines message broker operations, spanning both production and consumption.                                 | `enqueue_tasks()`, `dequeue_tasks()`, `acknowledge_tasks()`, `recover_stuck_tasks()` |
| **`RateLimiterInterface`**         | Defines a contract for throttling execution to protect downstream services or adhere to external API limits. | `allow_request()`                                                                    |

---

## II. Concrete Classes (The Implementations)

These are the functional classes provided by the package,
categorized by their role in the pipeline: DAOs (storage backends) or Orchestrators (logic controllers).

### A. DAO Implementations (Provided Backends)

These classes implement the interfaces using common infrastructure and are ready to be used out-of-the-box.

| Interface Implemented          | Concrete Class                          | Technology / Role                                                                                                                            |
| :----------------------------- | :-------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------- |
| `TaskOutboxInterface`          | **`SqliteTaskOutbox`**                  | A default, thread-safe implementation using **SQLite** for a local Outbox store.                                                             |
| `TaskQueueRepositoryInterface` | **`RedisTaskQueueRepository`**          | Implements the queue logic using **Redis Streams** and Consumer Groups. Provides robust support for at-least-once delivery and PEL recovery. |
| `DeduplicationCacheInterface`  | **`RedisDeduplicationCacheRepository`** | Uses a **Redis** set structure to quickly store and check task IDs for idempotency checks.                                                   |
| `RateLimiterInterface`         | **`RedisDailyRateLimiterDao`**          | Implements fixed window (1 day) rate limiting using Redis to enforce limits.                                                                 |

### B. Orchestrator Classes (Core Logic Controllers)

These classes house the complex resilience logic and are configured by injecting the appropriate DAO instances.

| Class                       | Role in Pipeline                                                                                                                                                                                      | Key DAO Dependencies                                  |
| :-------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------- |
| **`TaskProducerManager`**   | **Producer Orchestrator:** Manages the transactional logic, ensuring tasks are persisted locally (`TaskOutboxInterface`) before being flushed to the external queue (`TaskQueueRepositoryInterface`). | `TaskOutboxInterface`, `TaskQueueRepositoryInterface` |
| **`TaskConsumer`**          | **Worker Wrapper:** Wraps the user's core business function, applying resilience layers such as retries, circuit breaking, and rate limiting based on injected dependencies.                          | `RateLimiterInterface` (optional)                     |
| **`TaskProcessingManager`** | **Consumer Orchestrator:** Manages the overall worker loop, handling task fetching, acknowledgment, and error recovery (like reclaiming stuck messages).                                              | `TaskQueueRepositoryInterface`, `TaskConsumer`        |
