    # OWM Worker Weather Categorization

    This Python application is a multithreaded worker designed to handle a two-step process:

    - fetching weather data from an external API (simulated as OWM)
    - queuing it for categorization.

    It's built as an Extract, Transform, Load (ETL) pipeline using Redis Streams for task queues and various resilience/reliability patterns.

    ## Architecture

    The application runs with a main thread and two helper background threads to handle the three main responsibilities:

    - Main Thread :
    - Consume upstream tasks using a competing cosumer model (Redis Stream Consumer Groups) and writes downstream tasks to disk.
    - Continuously polls for new tasks in the upstream queue and transforms the upstream event (weather data to fetch)
    - Helper Thread 1 :
    - Flush downstream tasks from disk to the downstream Redis queue.
    - Helper Thread 2 :
    - Watch the upstream Redis Stream's Pending Entries List (PEL) for stuck tasks to claim

    ## Technology stack

    - `Rate Limiting` is done using a custom, Redis backed rate limiter to enforce API restrictions.
    - `Circuit Breaking` is implemented through `pybreaker`
    - `Deduplication` is implemented through a Redis set with a time to live to manage cache size.
    - `Consuming` with at-least-once delivery is handled through Redis Streams with a pending entries list, controlled by the `outpost-etl-core` package.
    - `Producing` with a transactional outbox pattern is handled through Redis Streams, backed by an `sqlite` database. Handled by the `outpost-etl-core` package.

    ## Safety mechanisms

    - Circuit breakers exist on each high-risk external dependency: one protecting the third-party API, and a second protecting all Redis queue interfaces.
      - This design assumes a single, centralized Redis cluster for all queueing and caching needs.
      - If future architecture requires separate, per-microservice Redis instances, the queue breaker must be deployed and configured independently for each service's Redis connection.
    - Tasks that fail processing for unhandled errors are immediately sent to a dead letter queue.
    - Retry logic exists on each interface to handle transient errors: upstream queue, third party API, and downstream queue.
    - A deduplication cache checks the UUID of the upstream task, reducing (but not eliminating) the risk of reprocessing a task.
    - A distributed rate limiter counts the number of API requests per day, aiming to stay in the free tier. The max number of requests is well below the free tier limits, to ensure a buffer exists.
    - A transactional outbox pattern is used to produce downstream tasks, ensuring at-least-once delivery.
    - Stuck tasks are pulled from a pending entries list (PEL) in the Redis stream after a certain threshold has passed, ensuring at-least-once-delivery when a worker fails.
    - Batching data fetches to ensure upstream services are not blocked for long periods of time.

    ## Configuration & Operational Limits ⚙️

    The worker's performance and resilience are tuned using the following key parameters:

    | Component                          | Parameter                                              | Value                                                  | Purpose                                                      |
    | :--------------------------------- | :----------------------------------------------------- | :----------------------------------------------------- | :----------------------------------------------------------- |
    | **Rate Limiter**                   | $OWM\_MAX\_DAILY\_REQUESTS$                            | 500                                                    | Distributed limit to stay within API free-tier.              |
    | **Consumer (Main Thread)**         | $CONSUME\_COUNT$ / $CONSUME\_BLOCK\_MS$                | $100 \text{ tasks} / 1000 \text{ms}$                   | Max tasks to read per poll / wait time for a blocking read.  |
    | **PEL Watchdog (Helper Thread 2)** | $PEL\_CHECK\_FREQUENCY\_SECONDS$                       | $60 \text{ seconds}$                                   | How often to check for stuck tasks.                          |
    | **PEL Task Threshold**             | $INGESTION\_MAX\_IDLE\_MS$ / $INGESTION\_MAX\_RETRIES$ | $600000 \text{ms}$ ($10 \text{ min}$) / 5              | Max idle time or retries before a task is claimed as stuck.  |
    | **Flusher (Helper Thread 1)**      | $REDIS\_SUBMIT\_FREQUENCY\_SECONDS$                    | $30 \text{ seconds}$                                   | How often to flush the SQLite Outbox to Redis.               |
    | **Circuit Breakers**               | Fail Max / Reset Timeout                               | 10 failures / $600 \text{ seconds}$ ($10 \text{ min}$) | Tolerance before opening / duration before attempting reset. |
    | **Outbox Storage**                 | Database Path                                          | `/tmp/categorization_producer_tasks.db`                | Local persistent storage for the Transactional Outbox.       |

    ## Interface with external services

    ### Upstream queue

    This application consumes tasks that are fomatted in the following manner:

    ```python
        task_id: UUID
        latitude_deg: float
        longitude_deg: float
        city: str
        state: str
        forecast_duration_hours: int = 72
        historical_duration_hours: int = 72
    ```

    With the current structure being fetched from Redis as a binary string.

    ```
    {b'data': b'{"task_id":"111986bc-45b1-4b74-b6fd-cc215914a4f9","latitude_deg":40.65,"longitude_deg":-111.5,"city":"park-city","state":"utah","forecast_duration_hours":72,"historical_duration_hours":72}'}
    ```

    ### Third party API

    We've selected [Open Weather Maps](https://openweathermap.org/api/one-call-3) One Call API 3.0 to collect our weather data.

    #### Current Weather data

    Fetching hourly historical data is costly using openweathermap.
    Because this application simply classifies quality of conditions, we can use more coarse grained API calls (e.g., daily).

    `https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lon}&date={date}&appid={API key}`

    #### Weather Forecast

    `https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API key}`

    A single call to this endpoint returns a vast amount of data, which includes an hourly array covering the next 48 hours (2 days).
    Hourly Forecast: $\mathbf{48 \text{ hours}}$ (2 days) of hourly data.
    Daily Forecast: $8 \text{ days}$ of daily summaries.Current Weather: The current conditions.

    The exclude parameter can be used to drop unnecessary data: e.g., `minutely,daily,alerts`.

    #### Free tier limits

    The One Call API allows `1000 requests per day`, with each additional call costing `0.0015 USD`.
    Assuming:

    - Forecasts are fetched 4 times per day (6 hour increments)
    - Historical data is fetched 1 time per day (24 hour increments)

    We have 5 calls per location, allowing for 200 distinct locations per day in the free tier.

    However, we have at-least-once delivery semantics on messages.
    Although the deduplication cache should help reduce retried tasks, it is not guaranteed.
    Exactly-once-delivery semantics are too costly to implement for such a simple task.

    - Two-phase commit introduces significant complexity.
    - Requires additional networking that may cost more than the API service!

    Additionally, `1000` tasks may be enqueued in two subsequent days,
    but all `2000` could be processed on the same day due to latency / failures.
    There is no guarantee that tasks will be consumed on the day they are written.
    So, we have a daily rate limiting service that should help prevent exceeding the limit.

    We can hedge our bets by setting a buffer on the rate limit (e.g., set to `800`, when `1000` is the true limit).
    This allows us to support `160` distinct locations, and rate limiting will be set to `800` API calls per day.
    If the rate limiting and deduplication cache are down, API tasks should fail and be retried later.

    #### Terms and conditions

    ### Downstream queue

    Upon successful fetching, it creates a new tasks in a downstream queue in the following format

    ```python
        task_id: UUID
        city: str
        state: str
        historical_data: list[dict[str, Any]]
        forecast_data: list[dict[str, Any]]
    ```

    With the current structure being generated as a binary string in the form:

    ```
    {b'data': b'{"task_id":"f74b1981-ec39-4e41-959e-f040592e9918","city":"prescott","state":"arizona","historical_data":[{"date":"2024-04-01 00:00:00","rain_inches":1,"wind_mps":6}],"forecast_data":[{"date":"2024-04-01 01:00:00","rain_inches":1,"wind_mps":6}]}'}
    ```

    ### Versioning `outpost-etl-core`

    - Increment the major/minor version of the `outpost-etl-core` repository.
    - Build the wheel into the `outpost-etl-core/dist` directory.
    - Copy the wheel into `./dist` within this repository.
    - Install it with `uv add outpost-etl-core --find-links ./dist/`

    That should update the lock files, and the images should build correctly.
