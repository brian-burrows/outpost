import logging
import sys

from etl.handlers import SignalCatcher

from src.tasks import make_upstream_consumer_and_dlq

# TODO: migrate these to `src/tasks.py` with a refactor
CONSUME_COUNT = 100
CONSUME_BLOCK_MS = 1000
CATEGORIZATION_MAX_RETRIES = 5
CATEGORIZATION_MAX_IDLE_MS = 600000
CATEGORIZATION_EXPIRY_TIME_MS = 2.16e7

logging.basicConfig(
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)


def main(catcher):
    consumer = make_upstream_consumer_and_dlq()
    try:
        while not catcher.stop_event.wait(timeout=1):
            # TOOD: These should be instance arguments so that the main function doesn't have to know how the tasks are consumed.
            # The main function should just be responsible for orchestrating the components, not configuration.
            consumer.get_and_process_batch(
                batch_size=CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            consumer.get_and_process_stuck_tasks(
                max_idle_ms=CATEGORIZATION_MAX_IDLE_MS,
                expiry_time_ms=CATEGORIZATION_EXPIRY_TIME_MS,
                max_retries=CATEGORIZATION_MAX_RETRIES,
                batch_size=CONSUME_COUNT,
            )
    finally:
        catcher.stop_event.set()
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == "__main__":
    with SignalCatcher() as catcher:
        exit_code = 0
        try:
            exit_code = main(catcher)
        except Exception as e:
            LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
            exit_code = 1
        finally:
            catcher.stop_event.set()
            LOGGER.info("Shutdown complete.")
            sys.exit(exit_code)
