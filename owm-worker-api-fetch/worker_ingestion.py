import logging
import sys
import threading

from etl.handlers import SignalCatcher
from src.tasks import (
    check_pending_entries_list,
    flush_producer_from_disk_to_queue,
    make_downstream_producer_and_persistent_storage,
    make_upstream_consumer_and_dlq,
)

logging.basicConfig(
    encoding='utf-8', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)
CONSUME_COUNT = 100
CONSUME_BLOCK_MS = 1000

def main(catcher):
    consumer = make_upstream_consumer_and_dlq()
    producer = make_downstream_producer_and_persistent_storage()
    threads = [
        threading.Thread(
            group = None, 
            target=flush_producer_from_disk_to_queue,
            name="FlusherThread",
            args=(producer, catcher)
        ),
        threading.Thread(
            group=None,
            target=check_pending_entries_list,
            name="PelCheckerThread",
            args=(consumer, producer, catcher)
        )
    ]
    for thread in threads:
        thread.start()
    # Main thread pulls tasks from the upstream queue and processes them, 
    # then passes new tasks downstream. The PEL is relative to the consumer, which 
    # checks periodically for stuck tasks in other workers and claims them
    try:
        while not catcher.stop_event.wait(timeout=1):
            # We'll ignore `failed_tasks` from `get_and_process_batch` and `produce_batch_to_disk`
            # by taking only the first entry, which are the successful tasks to send downstream
            LOGGER.info("Attempting to fetch upstream tasks and process them")
            downstream_tasks, _ = consumer.get_and_process_batch(
                batch_size = CONSUME_COUNT,
                block_ms=CONSUME_BLOCK_MS,
            )
            LOGGER.debug(f"Downstream tasks {downstream_tasks}")
            if downstream_tasks:
                LOGGER.info(f"Processed batch: obtained {len(downstream_tasks)} tasks for downstream production.")
                producer.produce_batch_to_disk(list(downstream_tasks.values()))
            else:
                LOGGER.info(f"No downstream tasks to be processed: {len(downstream_tasks)}")
    finally:
        catcher.stop_event.set()
        for t in threads:
            t.join(timeout=60)
            if t.is_alive():
                LOGGER.warning(f"{t.name} did not exit in time!")
    LOGGER.info("Worker loop finished. Exiting gracefully.")
    return 0


if __name__ == '__main__':
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