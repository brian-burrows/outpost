import logging
import sys
import time
from src.tasks import PRODUCER_MANAGER, get_location_data
logging.basicConfig(encoding='utf-8', level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

REDIS_STREAM_KEY = "owm-ingestion-stream"
SQLITE_DB_PATH = "/tmp/owm_producer_tasks.db"

WEATHER_API_HOST = "outpost-api-weather"
WEATHER_API_PORT = "8000"

MAX_STREAM_SIZE = 100000 
TASK_SUBMIT_BATCH_SIZE = 500
API_LIMIT = 100

MAX_RETRIES = 5

class ApiRequestError(Exception):
    """Custom exception for HTTP errors (e.g., 404, 500) from the API."""
    pass

def main():
    """Executes single, one-off run task generation for all locations in the database"""
    LOGGER.info("Cron Scheduler started: Initializing producer stack.")
    manager = PRODUCER_MANAGER
    expire_time_seconds = 86400 * 2
    manager.clear_tasks(expire_time_seconds=expire_time_seconds)
    start_time = time.time() 
    try:
        LOGGER.info("Starting pagination and task production.")
        tasks = list(get_location_data())
        manager.produce_batch_to_disk(tasks) 
        total_num_flushed = 0
        while True:
            num_flushed = manager.flush_tasks_to_queue(batch_size=TASK_SUBMIT_BATCH_SIZE)
            total_num_flushed += num_flushed
            if num_flushed == 0:
                break
        end_time = time.time()
        LOGGER.info(
            f"Cron job complete: Produced and Flushed {total_num_flushed} tasks "
            f"in {(end_time - start_time):.4f} seconds. Exiting gracefully."
        )
        manager.clear_tasks(expire_time_seconds=expire_time_seconds)
        return 0
    except Exception as e:
        LOGGER.fatal(f"Unhandled error during production run: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    exit_code = 0
    try:
        exit_code = main() 
    except Exception as e:
        LOGGER.fatal(f"Startup check failed or unhandled error: {e}", exc_info=True)
        exit_code = 1
    finally:
        LOGGER.info("Shutdown complete.")
        sys.exit(exit_code)