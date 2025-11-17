class RateLimitExceededError(Exception):
    """Signal to any producer/consumer that the message should be retried in the future."""
    pass 

class DuplicateTaskError(Exception):
    """Signal to any producer/consumer that the task has already been processed."""
    pass