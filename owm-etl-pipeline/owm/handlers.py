import logging
import signal
import threading

logger = logging.getLogger()

class SignalCatcher:
    """A context manager to register signals on entry."""
    
    def __init__(self, signals=(signal.SIGINT, signal.SIGTERM)):
        self._signals = signals
        self._original_handlers = {}
        self.stop_event = threading.Event()

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}. Setting should_run to False.")
        self.stop_event.set()

    @property
    def should_run(self):
        return not self.stop_event.is_set()

    def __enter__(self):
        # We want to make sure that we handle the signals gracefully
        # And, we want to ensure that once we break out of the loop
        # we can replace the original signal handlers to unburden
        # the end user from having to exit properly
        for sig in self._signals:
            self._original_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self.signal_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)