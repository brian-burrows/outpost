import logging
from abc import ABC, abstractmethod
from uuid import UUID

LOGGER = logging.getLogger(__name__)


class DeduplicationCacheInterface(ABC):
    @abstractmethod
    def is_processed(self, task_id: UUID) -> bool:
        """Checks if a task_id has been recorded as processed."""
        pass

    @abstractmethod
    def record_processed(self, task_id: UUID) -> int:
        """Records a task_id as successfully processed."""
        pass
    
    @abstractmethod
    def discard(self, task_id: UUID) -> int:
        """Removes a task_id from the processed list (e.g., if the transaction failed)."""
        pass