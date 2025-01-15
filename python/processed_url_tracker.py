import hashlib
from prometheus_client import Counter
import io
import logging
from exceptions import StorageError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)

storage_errors = Counter(
    "storage_errors_total", "Storage operation errors", ["error_type"]
)

class ProcessedURLTracker:
    def __init__(self, storage):
        self.storage = storage

    def is_processed(self, url: str, timestamp: str) -> bool:
        if not url:
            raise ValueError("url cannot be empty")
        if not timestamp:
            raise ValueError("timestamp cannot be empty")
        key = self._generate_key(url, timestamp)
        return self.storage.key_exists(f"{key}.marker")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(StorageError),
    )
    def mark_processed(self, url: str, timestamp: str) -> None:
        """Mark URL as processed with proper error handling
        
        Args:
            url: The URL to mark as processed
            timestamp: The timestamp for the URL
            
        Raises:
            ValueError: If url or timestamp is empty
            StorageError: If storage operation fails
        """
        if not url:
            print("not url")
            raise ValueError("url cannot be empty")
        if not timestamp:
            print("not timestamp")
            raise ValueError("timestamp cannot be empty")
            
        key = self._generate_key(url, timestamp)
        empty_data = b""
        
        try:
            self.storage.put_object(f"{key}.marker", empty_data)
        except StorageError:
            logger.error(f"Failed to mark URL as processed:")
            storage_errors.labels(error_type="mark_processed").inc()
            raise

    def _generate_key(self, url: str, timestamp: str) -> str:
        """Generate a unique key for the URL record"""
        return hashlib.sha256(f"{url}:{timestamp}".encode()).hexdigest()
