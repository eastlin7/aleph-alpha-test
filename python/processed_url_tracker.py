import hashlib
from minio import Minio
from minio.error import MinioException, S3Error
from prometheus_client import Counter
import io
import os
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Set up logging
logger = logging.getLogger(__name__)

# Metrics
documents_duplicate = Counter(
    "batcher_documents_duplicate_total",
    "Documents filtered due to being already processed",
)
storage_errors = Counter(
    "storage_errors_total", "Storage operation errors", ["error_type"]
)
minio_retries = Counter("minio_retries_total", "Number of retried Minio operations")


class StorageError(Exception):
    """Base exception for storage-related errors"""

    pass


class ProcessedURLTracker:
    def __init__(self):
        """Initialize the URL tracker with proper error handling"""
        try:
            self.client = Minio(
                os.getenv("MINIO_ENDPOINT", "localhost:9500"),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                secure=False,
            )
            self.bucket_name = "processed-urls"
            self._ensure_bucket_exists()

        except MinioException as e:
            storage_errors.labels(error_type="bucket_creation").inc()
            raise StorageError(f"Bucket operation failed: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(MinioException),
    )
    def is_processed(self, url: str, timestamp: str) -> bool:
        """Check if URL has been processed with proper error handling"""
        try:
            key = self._generate_key(url, timestamp)
            self.client.stat_object(self.bucket_name, f"{key}.marker")
            documents_duplicate.inc()
            return True
        except MinioException as e:
            if "NoSuchKey" in str(e):  # Object doesn't exist
                return False
            logger.error(f"Error checking processed status: {e}")
            storage_errors.labels(error_type="check_processed").inc()
            raise StorageError(f"Failed to check processed status: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(StorageError),
    )
    def mark_processed(self, url: str, timestamp: str) -> None:
        """Mark URL as processed with proper error handling"""
        try:
            key = self._generate_key(url, timestamp)
            empty_data = io.BytesIO(b"")
            self.client.put_object(self.bucket_name, f"{key}.marker", empty_data, 0)
            logger.debug(f"Marked as processed: {url}")
        except MinioException as e:
            logger.error(f"Failed to mark URL as processed: {e}")
            storage_errors.labels(error_type="mark_processed").inc()
            raise StorageError(f"Failed to mark as processed: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(MinioException),
        after=lambda retry_state: minio_retries.inc(),
    )
    def _ensure_bucket_exists(self) -> None:
        """Ensure bucket exists with retries"""
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            logger.info(f"Created new bucket: {self.bucket_name}")

    def _generate_key(self, url: str, timestamp: str) -> str:
        """Generate a unique key for the URL record"""
        return hashlib.sha256(f"{url}:{timestamp}".encode()).hexdigest()
