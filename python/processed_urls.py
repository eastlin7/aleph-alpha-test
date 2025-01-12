import hashlib
from minio import Minio
from prometheus_client import Counter
import io
import os
documents_duplicate = Counter('batcher_documents_duplicate_total', 'Documents filtered due to being already processed')

class ProcessedURLTracker:
    def __init__(self):
        self.client = Minio(
            os.getenv('MINIO_ENDPOINT', 'localhost:9500'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )
        self.bucket_name = 'processed-urls'
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def _generate_key(self, url: str, timestamp: str) -> str:
        # Key format: hash(url + timestamp)
        return hashlib.sha256(f"{url}:{timestamp}".encode()).hexdigest()

    def is_processed(self, url: str, timestamp: str) -> bool:
        key = self._generate_key(url, timestamp)
        try:
            self.client.stat_object(self.bucket_name, f"{key}.marker")
            documents_duplicate.inc()
            return True
        except:
            return False

    def mark_processed(self, url: str, timestamp: str) -> None:
        key = self._generate_key(url, timestamp)
        try:
            empty_data = io.BytesIO(b"")
            self.client.put_object(
                self.bucket_name,
                f"{key}.marker",
                empty_data,
                0
            )
        except Exception as e:
            print(f"Error marking URL+timestamp as processed: {e}")