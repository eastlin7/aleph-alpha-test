import io
import json
import uuid
from minio import Minio
from prometheus_client import Counter
import os

storage_successes = Counter('worker_storage_successes_total', 'Successfully stored documents')
storage_failures = Counter('worker_storage_failures_total', 'Failed storage attempts')

class MinIOStorage:
    def __init__(self):
        self.client = Minio(
            os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        self.bucket_name = 'crawled-docs'
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def store_document(self, text, metadata):
        try:
            doc_id = str(uuid.uuid4())
            document = {
                "text": text,
                "metadata": metadata,
                "timestamp": metadata.get("timestamp"),
                "url": metadata.get("url")
            }
            
            json_data = json.dumps(document).encode('utf-8')
            self.client.put_object(
                self.bucket_name,
                f"{doc_id}.json",
                io.BytesIO(json_data),
                len(json_data)
            )
            storage_successes.inc()
            return True
        except Exception as e:
            print(f"Error storing document: {e}")
            storage_failures.inc()
            return False