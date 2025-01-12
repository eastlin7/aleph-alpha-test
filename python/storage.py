import io
import json
import uuid
from minio import Minio
from prometheus_client import Counter

storage_successes = Counter('worker_storage_successes_total', 'Successfully stored documents')
storage_failures = Counter('worker_storage_failures_total', 'Failed storage attempts')

class MinIOStorage:
    def __init__(self, endpoint, access_key, secret_key, bucket_name, secure=False):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
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