from minio import Minio
import os
import io
from exceptions import StorageError
from minio.error import MinioException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

class MinIOStorage:
    def __init__(self, bucket_name, endpoint, access_key, secret_key):
        self.client = Minio(
            endpoint,
            access_key,
            secret_key,
            secure=False
        )
        self.bucket_name = bucket_name
        self._ensure_bucket_exists()

    def raise_storage_error(retry_state):
        # This callback is invoked after all retry attempts fail.
        # We raise a StorageError with our custom message.
        raise StorageError("Failed to check processed status") from retry_state.outcome.exception()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(MinioException),
        retry_error_callback=raise_storage_error,
    )
    def put_object(self, key, data, length=0):
        self.client.put_object(self.bucket_name, key, io.BytesIO(data), length)
    
    def _ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(MinioException),
        retry_error_callback=raise_storage_error,
    )
    def key_exists(self, key):
        try:
            self.client.stat_object(self.bucket_name, key)
        except MinioException as e:
            if "NoSuchKey" in str(e):
                return False
            raise

        return True