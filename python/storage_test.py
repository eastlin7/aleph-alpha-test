import pytest
from unittest.mock import Mock, patch, MagicMock
from minio.error import MinioException, S3Error
from tenacity import RetryError
from storage import MinIOStorage
import io
import os
from exceptions import StorageError

patch('storage.retry', lambda *args, **kwargs: lambda f: f).start()
patch('tenacity.retry', lambda *args, **kwargs: lambda f: f).start()

    
@pytest.fixture
def disable_retry():  # Remove autouse=True
    """Disable retry mechanism for tests that need it disabled"""
    patches = [
        patch('storage.retry', lambda *args, **kwargs: lambda f: f),
        patch('tenacity.retry', lambda *args, **kwargs: lambda f: f),
        patch('storage.stop_after_attempt', lambda x: x),
        patch('storage.wait_exponential', lambda **kwargs: None),
        patch('storage.retry_if_exception_type', lambda x: x),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


@pytest.fixture
def mock_minio():
    with patch('storage.Minio', autospec=True) as mock:
        yield mock


# Initialization Tests
def test_initialization_creates_bucket_if_not_exists(mock_minio):
    """Test bucket creation when it doesn't exist"""
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = False
    MinIOStorage("test", "", "", "")
    mock_client.bucket_exists.assert_called_once_with("test")
    mock_client.make_bucket.assert_called_once_with("test")

def test_initialization_bucket_exists(mock_minio):
    """Test initialization when bucket already exists"""
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = True
    MinIOStorage("test", "", "", "")
    mock_client.bucket_exists.assert_called_once_with("test")
    mock_client.make_bucket.assert_not_called()

def test_initialization_handles_minio_exception(mock_minio):
    """Test exception handling during initialization"""
    with patch('storage.MinIOStorage._ensure_bucket_exists', 
               side_effect=StorageError("Bucket operation failed")):
        with pytest.raises(StorageError, match="Bucket operation failed"):
            MinIOStorage("test", "", "", "")


# TODO Add the missing 2 tests