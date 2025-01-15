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

def test_key_exists_success(disable_retry, mock_minio):
    """Test successful check for existing key"""
    mock_client = mock_minio.return_value
    storage = MinIOStorage("test-bucket", "localhost:9000", "access", "secret")
    
    # Setup mock for existing object
    mock_client.stat_object.return_value = True
    
    result = storage.key_exists("existing-key")
    
    assert result is True
    mock_client.stat_object.assert_called_once_with("test-bucket", "existing-key")

def test_key_exists_missing_key(disable_retry, mock_minio):
    """Test behavior when key doesn't exist"""
    mock_client = mock_minio.return_value
    storage = MinIOStorage("test-bucket", "localhost:9000", "access", "secret")
    
    # Setup mock for missing object
    mock_client.stat_object.side_effect = MinioException("NoSuchKey: The specified key does not exist")
    
    result = storage.key_exists("non-existing-key")
    
    assert result is False

    mock_client.stat_object.assert_called_once_with("test-bucket", "non-existing-key")


def test_put_object_success(disable_retry, mock_minio):
    mock_client = mock_minio.return_value
    storage = MinIOStorage("test-bucket", "localhost:9000", "access", "secret")
    
    key = "test-key"
    data = b"test data"
    length = len(data)
    
    storage.put_object(key, data, length)
    
    called_args = mock_client.put_object.call_args[0]
    assert called_args[0] == "test-bucket"  # bucket name
    assert called_args[1] == "test-key"     # key
    assert isinstance(called_args[2], io.BytesIO)  # data stream
    assert called_args[3] == length         # length

        
def test_put_object_failure(disable_retry, mock_minio):
    mock_client = mock_minio.return_value
    storage = MinIOStorage("test-bucket", "localhost:9000", "access", "secret")
    
    key = "test-key"
    data = b"test data"
    length = len(data)
    
    # Mock MinioException instead of ValueError
    mock_client.put_object.side_effect = MinioException("Failed to put object")
    
    with pytest.raises(StorageError) as exc_info:
        storage.put_object(key, data, length)
    
    assert "Failed to put object" in str(exc_info.value)
    mock_client.put_object.assert_called_once()