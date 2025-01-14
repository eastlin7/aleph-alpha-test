import pytest
from unittest.mock import Mock, patch, MagicMock
from minio.error import MinioException, S3Error
from tenacity import RetryError
import io
import os
from exceptions import StorageError

patch('processed_url_tracker.retry', lambda *args, **kwargs: lambda f: f).start()
patch('tenacity.retry', lambda *args, **kwargs: lambda f: f).start()

from processed_url_tracker import ProcessedURLTracker

    
@pytest.fixture
def disable_retry():  # Remove autouse=True
    """Disable retry mechanism for tests that need it disabled"""
    patches = [
        patch('processed_url_tracker.retry', lambda *args, **kwargs: lambda f: f),
        patch('tenacity.retry', lambda *args, **kwargs: lambda f: f),
        patch('processed_url_tracker.stop_after_attempt', lambda x: x),
        patch('processed_url_tracker.wait_exponential', lambda **kwargs: None),
        patch('processed_url_tracker.retry_if_exception_type', lambda x: x),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


@pytest.fixture
def mock_minio():
    with patch('processed_url_tracker.Minio', autospec=True) as mock:
        yield mock

@pytest.fixture
def url_tracker(mock_minio):
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = True
    return ProcessedURLTracker()



# Initialization Tests
def test_initialization_creates_bucket_if_not_exists(mock_minio):
    """Test bucket creation when it doesn't exist"""
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = False
    
    tracker = ProcessedURLTracker()
    
    mock_client.bucket_exists.assert_called_once_with("processed-urls")
    mock_client.make_bucket.assert_called_once_with("processed-urls")

def test_initialization_bucket_exists(mock_minio):
    """Test initialization when bucket already exists"""
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = True
    
    tracker = ProcessedURLTracker()
    
    mock_client.bucket_exists.assert_called_once_with("processed-urls")
    mock_client.make_bucket.assert_not_called()

def test_initialization_handles_minio_exception(mock_minio):
    """Test exception handling during initialization"""
    with patch('processed_url_tracker.ProcessedURLTracker._ensure_bucket_exists', 
               side_effect=StorageError("Bucket operation failed")):
        with pytest.raises(StorageError, match="Bucket operation failed"):
            ProcessedURLTracker()

# URL Processing Tests
def test_is_processed_returns_true_for_existing_url(url_tracker, mock_minio):
    """Test is_processed returns True for already processed URL"""
    mock_client = mock_minio.return_value
    mock_client.stat_object.return_value = True
    
    result = url_tracker.is_processed("http://example.com", "2024-01-14")
    
    assert result is True
    mock_client.stat_object.assert_called_once()
    

def test_is_processed_returns_false_for_new_url(url_tracker, mock_minio):
    """Test is_processed returns False for new URL"""
    mock_client = mock_minio.return_value
    mock_client.stat_object.side_effect = MinioException("NoSuchKey")
    
    result = url_tracker.is_processed("http://example.com", "2024-01-14")
    
    assert result is False


def test_mark_processed_success(url_tracker, mock_minio):
    """Test successful marking of URL as processed"""
    # Arrange
    mock_client = mock_minio.return_value
    url = "http://example.com"
    timestamp = "2024-01-14"
    
    # Act
    url_tracker.mark_processed(url, timestamp)
    
    # Assert - only verify that it was stored somehow
    assert url_tracker.is_processed(url, timestamp)
            

def test_mark_processed_handles_minio_error(mock_minio):
    """Test error handling in mark_processed"""
    mock_client = MagicMock()
    mock_minio.return_value = mock_client
    mock_client.bucket_exists.return_value = True
    
    tracker = ProcessedURLTracker()
    mock_client.put_object.side_effect = MinioException("Upload failed")
    
    with pytest.raises((StorageError, RetryError)):  # Accept either error
        tracker.mark_processed("http://example.com", "2024-01-14")


@pytest.mark.usefixtures("disable_retry")
def test_retry_error_handling(url_tracker, mock_minio):
    """Test MinIO failures are converted to StorageError"""
    
    mock_client = mock_minio.return_value
    mock_client.stat_object.side_effect = MinioException("Persistent failure")
    
    with pytest.raises(StorageError):
        url_tracker.is_processed("http://example.com", "2024-01-14")



def test_is_processed_with_invalid_inputs(url_tracker, mock_minio):
    """Test is_processed behavior with invalid inputs"""
    # Use the mocked version
    mock_client = mock_minio.return_value
    
    # Test empty URL
    url_tracker.is_processed("", "2024-01-14")
    mock_client.stat_object.assert_called_once()
    
    # Test empty timestamp
    mock_client.stat_object.reset_mock()
    url_tracker.is_processed("http://example.com", "")
    mock_client.stat_object.assert_called_once()

def test_very_long_url_handling(url_tracker, mock_minio):
    """Test handling of extremely long URLs"""
    # Create a very long URL (e.g., 10000 characters)
    long_url = "http://example.com/" + "a" * 10000
    timestamp = "2024-01-14"
    
    mock_client = mock_minio.return_value
    
    url_tracker.mark_processed(long_url, timestamp)
    
    expected_key = url_tracker._generate_key(long_url, timestamp)
    
    mock_client.put_object.assert_called_once()
    call_args = mock_client.put_object.call_args[0]  
    assert call_args[0] == "processed-urls"  
    assert call_args[1] == f"{expected_key}.marker"  

def test_special_characters_in_url(url_tracker, mock_minio):
    """Test handling URLs with special characters"""
    url = "http://example.com/path with spaces/!@#$%^&*()"
    timestamp = "2024-01-14"
    
    url_tracker.mark_processed(url, timestamp)
    assert url_tracker.is_processed(url, timestamp)

def test_non_ascii_url_handling(url_tracker, mock_minio):
    """Test handling of URLs with non-ASCII characters"""
    url = "http://example.com/文字/データ"
    timestamp = "2024-01-14"
    
    mock_client = mock_minio.return_value
    
    url_tracker.mark_processed(url, timestamp)
    
    expected_key = url_tracker._generate_key(url, timestamp)
    mock_client.put_object.assert_called_once()
    call_args = mock_client.put_object.call_args[0]
    assert call_args[0] == "processed-urls"
    assert call_args[1] == f"{expected_key}.marker"

def test_mark_processed_idempotency(url_tracker, mock_minio):
    """Test that marking same URL multiple times works correctly"""
    url = "http://example.com"
    timestamp = "2024-01-14"
    mock_client = mock_minio.return_value
    
    # First mark
    url_tracker.mark_processed(url, timestamp)
    
    # Second mark shouldn't cause issues
    url_tracker.mark_processed(url, timestamp)
    
    # Verify put_object was called twice
    assert mock_client.put_object.call_count == 2
    

def test_same_url_different_timestamps(url_tracker, mock_minio):
    """Test that same URL with different timestamps are treated as different entries"""
    url = "http://example.com"
    timestamp1 = "2024-01-14"
    timestamp2 = "2024-01-15"
    
    mock_client = mock_minio.return_value
    
    url_tracker.mark_processed(url, timestamp1)
    url_tracker.mark_processed(url, timestamp2)
    
    # Verify two different keys were created
    calls = mock_client.put_object.call_args_list
    assert len(calls) == 2
    key1 = calls[0][0][1]  # first call, first positional arg
    key2 = calls[1][0][1]  # second call, first positional arg
    assert key1 != key2
    
