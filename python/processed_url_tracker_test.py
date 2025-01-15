import pytest
from unittest.mock import Mock, patch, MagicMock, call
from minio.error import MinioException, S3Error
from tenacity import RetryError
import io
import os
from exceptions import StorageError

patch('processed_url_tracker.retry', lambda *args, **kwargs: lambda f: f).start()
patch('tenacity.retry', lambda *args, **kwargs: lambda f: f).start()

from processed_url_tracker import ProcessedURLTracker

@pytest.fixture
def url_tracker(mock_storage):
    return ProcessedURLTracker(mock_storage)

@pytest.fixture
def mock_storage():
    mock_storage = MagicMock()
    return mock_storage

# URL Processing Tests
def test_is_processed_returns_true_for_existing_url(url_tracker, mock_storage):
    """Test is_processed returns True for already processed URL"""
    mock_storage.key_exists.return_value = True
    result = url_tracker.is_processed("http://example.com", "2024-01-14")

    assert result is True
    mock_storage.key_exists.assert_called_once_with("b5cbf8ebe0d9aceb153ce90dee98025f4b04aea00a5be95a328d4dc6f7c917be.marker")

def test_is_processed_returns_false_for_new_url(url_tracker, mock_storage):
    """Test is_processed returns False for new URL"""
    mock_storage.key_exists.return_value = False
    result = url_tracker.is_processed("http://example.com", "2024-01-14")

    assert result is False

def test_mark_processed_success(url_tracker, mock_storage):
    """Test successful marking of URL as processed"""
    url = "http://example.com"
    timestamp = "2024-01-14"

    url_tracker.mark_processed(url, timestamp)
    
    mock_storage.put_object.assert_called_once_with("b5cbf8ebe0d9aceb153ce90dee98025f4b04aea00a5be95a328d4dc6f7c917be.marker", "")

def test_mark_processed_handles_storage_error(mock_storage):
    """Test error handling in mark_processed"""

    tracker = ProcessedURLTracker(mock_storage)
    mock_storage.put_object.side_effect = StorageError("Upload failed")

    with pytest.raises((StorageError, RetryError)):
        tracker.mark_processed("http://example.com", "2024-01-14")

def test_is_processed_with_invalid_inputs(url_tracker, mock_storage):
    """Test is_processed behavior with invalid inputs"""
    
    url_tracker.is_processed("", "2024-01-14") # TODO: This should explode!!, like, be a thing. Dont allow empty strings
    mock_storage.key_exists.assert_called_once_with("16d3ed2a88f738b8ea873b0cfe72d851c1dcc2e6ce9f2ce821213d2f53f2b9c1.marker")
    
    mock_storage.key_exists.reset_mock()
    url_tracker.is_processed("http://example.com", "")
    mock_storage.key_exists.assert_called_once_with("79c56239195762a5921c37f4a50792109bbdaaf1b0a650e46101818e83b27145.marker")

def test_very_long_url_handling(url_tracker, mock_storage):
    """Test handling of extremely long URLs"""
    # Create a very long URL (e.g., 10000 characters)
    long_url = "http://example.com/" + "a" * 10000
    timestamp = "2024-01-14"

    url_tracker.mark_processed(long_url, timestamp)

    mock_storage.put_object.assert_called_once_with(f"c3e77a4294e8dfb9a180e7e31da9fc846d2b7bf517e4c7e244c0935ecb840c93.marker", "")

def test_special_characters_in_url(url_tracker, mock_storage):
    """Test handling URLs with special characters"""
    url = "http://example.com/path with spaces/!@#$%^&*()"
    timestamp = "2024-01-14"
    url_tracker.mark_processed(url, timestamp)
    mock_storage.put_object.assert_called_once_with(f"5963590340f618797cca659bbb385b87abf92dac56ee7650d966fedf74f81f34.marker", "")

def test_non_ascii_url_handling(url_tracker, mock_storage):
    """Test handling of URLs with non-ASCII characters"""
    url = "http://example.com/文字/データ"
    timestamp = "2024-01-14"

    url_tracker.mark_processed(url, timestamp)
    mock_storage.put_object.assert_called_once_with(f"336bd18384c5b50c362b4e853e30f102f2d4b5b9583e76c01454db4736d9ecd3.marker", "")

def test_mark_processed_idempotency(url_tracker, mock_storage):
    """Test that marking same URL multiple times works correctly"""
    url = "http://example.com"
    timestamp = "2024-01-14"

    url_tracker.mark_processed(url, timestamp)
    url_tracker.mark_processed(url, timestamp)
    
    mock_storage.put_object.assert_has_calls([
        call("b5cbf8ebe0d9aceb153ce90dee98025f4b04aea00a5be95a328d4dc6f7c917be.marker", ""),
        call("b5cbf8ebe0d9aceb153ce90dee98025f4b04aea00a5be95a328d4dc6f7c917be.marker", ""),
    ])

def test_same_url_different_timestamps(url_tracker, mock_storage):
    """Test that same URL with different timestamps are treated as different entries"""
    url = "http://example.com"
    timestamp1 = "2024-01-14"
    timestamp2 = "2024-01-15"

    url_tracker.mark_processed(url, timestamp1)
    url_tracker.mark_processed(url, timestamp2)

    mock_storage.put_object.assert_has_calls([
        call("b5cbf8ebe0d9aceb153ce90dee98025f4b04aea00a5be95a328d4dc6f7c917be.marker", ""),
        call("e381c10ee596c6951455d39bb5b7c429454ddee6a16408bf57db6a48eb9b1482.marker", ""),
    ])

