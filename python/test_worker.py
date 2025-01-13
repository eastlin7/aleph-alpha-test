import pytest
from unittest.mock import Mock, patch, MagicMock
import io
import json
from worker import process_batch
from commoncrawl import Downloader
from tokenized_storage import TokenizedStorage

class MockWARCRecord:
    def __init__(self, html_content):
        self.html_content = html_content
        self.rec_type = "response"
    
    def content_stream(self):
        return io.BytesIO(self.html_content.encode('utf-8'))

class MockWARCIterator:
    def __init__(self, records):
        self.records = records
        self.index = 0
    
    def __iter__(self):
        return self
        
    def __next__(self):
        if self.index >= len(self.records):
            raise StopIteration
        record = self.records[self.index]
        self.index += 1
        return record

class FakeDownloader(Downloader):
    def __init__(self, content):
        self.content = content
    
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        return self.content

@pytest.fixture
def mock_storage():
    return Mock(spec=TokenizedStorage)

@pytest.fixture
def mock_channel():
    return Mock()

@pytest.fixture
def mock_method():
    method = Mock()
    method.delivery_tag = "test_tag"
    return method

def test_successful_batch_processing(mock_storage, mock_channel, mock_method):
    """Test successful processing of a batch with valid HTML content"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        # Prepare test data
        html_content = "<html><body><p>Test content</p></body></html>"
        mock_record = MockWARCRecord(html_content)
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        
        batch = [
            {
                "metadata": {
                    "filename": "test.warc.gz",
                    "offset": "0",
                    "length": "100"
                },
                "timestamp": "20240113000000"
            }
        ]

        # Mock trafilatura.extract to return some text
        with patch('trafilatura.extract', return_value="Extracted text"):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            # Verify expected behavior
            mock_storage.store_document.assert_called_once()
            mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")

def test_empty_html_content(mock_storage, mock_channel, mock_method):
    """Test handling of empty HTML content"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        # Create empty WARC record
        mock_record = MockWARCRecord("")
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        
        batch = [
            {
                "metadata": {
                    "filename": "test.warc.gz",
                    "offset": "0",
                    "length": "100"
                },
                "timestamp": "20240113000000"
            }
        ]

        # Mock trafilatura.extract to return None for empty content
        with patch('trafilatura.extract', return_value=None):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            # Verify storage was not called due to empty content
            mock_storage.store_document.assert_not_called()
            # Verify message was acknowledged
            mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")

def test_invalid_warc_format(mock_storage, mock_channel, mock_method):
    """Test handling of invalid WARC format"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        mock_warc_iterator.return_value = MockWARCIterator([])  # No records
        
        downloader = FakeDownloader(b"Invalid WARC data")
        
        batch = [
            {
                "metadata": {
                    "filename": "test.warc.gz",
                    "offset": "0",
                    "length": "100"
                },
                "timestamp": "20240113000000"
            }
        ]

        process_batch(
            mock_storage,
            downloader,
            mock_channel,
            mock_method,
            None,
            json.dumps(batch).encode()
        )

        # Verify storage was not called due to invalid WARC
        mock_storage.store_document.assert_not_called()
        # Verify message was acknowledged
        mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")

def test_downloader_failure(mock_storage, mock_channel, mock_method):
    """Test handling of downloader failures"""
    # Create downloader that raises an exception
    failing_downloader = Mock(spec=Downloader)
    failing_downloader.download_and_unzip.side_effect = Exception("Download failed")
    
    batch = [
        {
            "metadata": {
                "filename": "test.warc.gz",
                "offset": "0",
                "length": "100"
            },
            "timestamp": "20240113000000"
        }
    ]

    process_batch(
        mock_storage,
        failing_downloader,
        mock_channel,
        mock_method,
        None,
        json.dumps(batch).encode()
    )

    
    mock_storage.store_document.assert_not_called()
    # Verify message was acknowledged despite failure
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")

def test_multiple_documents_in_batch(mock_storage, mock_channel, mock_method):
    """Test processing multiple documents in a single batch"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        # Create two separate record instances
        mock_record1 = MockWARCRecord("<html><body><p>Test content 1</p></body></html>")
        mock_record2 = MockWARCRecord("<html><body><p>Test content 2</p></body></html>")
        
        # Make WARCIterator return different records for each document
        mock_warc_iterator.side_effect = [
            MockWARCIterator([mock_record1]),
            MockWARCIterator([mock_record2])
        ]
        
        downloader = FakeDownloader(b"dummy_warc_data")
        
        batch = [
            {
                "metadata": {"filename": "test1.warc.gz", "offset": "0", "length": "100"},
                "timestamp": "20240113000000"
            },
            {
                "metadata": {"filename": "test2.warc.gz", "offset": "0", "length": "100"},
                "timestamp": "20240113000000"
            }
        ]

        with patch('trafilatura.extract', return_value="Extracted text"):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            assert mock_storage.store_document.call_count == 2
            mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")
            

def test_metrics_incrementation(mock_storage, mock_channel, mock_method):
    """Test that metrics are properly incremented"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator, \
         patch('worker.documents_processed') as mock_docs_counter, \
         patch('worker.warc_records_processed') as mock_warc_counter, \
         patch('worker.successful_extractions') as mock_success_counter:
        
        mock_record = MockWARCRecord("<html><body><p>Test content</p></body></html>")
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        batch = [{
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000"
        }]

        with patch('trafilatura.extract', return_value="Extracted text"):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            mock_docs_counter.inc.assert_called_once()
            mock_warc_counter.inc.assert_called_once()
            mock_success_counter.inc.assert_called_once()

def test_malformed_json_batch(mock_storage, mock_channel, mock_method):
    """Test handling of malformed JSON in batch"""
    process_batch(
        mock_storage,
        FakeDownloader(b"dummy_warc_data"),
        mock_channel,
        mock_method,
        None,
        b'{"invalid json'
    )
    
    # Should acknowledge message even if JSON is invalid
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")
    mock_storage.store_document.assert_not_called()

def test_missing_metadata_fields(mock_storage, mock_channel, mock_method):
    """Test handling of batch items with missing metadata fields"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        mock_record = MockWARCRecord("<html><body><p>Test content</p></body></html>")
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        batch = [{
            # Missing required metadata fields
            "metadata": {},
            "timestamp": "20240113000000"
        }]

        with patch('trafilatura.extract', return_value="Extracted text"):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            # Should handle gracefully and acknowledge message
            mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")

def test_non_response_warc_record(mock_storage, mock_channel, mock_method):
    """Test handling of non-response WARC records"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        # Create a record with different rec_type
        mock_record = MockWARCRecord("<html><body><p>Test content</p></body></html>")
        mock_record.rec_type = "request"  # Change record type
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        batch = [{
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000"
        }]

        process_batch(
            mock_storage,
            downloader,
            mock_channel,
            mock_method,
            None,
            json.dumps(batch).encode()
        )

        # Should not process non-response records
        mock_storage.store_document.assert_not_called()

def test_text_extraction_error_handling(mock_storage, mock_channel, mock_method):
    """Test handling of trafilatura extraction errors"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator, \
         patch('worker.text_extraction_failures') as mock_failure_counter:
        
        mock_record = MockWARCRecord("<html><body><p>Test content</p></body></html>")
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        batch = [{
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000"
        }]

        # Simulate trafilatura raising an exception
        with patch('trafilatura.extract', side_effect=Exception("Extraction failed")):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            mock_failure_counter.inc.assert_called_once()
            mock_storage.store_document.assert_not_called()

def test_storage_error_handling(mock_storage, mock_channel, mock_method):
    """Test handling of storage errors"""
    with patch('worker.WARCIterator', autospec=True) as mock_warc_iterator:
        mock_record = MockWARCRecord("<html><body><p>Test content</p></body></html>")
        mock_warc_iterator.return_value = MockWARCIterator([mock_record])
        
        downloader = FakeDownloader(b"dummy_warc_data")
        batch = [{
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000"
        }]

        # Make storage.store_document raise an exception
        mock_storage.store_document.side_effect = Exception("Storage failed")

        with patch('trafilatura.extract', return_value="Extracted text"):
            process_batch(
                mock_storage,
                downloader,
                mock_channel,
                mock_method,
                None,
                json.dumps(batch).encode()
            )

            # Should acknowledge message even if storage fails
            mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")