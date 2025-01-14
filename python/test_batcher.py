from batcher import process_index, publish_batch, PublishError
from commoncrawl import Downloader, IndexReader
from commoncrawl import Downloader, IndexReader
from rabbitmq import MessageQueueChannel
from unittest.mock import Mock
import pytest
import json

class FakeReader(IndexReader):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return iter(self.data)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeDownloader(Downloader):
    def __init__(self, row: str):
        self.row = row

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        return f"{self.row}".encode("utf-8")


class ChannelSpy(MessageQueueChannel):
    def __init__(self):
        self.num_called = 0

    def basic_publish(self, exchange, routing_key, body):
        self.num_called += 1

def test_filter_non_english_documents():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    url_tracker = Mock()
    url_tracker.is_processed.return_value = False  # Ensure URL isn't marked as processed
    process_index(reader, channel, downloader, url_tracker, 2)
    assert channel.num_called == 0

def test_filter_bad_status_code():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    url_tracker = Mock()
    url_tracker.is_processed.return_value = False
    process_index(reader, channel, downloader, url_tracker, 2)
    assert channel.num_called == 0

def test_publish_all_urls():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
        '0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}'
    )
    url_tracker = Mock()
    url_tracker.is_processed.return_value = False
    process_index(reader, channel, downloader, url_tracker, 2)
    assert channel.num_called > 0, "Channel was never called"
    assert channel.num_called == 3, f"Expected 3 calls, got {channel.num_called}"




def test_publish_batch_handles_publish_error():
    """Test handling of publish failures"""
    channel = Mock()
    channel.basic_publish.side_effect = Exception("Connection failed")
    url_tracker = Mock()
    
    batch = [
        {
            "surt_url": "test.com",
            "timestamp": "20240101",
            "metadata": {"status": "200"}
        }
    ]
    
    with pytest.raises(PublishError):
        publish_batch(channel, batch, url_tracker)
    
    # Verify URL tracker wasn't called since publish failed
    url_tracker.mark_processed.assert_not_called()

def test_process_index_handles_json_decode_error():
    """Test handling of invalid JSON in the input data"""
    reader = FakeReader([
        ["test.com", "cdx-00000.gz", "0", "100", "1"]
    ])
    
    channel = ChannelSpy()
    # Provide invalid JSON in the data
    downloader = FakeDownloader('test.com 20240101 {invalid_json}')
    url_tracker = Mock()
    url_tracker.is_processed.return_value = False
    
    # Should not raise exception, should continue processing
    process_index(reader, channel, downloader, url_tracker, 2)
    assert channel.num_called == 0

def test_batch_processing_respects_size():
    """Test that batches are published when reaching specified size"""
    reader = FakeReader([
        ["url1", "cdx-00000.gz", "0", "100", "1"],
        ["url2", "cdx-00000.gz", "100", "100", "2"],
        ["url3", "cdx-00000.gz", "200", "100", "3"]
    ])
    
    channel = ChannelSpy()
    valid_doc = {
        "url": "test.com",
        "status": "200",
        "languages": ["eng"]
    }
    
    downloader = FakeDownloader(
        f'test.com 20240101 {json.dumps(valid_doc)}'
    )
    
    url_tracker = Mock()
    url_tracker.is_processed.return_value = False
    
    batch_size = 2
    process_index(reader, channel, downloader, url_tracker, batch_size)
    
    # With 3 valid documents and batch size 2, should have 2 batches
    # One full batch of 2 and one partial batch of 1
    assert channel.num_called == 2

def test_skip_already_processed_urls():
    """Test that already processed URLs are skipped"""
    reader = FakeReader([
        ["test.com", "cdx-00000.gz", "0", "100", "1"]
    ])
    
    channel = ChannelSpy()
    valid_doc = {
        "url": "test.com",
        "status": "200",
        "languages": ["eng"]
    }
    
    downloader = FakeDownloader(
        f'test.com 20240101 {json.dumps(valid_doc)}'
    )
    
    url_tracker = Mock()
    # Simulate URL already being processed
    url_tracker.is_processed.return_value = True
    
    process_index(reader, channel, downloader, url_tracker, 2)
    assert channel.num_called == 0