import pytest
from unittest.mock import Mock, patch, MagicMock, ANY
import io
import json
from worker import process_batch
from commoncrawl import Downloader
from tokenizer import Tokenizer
from storage import MinIOStorage
from scraper import Scraper

@pytest.fixture
def mock_tokenizer():
    return Mock(spec=Tokenizer)

@pytest.fixture
def mock_storage():
    return Mock(spec=MinIOStorage)

@pytest.fixture
def mock_scraper():
    return Mock(spec=Scraper)

@pytest.fixture
def mock_channel():
    return Mock()

@pytest.fixture
def mock_method():
    method = Mock()
    method.delivery_tag = "test_tag"
    return method

def test_successful_batch_processing(
    mock_tokenizer, mock_storage, mock_scraper, mock_channel, mock_method
):
    batch = [
        {
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000",
        }
    ]
    document_data = {"document": "text"}
    json_data = json.dumps(document_data).encode()

    mock_scraper.scrape.return_value = ["the text"]
    mock_tokenizer.create_document.return_value = document_data

    process_batch(
        mock_tokenizer,
        mock_storage,
        mock_scraper,
        mock_channel,
        mock_method,
        None,
        json.dumps(batch).encode(),
    )

    mock_storage.put_object.assert_called_once_with(ANY, json_data, len(json_data))
    mock_tokenizer.create_document.assert_called_once_with(
        "the text",
        {
            "filename": "test.warc.gz",
            "offset": "0",
            "length": "100",
            "timestamp": "20240113000000",
        },
    )
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")


def test_processes_next_item_even_when_one_fails(
    mock_tokenizer, mock_storage, mock_scraper, mock_channel, mock_method
):
    batch = [
        {
            "metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000",
        },
        {
            "metadata": {"filename": "test22222.warc.gz", "offset": "0", "length": "100"},
            "timestamp": "20240113000000",
        },
    ]
    document_data = {"document": "text"}
    json_data = json.dumps(document_data).encode()

    mock_scraper.scrape.side_effect = [Exception("error"), ["the text"]]
    mock_tokenizer.create_document.return_value = document_data

    process_batch(
        mock_tokenizer,
        mock_storage,
        mock_scraper,
        mock_channel,
        mock_method,
        None,
        json.dumps(batch).encode(),
    )

    mock_storage.put_object.assert_called_once_with(ANY, json_data, len(json_data))
    mock_tokenizer.create_document.assert_called_once_with(
        "the text",
        {
            "filename": "test22222.warc.gz",
            "offset": "0",
            "length": "100",
            "timestamp": "20240113000000",
        },
    )
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_tag")