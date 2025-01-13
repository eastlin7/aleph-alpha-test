from abc import ABC, abstractmethod
import csv
import gzip
from typing import Generator, List, Optional
import requests
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
)
from prometheus_client import Counter, Histogram

# Set up logging
logger = logging.getLogger(__name__)

# Constants
CRAWL_PATH = "cc-index/collections/CC-MAIN-2024-30/indexes"
BASE_URL = "https://data.commoncrawl.org"

# Metrics
download_retries = Counter(
    "commoncrawl_download_retries_total", "Number of retried downloads"
)
download_failures = Counter(
    "commoncrawl_download_failures_total", "Failed download attempts"
)
download_successes = Counter(
    "commoncrawl_download_successes_total", "Successful downloads"
)
download_time = Histogram(
    "commoncrawl_download_duration_seconds", "Time spent downloading"
)


class DownloadError(Exception):
    """Custom exception for download-related errors"""

    pass


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(
            (requests.RequestException, requests.ConnectionError)
        ),
        before=before_log(logger, logging.DEBUG),
        after=lambda retry_state: download_retries.inc(),
    )
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        """
        Download and decompress data from Common Crawl with comprehensive error handling.

        Args:
            url: The URL path to download
            start: Starting byte position
            length: Number of bytes to download

        Returns:
            Decompressed bytes of the downloaded content

        Raises:
            DownloadError: If download or decompression fails
        """
        try:
            headers = {"Range": f"bytes={start}-{start+length-1}"}
            full_url = f"{self.base_url}/{url}"

            with download_time.time():
                response = self.session.get(
                    full_url,
                    headers=headers,
                    timeout=(10, 30),  # (connect timeout, read timeout)
                )

            response.raise_for_status()

            try:
                decompressed_data = gzip.decompress(response.content)
                download_successes.inc()
                return decompressed_data
            except gzip.BadGzipFile as e:
                download_failures.inc()
                logger.error(f"Failed to decompress data from {url}: {e}")
                raise DownloadError(f"Decompression failed: {e}")

        except requests.RequestException as e:
            
            # TODO: is this correct thing to raise? What about raise for status?
            download_failures.inc()
            logger.error(f"Failed to download from {url}: {e}")
            raise DownloadError(f"Download failed: {e}")


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __enter__(self):
        """Support context manager protocol"""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support context manager protocol"""
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.file = None
        self.reader = None

    def __enter__(self):
        self.file = open(self.filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")
        return self
        

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)
        


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756\tcdx-00000.gz\t0\t188224\t1\n\
101,141,199,66)/robots.txt 20240714155331\tcdx-00000.gz\t188224\t178351\t2\n\
104,223,1,100)/ 20240714230020\tcdx-00000.gz\t366575\t178055\t3"

    filename.write_text(index)

    # Use context manager properly in test
    with CSVIndexReader(filename) as reader:
        result = list(reader)

    assert result == [
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
