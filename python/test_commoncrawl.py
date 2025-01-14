import unittest
from unittest.mock import Mock, patch
import requests
from commoncrawl import CCDownloader, CSVIndexReader, BASE_URL
from exceptions import DownloadError

class TestCCDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = CCDownloader(BASE_URL)

    def test_successful_data_retrieval(self):
        """Core Business Logic: System can retrieve and process data"""
        with patch('requests.Session') as mock_session_class:
            # Setup mock at the class level
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            # Configure response
            mock_response = Mock()
            mock_response.content = b"compressed-data-stub"
            mock_response.raise_for_status = Mock()
            mock_session.get.return_value = mock_response
            
            # Mock gzip at module level
            with patch('gzip.decompress', return_value=b"test data"):
                # Create new downloader instance after mocks
                downloader = CCDownloader(BASE_URL)
                result = downloader.download_and_unzip("test.gz", 0, 100)
                
                self.assertEqual(result, b"test data")

    def test_network_failure_handling(self):
        """Critical Failure: System handles network failures"""
        with patch('requests.Session') as mock_session:
            mock_session.return_value.get.side_effect = requests.ConnectionError()
            
            with self.assertRaises(DownloadError):
                self.downloader.download_and_unzip("test.gz", 0, 100)


    def test_corrupted_data_handling(self):
        """Critical Failure: System handles corrupted data"""
        with patch('requests.Session') as mock_session:
            mock_response = Mock()
            mock_response.content = b"corrupted"
            mock_response.raise_for_status = Mock()
            mock_session.return_value.get.return_value = mock_response
            
            with self.assertRaises(DownloadError):
                self.downloader.download_and_unzip("test.gz", 0, 100)

    def test_resource_exhaustion_handling(self):
        """Critical Failure: System handles resource exhaustion"""
        with patch('requests.Session') as mock_session:
            mock_session.return_value.get.side_effect = requests.exceptions.RequestException("Memory error")
            
            with self.assertRaises(DownloadError):
                self.downloader.download_and_unzip("test.gz", 0, 1000000000)  # Large request

class TestCSVIndexReader(unittest.TestCase):
    def test_index_data_accessibility(self):
        """Core Business Logic: System can access index data"""
        content = "url\tfile.gz\t0\t100\t1\n"
        
        with patch('builtins.open', unittest.mock.mock_open(read_data=content)):
            with CSVIndexReader("test.csv") as reader:
                data = next(reader)
                self.assertIsNotNone(data)

    def test_inaccessible_index_handling(self):
        """Critical Failure: System handles inaccessible index"""
        with patch('builtins.open', side_effect=FileNotFoundError()):
            with self.assertRaises(FileNotFoundError):
                with CSVIndexReader("nonexistent.csv"):
                    pass


if __name__ == '__main__':
    unittest.main()