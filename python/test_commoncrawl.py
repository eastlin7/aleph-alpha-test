import unittest
from unittest.mock import Mock, patch
import gzip
import requests
from commoncrawl import CCDownloader, BASE_URL, DownloadError, CSVIndexReader  

class TestCCDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = CCDownloader(BASE_URL)
        


    def test_recovers_from_temporary_failures(self):
        """Test system can recover from temporary failures"""
        test_data = b"test data"
        compressed_data = gzip.compress(test_data)
        
        with patch('requests.Session.get') as mock_get:
            mock_success = Mock()
            mock_success.content = compressed_data
            mock_success.raise_for_status = Mock()
            
            # First fail, then succeed
            mock_get.side_effect = [
                requests.ConnectionError(),
                mock_success
            ]
            
            result = self.downloader.download_and_unzip("test.gz", 0, 100)
            self.assertEqual(result, test_data)
            
            

    def test_fails_on_corrupted_data(self):
        """Test system handles corrupted data appropriately"""
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"corrupted data"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            with self.assertRaises(DownloadError):
                self.downloader.download_and_unzip("test.gz", 0, 100)
    
    
    def test_successful_download_and_decompress(self):
        """Test successful download and decompression of data"""
        test_data = b"test data"
        compressed_data = gzip.compress(test_data)
        
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = compressed_data
            mock_get.return_value = mock_response
            
            result = self.downloader.download_and_unzip("test.gz", 0, 100)
            
            # Verify correct headers were sent
            mock_get.assert_called_with(
                f"{BASE_URL}/test.gz",
                headers={"Range": "bytes=0-99"},
                timeout=(10, 30)
            )
            self.assertEqual(result, test_data)

    def test_download_network_failure(self):
        """Test retry and error handling for network failures"""
        with patch('requests.Session.get') as mock_get:
            mock_get.side_effect = requests.ConnectionError("Network error")
            
            with self.assertRaises(DownloadError) as context:
                self.downloader.download_and_unzip("test.gz", 0, 100)
            
            self.assertIn("Download failed", str(context.exception))

    def test_decompression_failure(self):
        """Test handling of corrupted gzip data"""
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"not gzipped data"
            mock_get.return_value = mock_response
            
            with self.assertRaises(DownloadError) as context:
                self.downloader.download_and_unzip("test.gz", 0, 100)
            
            self.assertIn("Decompression failed", str(context.exception))
            

    def test_partial_download_handling(self):
        """Test handling of partial or incomplete downloads"""
        test_url = "test.gz"
        expected_length = 100
        
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"partial"  # Not gzipped data
            mock_response.headers = {'Content-Length': str(expected_length)}
            mock_get.return_value = mock_response
            
            with self.assertRaises(DownloadError) as context:
                self.downloader.download_and_unzip(test_url, 0, expected_length)
            
            self.assertIn("Decompression failed", str(context.exception))


    def test_retry_mechanism_behavior(self):
        """Test the full retry mechanism behavior"""
        test_url = "test.gz"
        test_data = b"test data"
        compressed_data = gzip.compress(test_data)
        
        with patch('requests.Session.get') as mock_get:
            # Setup mock to fail twice then succeed
            mock_get.side_effect = [
                requests.ConnectionError("Network error"),
                requests.ConnectionError("Network error"),
                Mock(content=compressed_data)
            ]
            
            result = self.downloader.download_and_unzip(test_url, 0, 100)
            
            self.assertEqual(mock_get.call_count, 3)
            self.assertEqual(result, test_data)

    def test_concurrent_download_state(self):
        """Test handling multiple concurrent downloads"""
        test_urls = ["test1.gz", "test2.gz"]
        test_data = b"test data"
        compressed_data = gzip.compress(test_data)
        
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = compressed_data
            mock_get.return_value = mock_response
            
            # Simulate concurrent downloads
            import threading
            results = []
            threads = []
            
            def download_url(url):
                result = self.downloader.download_and_unzip(url, 0, 100)
                results.append(result)
            
            for url in test_urls:
                thread = threading.Thread(target=download_url, args=(url,))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
                
            self.assertEqual(len(results), 2)
            self.assertTrue(all(r == test_data for r in results))

    def test_large_file_resource_handling(self):
        """Test handling of large file downloads and resource constraints"""
        test_url = "large_file.gz"
        large_data = b"x" * (10 * 1024 * 1024)  # 10MB of data
        compressed_data = gzip.compress(large_data)
        
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.content = compressed_data
            mock_get.return_value = mock_response
            
            try:
                result = self.downloader.download_and_unzip(test_url, 0, len(compressed_data))
                self.assertEqual(len(result), len(large_data))
                
            except MemoryError:
                self.fail("Failed to handle large file download properly")

    

class TestCSVIndexReader(unittest.TestCase):
    
    def test_rejects_missing_required_fields(self):
        """Test rejects CSV rows with missing required fields"""
        test_content = [["incomplete_row", "file.gz", "0"]]  # Missing fields
        
        mock_file = Mock()
        mock_file.__iter__ = Mock(return_value=iter(test_content))
        
        with patch('builtins.open', Mock(return_value=mock_file)):
            reader = CSVIndexReader("test.csv")
            with reader:
                with self.assertRaises(ValueError):
                    next(reader)
                    
    
    def test_rejects_invalid_offset(self):
        """Test rejects CSV rows with invalid offset values"""
        test_content = [["url", "file.gz", "invalid", "1000", "1"]]
        
        mock_file = Mock()
        mock_file.__iter__ = Mock(return_value=iter(test_content))
        
        with patch('builtins.open', Mock(return_value=mock_file)):
            reader = CSVIndexReader("test.csv")
            with reader:
                with self.assertRaises(ValueError):
                    next(reader)
                    

    def test_rejects_invalid_length(self):
        """Test rejects CSV rows with invalid length values"""
        test_content = [["url", "file.gz", "0", "-100", "1"]]
        
        mock_file = Mock()
        mock_file.__iter__ = Mock(return_value=iter(test_content))
        
        with patch('builtins.open', Mock(return_value=mock_file)):
            reader = CSVIndexReader("test.csv")
            with reader:
                with self.assertRaises(ValueError):
                    next(reader)

    def test_handles_file_not_found(self):
        """Test handling of non-existent files"""
        with self.assertRaises(FileNotFoundError):
            with CSVIndexReader("nonexistent.csv"):
                pass
        
    def test_context_manager_cleanup(self):
        """Test that file is properly closed"""
        filename = "test.csv"
        with patch('builtins.open') as mock_open:
            mock_file = Mock()
            mock_csv_reader = Mock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock()
            mock_file.__iter__ = Mock(return_value=iter([]))  # Make it iterable
            mock_open.return_value = mock_file
            
            with CSVIndexReader(filename):
                pass
                
            mock_file.close.assert_called_once()
            
    def test_validate_index_structure(self):
        """Test validation of CSV index structure and content"""
        valid_data = "test.csv"
        test_content = [
            ["url 20240722120756", "file.gz", "0", "1000", "1"],  # Valid row
            ["url 20240722120756", "file.gz", "abc", "1000", "1"],  # Invalid offset
            ["url 20240722120756", "file.gz", "0", "-100", "1"],  # Invalid length
            ["incomplete_row", "file.gz", "0"],  # Missing fields
        ]
        
        with patch('builtins.open') as mock_open:
            mock_file = Mock()
            mock_file.__enter__.return_value = mock_file
            mock_file.__exit__.return_value = None
            mock_file.__iter__.return_value = iter(test_content)
            mock_open.return_value = mock_file
            
            reader = CSVIndexReader(valid_data)
            with reader:
                # Valid row should be readable
                row = next(reader)
                self.assertEqual(len(row), 5)
                
                # Invalid offset should raise ValueError
                with self.assertRaises(ValueError):
                    next(reader)
                    
                # Invalid length should raise ValueError
                with self.assertRaises(ValueError):
                    next(reader)
                    
                # Incomplete row should raise ValueError
                with self.assertRaises(ValueError):
                    next(reader)

