import unittest
import numpy as np  
import base64
from unittest.mock import Mock, patch
import json
from tokenized_storage import TokenizedStorage, TokenizationStatus
from exceptions import TokenizationError, StorageError

class TestTokenizedStorage(unittest.TestCase):
    @patch('tokenized_storage.MinIOStorage')
    def setUp(self, mock_minio):
        """Set up test fixtures before each test method"""
        with patch('tokenized_storage.AutoTokenizer') as mock_tokenizer:
            # Configure mock tokenizer
            mock_tokenizer.from_pretrained.return_value = Mock(
                pad_token_id=0,
                cls_token_id=101,
                sep_token_id=102,
                encode=Mock(return_value=[1, 2, 3, 4, 5])
            )
            self.storage = TokenizedStorage()
            self.storage.client = Mock()
        
            
            
        
    def test_tokenize_with_chunks_empty_text(self):
        """Test tokenization with empty text"""
        result = self.storage.tokenize_with_chunks("")
        self.assertEqual(result["status"], TokenizationStatus.FAILED)
        self.assertEqual(result["error"], "Empty or whitespace-only text")
        
    def test_tokenize_with_chunks_whitespace(self):
        """Test tokenization with whitespace-only text"""
        result = self.storage.tokenize_with_chunks("   \n\t   ")
        self.assertEqual(result["status"], TokenizationStatus.FAILED)
        self.assertEqual(result["error"], "Empty or whitespace-only text")
        
    def test_tokenize_with_chunks_normal(self):
        """Test tokenization with normal text"""
        # Mock the tokenizer.encode to return predictable tokens
        self.storage.tokenizer.encode.return_value = [1, 2, 3, 4, 5]
        
        text = "This is a sample text for testing tokenization."
        result = self.storage.tokenize_with_chunks(text)
        
        self.assertEqual(result["status"], TokenizationStatus.SUCCESS)
        self.assertIn("chunks", result)
        self.assertTrue(len(result["chunks"]) > 0)
        
        chunk = result["chunks"][0]
        self.assertIn("input_ids", chunk)
        self.assertIn("attention_mask", chunk)
        self.assertIn("chunk_index", chunk)
        self.assertIn("original_length", chunk)
            

    def test_store_document_maintains_padding(self):
        """Test that stored documents maintain proper padding through the entire flow"""
        # Setup
        self.storage.tokenizer.encode.return_value = [1, 2, 3]
        text = "Test text"
        metadata = {"timestamp": "2024-01-14T12:00:00"}
        
        # Capture stored data
        captured_data = {}
        def capture_stored_data(bucket, file, data, length):
            captured_data['json'] = json.loads(data.getvalue().decode())
        
        self.storage.client.put_object.side_effect = capture_stored_data
        
        # Act
        self.storage.store_document(text, metadata)
        
        # Assert
        first_chunk = captured_data['json']["chunks"][0]
        
        # Decode base64 strings back to arrays
        decoded_input_ids = np.frombuffer(
            base64.b64decode(first_chunk["input_ids"]), 
            dtype=np.int32
        )
        decoded_attention_mask = np.frombuffer(
            base64.b64decode(first_chunk["attention_mask"]), 
            dtype=np.int32
        )
        
        # Verify the chunk has proper padding
        self.assertEqual(len(decoded_input_ids), self.storage.max_length)
        self.assertEqual(len(decoded_attention_mask), self.storage.max_length)

    def test_store_document_success(self):
        """Test successful document storage"""
        # Mock tokenizer to return predictable values
        self.storage.tokenizer.encode.return_value = [1, 2, 3, 4, 5]
        
        text = "Sample text for testing document storage."
        metadata = {
            "timestamp": "2024-01-14T12:00:00",
            "url": "https://example.com"
        }
        
        self.storage.client.put_object = Mock()
        
        try:
            result = self.storage.store_document(text, metadata)
            self.assertTrue(result)  # Should return True on success
            
            # Verify put_object was called
            self.storage.client.put_object.assert_called_once()
            
            # Get the call arguments
            args = self.storage.client.put_object.call_args[0]
            
            # Check basic call structure
            self.assertEqual(args[0], self.storage.bucket_name)
            self.assertTrue(isinstance(args[1], str))
            self.assertTrue(args[1].endswith('.json'))
            
            # Check the data being stored
            stored_data = json.loads(args[2].getvalue().decode())
            self.assertIn("chunks", stored_data)
            self.assertIn("total_chunks", stored_data)
            self.assertIn("metadata", stored_data)
            
        except Exception as e:
            self.fail(f"store_document raised unexpected exception: {str(e)}")
            
    def test_store_document_tokenization_failure(self):
        """Test document storage with tokenization failure"""
        with self.assertRaises(StorageError) as context:
            self.storage.store_document("", {})
        self.assertIn("Tokenization failed: Empty or whitespace-only text", str(context.exception))


    
    def test_large_document_chunking(self):
        """Test tokenization of a large document"""
        # Mock tokenizer to return a long sequence
        self.storage.tokenizer.encode.return_value = list(range(1000))
        
        long_text = "This is a test. " * 1000
        result = self.storage.tokenize_with_chunks(long_text)
        
        self.assertEqual(result["status"], TokenizationStatus.SUCCESS)
        self.assertTrue(len(result["chunks"]) > 1)
        
        if len(result["chunks"]) > 1:
            chunk1 = result["chunks"][0]
            chunk2 = result["chunks"][1]
            self.assertEqual(chunk1["chunk_index"] + 1, chunk2["chunk_index"])

if __name__ == '__main__':
    unittest.main()