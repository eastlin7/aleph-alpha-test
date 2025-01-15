import unittest
import numpy as np
import base64
from unittest.mock import Mock, patch
import json
from tokenizer import Tokenizer, TokenizationStatus
from exceptions import TokenizationError, StorageError


class TestTokenizer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method"""
        with patch("tokenizer.AutoTokenizer") as mock_tokenizer:
            # Configure mock tokenizer
            mock_tokenizer.from_pretrained.return_value = Mock(
                pad_token_id=0,
                cls_token_id=101,
                sep_token_id=102,
                encode=Mock(return_value=[1, 2, 3, 4, 5]),
            )
            self.tokenizer = Tokenizer()

    def test_tokenize_with_chunks_empty_text(self):
        """Test tokenization with empty text"""
        result = self.tokenizer.tokenize_with_chunks("")
        self.assertEqual(result["status"], TokenizationStatus.FAILED)
        self.assertEqual(result["error"], "Empty or whitespace-only text")

    def test_tokenize_with_chunks_whitespace(self):
        """Test tokenization with whitespace-only text"""
        result = self.tokenizer.tokenize_with_chunks("   \n\t   ")
        self.assertEqual(result["status"], TokenizationStatus.FAILED)
        self.assertEqual(result["error"], "Empty or whitespace-only text")

    def test_tokenize_with_chunks_normal(self):
        """Test tokenization with normal text"""
        # Mock the tokenizer.encode to return predictable tokens
        self.tokenizer.tokenizer.encode.return_value = [1, 2, 3, 4, 5]

        text = "This is a sample text for testing tokenization."
        result = self.tokenizer.tokenize_with_chunks(text)

        self.assertEqual(result["status"], TokenizationStatus.SUCCESS)
        self.assertIn("chunks", result)
        self.assertTrue(len(result["chunks"]) > 0)

        chunk = result["chunks"][0]
        self.assertIn("input_ids", chunk)
        self.assertIn("attention_mask", chunk)
        self.assertIn("chunk_index", chunk)
        self.assertIn("original_length", chunk)

    def test_create_document_maintains_padding(self):
        """Test that stored documents maintain proper padding through the entire flow"""
        # Setup
        self.tokenizer.tokenizer.encode.return_value = [1, 2, 3]
        text = "Test text"
        metadata = {"timestamp": "2024-01-14T12:00:00"}

        document_data = self.tokenizer.create_document(text, metadata)

        first_chunk = document_data["chunks"][0]

        # Decode base64 strings back to arrays
        decoded_input_ids = np.frombuffer(
            base64.b64decode(first_chunk["input_ids"]), dtype=np.int32
        )
        decoded_attention_mask = np.frombuffer(
            base64.b64decode(first_chunk["attention_mask"]), dtype=np.int32
        )

        # Verify the chunk has proper padding
        self.assertEqual(len(decoded_input_ids), self.tokenizer.max_length)
        self.assertEqual(len(decoded_attention_mask), self.tokenizer.max_length)

    def test_create_document_success(self):
        """Test successful document storage"""
        # Mock tokenizer to return predictable values
        self.tokenizer.tokenizer.encode.return_value = [1, 2, 3, 4, 5]

        text = "Sample text for testing document storage."
        metadata = {"timestamp": "2024-01-14T12:00:00", "url": "https://example.com"}

        document_data = self.tokenizer.create_document(text, metadata)

        self.assertEqual(document_data["total_chunks"], 1)
        self.assertEqual(
            document_data["metadata"],
            {
                "timestamp": "2024-01-14T12:00:00",
                "url": "https://example.com",
                "stride": 256,
                "max_length": 512,
                "original_length": 5,
            },
        )
        
        self.assertRegex(document_data["chunks"][0]["input_ids"], r"^ZQAAAAEAAAACAAAAAwAAAAQAAAAFAAAAZgAAAA")
        self.assertRegex(document_data["chunks"][0]["attention_mask"], r"^AQAAAAEAAA")
        self.assertEqual(document_data["chunks"][0]["chunk_index"], 0)
        self.assertEqual(len(document_data["chunks"]), 1)
        

    def test_create_document_tokenization_failure(self):
        """Create document with tokenization failure"""
        with self.assertRaises(StorageError) as context:
            self.tokenizer.create_document("", {})
        self.assertIn(
            "Tokenization failed: Empty or whitespace-only text", str(context.exception)
        )

    def test_large_document_chunking(self):
        """Test tokenization of a large document"""
        # Mock tokenizer to return a long sequence
        self.tokenizer.tokenizer.encode.return_value = list(range(1000))

        long_text = "This is a test. " * 1000
        result = self.tokenizer.tokenize_with_chunks(long_text)

        self.assertEqual(result["status"], TokenizationStatus.SUCCESS)
        self.assertTrue(len(result["chunks"]) > 1)

        if len(result["chunks"]) > 1:
            chunk1 = result["chunks"][0]
            chunk2 = result["chunks"][1]
            self.assertEqual(chunk1["chunk_index"] + 1, chunk2["chunk_index"])


if __name__ == "__main__":
    unittest.main()
