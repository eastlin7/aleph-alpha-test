from transformers import AutoTokenizer
import numpy as np
from enum import Enum
import io
import json
import uuid
import base64
from storage import MinIOStorage
from prometheus_client import Counter, Histogram
import os
import warnings
import logging
from typing import Optional, List, Dict, Any, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
tokenization_successes = Counter('worker_tokenization_successes_total', 'Successfully tokenized documents')
tokenization_failures = Counter('worker_tokenization_failures_total', 'Failed tokenization attempts')
chunks_created = Counter('worker_chunks_created_total', 'Number of chunks created from documents')
padding_added = Counter('worker_padding_added_total', 'Number of times padding was needed')
padding_length = Histogram('worker_padding_length', 'Distribution of padding lengths added')
sequence_length = Histogram('worker_sequence_length', 'Distribution of sequence lengths before padding')

class TokenizationError(Exception):
    """Custom exception for tokenization errors"""
    pass

class TokenizationStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"

class TokenizedStorage(MinIOStorage):
    def __init__(self):
        """Initialize tokenizer with proper error handling"""
        try:
            super().__init__()
            self.tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
            self.bucket_name = os.getenv('MINIO_BUCKET_NAME', 'tokenized-docs')
            self.max_length = 512
            self.stride = 256
            
            # Validate tokenizer requirements
            self._validate_tokenizer()
            
        except Exception as e:
            logger.error(f"Tokenizer initialization failed: {e}")
            raise TokenizationError(f"Failed to initialize tokenizer: {e}")

    def _validate_tokenizer(self) -> None:
        """Validate tokenizer has required tokens"""
        required_tokens = ['pad_token', 'cls_token', 'sep_token']
        missing_tokens = [token for token in required_tokens 
                         if not hasattr(self.tokenizer, f'{token}_id')]
        
        if missing_tokens:
            raise TokenizationError(f"Tokenizer missing required tokens: {missing_tokens}")

    def pad_sequence(self, tokens: List[int]) -> Tuple[List[int], List[int]]:
        """Pad token sequence with proper validation and error handling"""
        try:
            if not tokens:
                raise ValueError("Empty token sequence")
                
            sequence_length.observe(len(tokens))
            padding_needed = self.max_length - len(tokens)
            
            if padding_needed > 0:
                padding_added.inc()
                padding_length.observe(padding_needed)
                padded_tokens = tokens + [self.tokenizer.pad_token_id] * padding_needed
            else:
                padded_tokens = tokens[:self.max_length]
            
            attention_mask = [1 if token != self.tokenizer.pad_token_id else 0 
                            for token in padded_tokens]
            
            # Validate outputs
            if len(padded_tokens) != self.max_length:
                raise ValueError(f"Incorrect padding length: {len(padded_tokens)}")
            
            if len(attention_mask) != self.max_length:
                raise ValueError(f"Incorrect attention mask length: {len(attention_mask)}")
                
            return padded_tokens, attention_mask
            
        except Exception as e:
            logger.error(f"Padding sequence failed: {e}")
            raise TokenizationError(f"Failed to pad sequence: {e}")

    def tokenize_with_chunks(self, text: str) -> Dict[str, Any]:
        """Tokenize text with proper error handling and validation"""
        if not text or not text.strip():
            logger.warning("Empty or whitespace-only text provided")
            return {
                "status": TokenizationStatus.FAILED,
                "error": "Empty or whitespace-only text"
            }

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                tokens = self.tokenizer.encode(text, add_special_tokens=False)
            
            chunks = []
            for i in range(0, len(tokens), self.max_length - self.stride):
                chunk_tokens = tokens[i:i + self.max_length - 2]
                chunk_tokens = [self.tokenizer.cls_token_id] + chunk_tokens + [self.tokenizer.sep_token_id]
                
                padded_tokens, attention_mask = self.pad_sequence(chunk_tokens)
                
                chunks.append({
                    'input_ids': padded_tokens,
                    'attention_mask': attention_mask,
                    'chunk_index': len(chunks),
                    'original_length': len(tokens)
                })
                chunks_created.inc()
            
            if not chunks:
                logger.warning("No chunks created from text")
                return {
                    "status": TokenizationStatus.FAILED,
                    "error": "No chunks created"
                }
            
            tokenization_successes.inc()
            return {
                "status": TokenizationStatus.SUCCESS,
                "chunks": chunks
            }
            
        except Exception as e:
            logger.error(f"Tokenization failed: {e}")
            tokenization_failures.inc()
            return {
                "status": TokenizationStatus.FAILED,
                "error": str(e)
            }

    def store_document(self, text: str, metadata: Dict[str, Any]) -> bool:
        """Store tokenized document with comprehensive error handling"""
        try:
            tokenization_result = self.tokenize_with_chunks(text)
            
            if tokenization_result["status"] != TokenizationStatus.SUCCESS:
                logger.error(f"Tokenization failed: {tokenization_result.get('error')}")
                return False
                
            doc_id = str(uuid.uuid4())
            document_data = {
                'chunks': [
                    {
                        'input_ids': base64.b64encode(
                            np.array(chunk['input_ids'], dtype=np.int32).tobytes()
                        ).decode('utf-8'),
                        'attention_mask': base64.b64encode(
                            np.array(chunk['attention_mask'], dtype=np.int32).tobytes()
                        ).decode('utf-8'),
                        'chunk_index': chunk['chunk_index']
                    }
                    for chunk in tokenization_result["chunks"]
                ],
                'total_chunks': len(tokenization_result["chunks"]),
                'metadata': {
                    'timestamp': metadata.get('timestamp'),
                    'url': metadata.get('url'),
                    'stride': self.stride,
                    'max_length': self.max_length,
                    'original_length': tokenization_result["chunks"][0]['original_length']
                }
            }
            
            json_data = json.dumps(document_data).encode()
            self.client.put_object(
                self.bucket_name,
                f"{doc_id}.json",
                io.BytesIO(json_data),
                len(json_data)
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to store document: {e}", exc_info=True)
            return False