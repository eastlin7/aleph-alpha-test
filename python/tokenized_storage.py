from transformers import AutoTokenizer
import numpy as np
import io
import json
import uuid
import base64
from storage import MinIOStorage
from prometheus_client import Counter, Histogram
import os
import warnings
import logging

tokenization_successes = Counter('worker_tokenization_successes_total', 'Successfully tokenized documents')
tokenization_failures = Counter('worker_tokenization_failures_total', 'Failed tokenization attempts')
chunks_created = Counter('worker_chunks_created_total', 'Number of chunks created from documents')
padding_added = Counter('worker_padding_added_total', 'Number of times padding was needed')
padding_length = Histogram('worker_padding_length', 'Distribution of padding lengths added')
sequence_length = Histogram('worker_sequence_length', 'Distribution of sequence lengths before padding')

logger = logging.getLogger(__name__)

class TokenizedStorage(MinIOStorage):
    def __init__(self):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
        self.bucket_name = os.getenv('MINIO_BUCKET_NAME', 'tokenized-docs')
        self.max_length = 512
        self.stride = 256 
        
        required_tokens = ['pad_token', 'cls_token', 'sep_token']
        for token in required_tokens:
            if not hasattr(self.tokenizer, f'{token}_id'):
                raise ValueError(f"Tokenizer missing required {token}")

    def pad_sequence(self, tokens: list) -> tuple[list, list]:
        """
        Pad token sequence to max_length and generate attention mask.
        
        Args:
            tokens: List of token IDs
            
        Returns:
            tuple: (padded_tokens, attention_mask)
        """
        try:
            sequence_length.observe(len(tokens))
            
            # Calculate required padding
            padding_needed = self.max_length - len(tokens)
            
            if padding_needed > 0:
                padding_added.inc()
                padding_length.observe(padding_needed)
                padded_tokens = tokens + [self.tokenizer.pad_token_id] * padding_needed
            else:
                padded_tokens = tokens[:self.max_length]
            
            # Generate attention mask (1 for real tokens, 0 for padding)
            attention_mask = [1 if token != self.tokenizer.pad_token_id else 0 
                            for token in padded_tokens]
            
            # Validate outputs
            if len(padded_tokens) != self.max_length:
                raise ValueError(f"Incorrect padding length: {len(padded_tokens)} != {self.max_length}")
            
            if len(attention_mask) != self.max_length:
                raise ValueError(f"Incorrect attention mask length: {len(attention_mask)} != {self.max_length}")
                
            return padded_tokens, attention_mask
            
        except Exception as e:
            logger.error(f"Error in padding sequence: {e}")
            tokenization_failures.inc()
            raise

    def tokenize_with_chunks(self, text):
        """
        Tokenize text into overlapping chunks with proper padding.
        """
        try:
            # Initial tokenization without truncation
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                tokens = self.tokenizer.encode(text, add_special_tokens=False)
            
            chunks = []
            
            # Process in chunks with overlap
            for i in range(0, len(tokens), self.max_length - self.stride):
                # Extract chunk and add special tokens
                chunk_tokens = tokens[i:i + self.max_length - 2]
                chunk_tokens = [self.tokenizer.cls_token_id] + chunk_tokens + [self.tokenizer.sep_token_id]
                
                # Pad sequence and get attention mask
                padded_tokens, attention_mask = self.pad_sequence(chunk_tokens)
                
                chunks.append({
                    'input_ids': padded_tokens,
                    'attention_mask': attention_mask,
                    'chunk_index': len(chunks),
                    'original_length': len(tokens)
                })
                chunks_created.inc()
            
            tokenization_successes.inc()
            return chunks
            
        except Exception as e:
            logger.error(f"Error in tokenization: {e}")
            tokenization_failures.inc()
            return None

    def store_document(self, text, metadata):
        """
        Store tokenized document with padding in MinIO.
        """
        try:
            doc_id = str(uuid.uuid4())
            chunks = self.tokenize_with_chunks(text)
            
            if not chunks:
                logger.error("Failed to tokenize document")
                return False
                
            document_data = {
                'chunks': [
                    {
                        'input_ids': base64.b64encode(np.array(chunk['input_ids'], dtype=np.int32).tobytes()).decode('utf-8'),
                        'attention_mask': base64.b64encode(np.array(chunk['attention_mask'], dtype=np.int32).tobytes()).decode('utf-8'),
                        'chunk_index': chunk['chunk_index']
                    }
                    for chunk in chunks
                ],
                'total_chunks': len(chunks),
                'metadata': {
                    'timestamp': metadata.get('timestamp'),
                    'url': metadata.get('url'),
                    'stride': self.stride,
                    'max_length': self.max_length,
                    'original_length': chunks[0]['original_length']
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
            logger.error(f"Error storing tokenized document: {e}")
            tokenization_failures.inc()
            return False