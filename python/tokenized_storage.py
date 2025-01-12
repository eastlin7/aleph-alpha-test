from transformers import AutoTokenizer
import numpy as np
import io
import json
import uuid
import base64
from storage import MinIOStorage
from prometheus_client import Counter
import os

tokenization_successes = Counter('worker_tokenization_successes_total', 'Successfully tokenized documents')
tokenization_failures = Counter('worker_tokenization_failures_total', 'Failed tokenization attempts')

class TokenizedStorage(MinIOStorage):
    def __init__(self):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
        self.bucket_name = os.getenv('MINIO_BUCKET_NAME', 'tokenized-docs')

    def store_document(self, text, metadata):
        try:
            doc_id = str(uuid.uuid4())
            
            tokens = self.tokenizer(
                text, 
                max_length=512,
                truncation=True,
                padding='max_length',
                return_tensors='np'
            )
            
            # Convert numpy arrays to base64 strings for JSON serialization
            token_data = {
                'input_ids': base64.b64encode(tokens['input_ids'].astype(np.int32).tobytes()).decode('utf-8'),
                'attention_mask': base64.b64encode(tokens['attention_mask'].astype(np.int32).tobytes()).decode('utf-8'),
                'metadata': {
                    'timestamp': metadata.get('timestamp'),
                    'url': metadata.get('url')
                }
            }
            
            json_data = json.dumps(token_data).encode()
            self.client.put_object(
                self.bucket_name,
                f"{doc_id}.bin",
                io.BytesIO(json_data),
                len(json_data)
            )
            tokenization_successes.inc()
            return True
        except Exception as e:
            print(f"Error storing tokenized document: {e}")
            tokenization_failures.inc()
            return False