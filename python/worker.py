import io
import json
from prometheus_client import start_http_server, Counter
import trafilatura
from warcio.archiveiterator import WARCIterator
import os

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from tokenized_storage import TokenizedStorage
from dotenv import load_dotenv
load_dotenv()

batches_received = Counter('worker_batches_received_total', 'Total number of batches received')
documents_processed = Counter('worker_documents_processed_total', 'Total number of documents processed')
warc_records_processed = Counter('worker_warc_records_processed_total', 'Total WARC records processed')
text_extraction_failures = Counter('worker_text_extraction_failures_total', 'Failed text extractions')
successful_extractions = Counter('worker_successful_extractions_total', 'Successful text extractions')
batch_counter = Counter("worker_batches", "Number of consumed batches")

def process_batch(storage, downloader: Downloader, ch, method, _properties, body):
    batches_received.inc()
    batch = json.loads(body)
    print("Received batch of size", len(body))
    
    for item in batch:
        documents_processed.inc()
        try:
            data = downloader.download_and_unzip(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            )
            for record in WARCIterator(io.BytesIO(data)):
                warc_records_processed.inc()
                if record.rec_type == "response":
                    try:
                        text = trafilatura.extract(record.content_stream().read())
                        if text is None:
                            text_extraction_failures.inc()
                        else:
                            successful_extractions.inc()
                            storage.store_document(text, item["metadata"])
                    except Exception:
                        text_extraction_failures.inc()
        except Exception as e:
            print(f"Error processing document: {e}")
            
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main() -> None:
    start_http_server(9001)
    storage = TokenizedStorage()
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            storage, downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()

if __name__ == "__main__":
    main()