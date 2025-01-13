import io
import json
from prometheus_client import start_http_server, Counter
import trafilatura
from warcio.archiveiterator import WARCIterator
from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from tokenized_storage import TokenizedStorage
from dotenv import load_dotenv

load_dotenv()

batches_received = Counter(
    "worker_batches_received_total", "Total number of batches received"
)
documents_processed = Counter(
    "worker_documents_processed_total", "Total number of documents processed"
)
warc_records_processed = Counter(
    "worker_warc_records_processed_total", "Total WARC records processed"
)
text_extraction_failures = Counter(
    "worker_text_extraction_failures_total", "Failed text extractions"
)
successful_extractions = Counter(
    "worker_successful_extractions_total", "Successful text extractions"
)
batch_counter = Counter("worker_batches", "Number of consumed batches")


def process_batch(storage, downloader: Downloader, ch, method, _properties, body):
    batches_received.inc()
    batch = json.loads(body) # TODO: This can fail, is fine or no
    print("Received batch of size", len(body)) # TODO: Clean up all prints?

    for item in batch:
        try:
            data = downloader.download_and_unzip(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            ) # TODO: This can fail, thats ok
            documents_processed.inc() # TODO: Think if this is good spot
            for record in WARCIterator(io.BytesIO(data)):
                warc_records_processed.inc()
                if record.rec_type == "response":
                    try:
                        text = trafilatura.extract(record.content_stream().read())
                        if text is None:
                            text_extraction_failures.inc()
                        else:
                            item["metadata"]["timestamp"] = item["timestamp"]
                            storage.store_document(text, item["metadata"])
                            successful_extractions.inc()
                    except Exception:
                        # TODO: DUMB WHY
                        text_extraction_failures.inc()
        except Exception as e:
            # TODO: How should we do error handling here
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