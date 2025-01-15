import io
import json
from prometheus_client import start_http_server, Counter
from scraper import Scraper
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from tokenizer import Tokenizer
from storage import MinIOStorage
from dotenv import load_dotenv
import uuid
import os
import sys
import logging

logger = logging.getLogger(__name__)

load_dotenv()

batches_received = Counter(
    "worker_batches_received_total", "Total number of batches received"
)
successful_extractions = Counter(
    "worker_successful_extractions_total", "Successful text extractions"
)
failed_extractions = Counter(
    "worker_failed_extractions_total", "Failed text extractions"
)
batch_counter = Counter("worker_batches", "Number of consumed batches")

def process_batch(tokenizer, storage, scraper, ch, method, _properties, body):
    batches_received.inc()
    batch = None

    try:
        batch = json.loads(body)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse batch JSON:")
        # Acknowledge the message so it's not requeued
        # In production we might use something like Sentry
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    logger.info("Received batch of size", len(body))

    for item in batch:
        try:
            for text in scraper.scrape(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            ):
                item["metadata"]["timestamp"] = item["timestamp"]
                document_data = tokenizer.create_document(text, item["metadata"])

                json_data = json.dumps(document_data).encode()

                storage.put_object(
                    f"{str(uuid.uuid4())}.json",
                    json_data,
                    len(json_data),
                )

                successful_extractions.inc()
        except:
            # Also could use something like Sentry in production
            failed_extractions.inc()
            logger.error("Error while processing document")

    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main() -> None:
    try:
        start_http_server(9001)
        tokenizer = Tokenizer()
        storage = MinIOStorage(
            os.getenv("MINIO_BUCKET_CRAWLED_DOCS_NAME"),
            os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
        )
        scraper = Scraper()
        channel = rabbitmq_channel()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=lambda ch, method, properties, body: process_batch(
                tokenizer, storage, scraper, ch, method, properties, body
            ),
        )
        channel.start_consuming()
    except Exception:
        logger.exception("Unhandled exception in main:")
        sys.exit(1)

if __name__ == "__main__":
    main()
