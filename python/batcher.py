import json
import argparse
import logging
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server
import sys
from commoncrawl import (
    BASE_URL,
    CRAWL_PATH,
    CCDownloader,
    CSVIndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel
from processed_url_tracker import ProcessedURLTracker
from dotenv import load_dotenv
from exceptions import PublishError

MAX_BATCH_SIZE = 15  # or whatever limit makes sense for your use case

logger = logging.getLogger(__name__)

documents_processed = Counter(
    "batcher_documents_processed_total", "Total number of documents processed"
)
documents_non_english = Counter(
    "batcher_documents_filtered_non_english_total",
    "Documents filtered due to non-English language",
)

documents_invalid_json = Counter(
    "batcher_documents_invalid_json_total", "Documents with invalid JSON metadata"
)

documents_bad_status = Counter(
    "batcher_documents_filtered_status_total",
    "Documents filtered due to non-200 status",
)
documents_accepted = Counter(
    "batcher_documents_accepted_total", "Documents that passed all filters"
)
load_dotenv()

BATCH_SIZE = 5

batch_counter = Counter("batcher_batches", "Number of published batches")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()


def _validate_batch(batch: Sequence[Mapping[str, Any]]) -> bool:
    """Validate batch size and content"""
    if not batch:
        logger.warning("Received empty batch to publish")
        return False
        
    if len(batch) > MAX_BATCH_SIZE:
        raise ValueError(f"Batch size {len(batch)} exceeds maximum {MAX_BATCH_SIZE}")
    
    return True

def _publish_to_queue(channel: MessageQueueChannel, batch: Sequence[Mapping[str, Any]]) -> None:
    """Publish batch to message queue"""
    try:
        serialized_batch = json.dumps(batch)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=serialized_batch
        )
    except Exception as e:
        logger.error(f"Failed to publish batch: {e}")
        raise PublishError(f"Message queue publish failed: {e}")

def _track_processed_urls(url_tracker: ProcessedURLTracker, batch: Sequence[Mapping[str, Any]]) -> None:
    """Mark URLs as processed in tracker"""
    for item in batch:
        url_tracker.mark_processed(item["surt_url"], item["timestamp"])

def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
    url_tracker: ProcessedURLTracker,
) -> None:
    """Publish batch and track URLs"""
    if not _validate_batch(batch):
        return
        
    logger.info(f"Publishing batch of {len(batch)} items")
    
    _publish_to_queue(channel, batch)
    _track_processed_urls(url_tracker, batch)
    
    batch_counter.inc()
    logger.info(f"Successfully published batch of {len(batch)} items")


def process_index(index, channel, downloader, url_tracker, batch_size):
    found_urls = []
    for cdx_chunk in index:
        data = downloader.download_and_unzip(  # TODO: Break out, deal exception maybe
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")

        for line in data.split("\n"):
            if line == "":
                continue
            documents_processed.inc()
            values = line.split(" ")
            try:
                metadata = json.loads("".join(values[2:]))

                if "languages" not in metadata or "eng" not in metadata["languages"]:
                    documents_non_english.inc()
                    continue

                if metadata["status"] != "200":
                    documents_bad_status.inc()
                    continue

                url = values[0]
                timestamp = values[1]

                if url_tracker.is_processed(url, timestamp): # TODO: Error handle the is_processed
                    continue

                documents_accepted.inc()
                found_urls.append(
                    {
                        "surt_url": url,
                        "timestamp": timestamp,
                        "metadata": metadata,
                    }
                )

                if len(found_urls) >= batch_size:
                    publish_batch(channel, found_urls, url_tracker)
                    found_urls = []

            except json.JSONDecodeError as e:
                logger.warning("failed JSON decode, continuing ", exc_info=1)
                documents_invalid_json.inc()
                continue

    if len(found_urls) > 0:
        publish_batch(channel, found_urls, url_tracker)


def main() -> None:
    try:
        args = parse_args()
        start_http_server(9000)
        url_tracker = ProcessedURLTracker()
        channel = RabbitMQChannel()
        downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")

        with CSVIndexReader(args.cluster_idx_filename) as index_reader:
            process_index(index_reader, channel, downloader, url_tracker, BATCH_SIZE)

    except Exception as e:
        logger.exception("Unhandled exception in main:")
        sys.exit(1)

if __name__ == "__main__":
    main()
