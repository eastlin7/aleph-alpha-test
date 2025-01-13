from abc import ABC, abstractmethod
import json
import argparse
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server

from commoncrawl import (
    BASE_URL,
    CRAWL_PATH,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel
from processed_urls import ProcessedURLTracker
from dotenv import load_dotenv

documents_processed = Counter('batcher_documents_processed_total', 'Total number of documents processed')
documents_non_english = Counter('batcher_documents_filtered_non_english_total', 'Documents filtered due to non-English language')
documents_bad_status = Counter('batcher_documents_filtered_status_total', 'Documents filtered due to non-200 status')
documents_accepted = Counter('batcher_documents_accepted_total', 'Documents that passed all filters')
load_dotenv()

BATCH_SIZE = 5

batch_counter = Counter("batcher_batches", "Number of published batches")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()

def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
    url_tracker: ProcessedURLTracker,
) -> None:
    print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    
    for item in batch:
        url_tracker.mark_processed(item["surt_url"], item["timestamp"])
    batch_counter.inc()

def process_index(index, channel, downloader, url_tracker, batch_size):
    print("indexing")
    found_urls = []
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
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
                
                if url_tracker.is_processed(url, timestamp):
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
                
                continue

    if len(found_urls) > 0:
        publish_batch(channel, found_urls, url_tracker)



def main() -> None:
    args = parse_args()
    start_http_server(9000)
    url_tracker = ProcessedURLTracker()
    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, url_tracker, BATCH_SIZE)

if __name__ == "__main__":
    main()