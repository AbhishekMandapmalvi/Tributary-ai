"""Distributed pipeline using Google Cloud Pub/Sub as the message broker.

Prerequisites
-------------
- Two Pub/Sub topics and two subscriptions (one pair per queue):
    gcloud pubsub topics create tributary-docs
    gcloud pubsub subscriptions create tributary-docs-sub \\
        --topic=tributary-docs
    gcloud pubsub topics create tributary-chunks
    gcloud pubsub subscriptions create tributary-chunks-sub \\
        --topic=tributary-chunks
- GOOGLE_APPLICATION_CREDENTIALS set to a service-account JSON path,
  or run on a GCP environment with an attached service account.
- GCP_PROJECT set in your environment.
- OPENAI_API_KEY set in your environment.

Notes
-----
Pub/Sub doesn't expose a native delivery counter, so retry_count is tracked
in message attributes by the queue backend. Messages over max_retries are
acked on the source subscription and republished to a `{topic}-dlq` topic.
Create the DLQ topic in advance if you want messages to survive there.
"""
import asyncio
import os

from gcloud.aio.pubsub import PublisherClient, SubscriberClient

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.pubsub_queue import PubSubQueue


async def main():
    project = os.environ["GCP_PROJECT"]

    publisher = PublisherClient()
    subscriber = SubscriberClient()

    document_queue = PubSubQueue(
        publisher=publisher,
        subscriber=subscriber,
        topic_path=f"projects/{project}/topics/tributary-docs",
        subscription_path=(
            f"projects/{project}/subscriptions/tributary-docs-sub"
        ),
        max_retries=3,
    )
    chunk_queue = PubSubQueue(
        publisher=publisher,
        subscriber=subscriber,
        topic_path=f"projects/{project}/topics/tributary-chunks",
        subscription_path=(
            f"projects/{project}/subscriptions/tributary-chunks-sub"
        ),
        max_retries=3,
    )

    pipeline = DistributedPipeline(
        source=LocalSource(directory="./docs"),
        document_queue=document_queue,
        chunk_queue=chunk_queue,
        extractor=TextExtractor(),
        chunker=FixedChunker(chunk_size=500, overlap=50),
        embedder=OpenAIEmbedder(api_key=os.getenv("OPENAI_API_KEY")),
        destination=JSONDestination("./output.jsonl"),
        n_extraction_workers=4,
        n_embedding_workers=2,
    )
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
