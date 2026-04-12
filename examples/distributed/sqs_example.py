"""Distributed pipeline using AWS SQS as the message broker.

Prerequisites
-------------
- Two SQS queues (document and chunk). Create them first in the AWS console or:
    aws sqs create-queue --queue-name tributary-docs
    aws sqs create-queue --queue-name tributary-chunks
- AWS credentials in the environment (or an IAM role on EC2/ECS/Lambda):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
- OPENAI_API_KEY set in your environment.

Notes
-----
SQS tracks delivery count natively via ApproximateReceiveCount — no separate
retry counter is needed. Messages that exceed max_retries should be routed to
an SQS dead-letter queue via the AWS console (set a redrive policy on the
queue). This example uses per-message nack → change_message_visibility.
"""
import asyncio
import os

import aioboto3

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.sqs_queue import SQSQueue


async def main():
    document_queue_url = os.environ["DOCUMENT_QUEUE_URL"]
    chunk_queue_url = os.environ["CHUNK_QUEUE_URL"]
    region = os.getenv("AWS_REGION", "us-east-1")

    session = aioboto3.Session()
    async with session.client("sqs", region_name=region) as sqs_client:
        document_queue = SQSQueue(
            sqs_client, queue_url=document_queue_url, max_retries=3
        )
        chunk_queue = SQSQueue(
            sqs_client, queue_url=chunk_queue_url, max_retries=3
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
