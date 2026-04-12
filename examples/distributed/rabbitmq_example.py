"""Distributed pipeline using RabbitMQ as the message broker.

Prerequisites
-------------
- RabbitMQ running locally:
    docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
- OPENAI_API_KEY set in your environment.

Notes
-----
RabbitMQQueue requires an already-open aio_pika channel, so we create the
connection and channel here rather than using the get_queue() factory.
Messages that exceed max_retries are rejected without requeue (dropped) —
configure a dead-letter exchange on your RabbitMQ queue for real DLQ routing.
"""
import asyncio
import os

import aio_pika

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.rabbitmq_queue import RabbitMQQueue


async def main():
    url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()

        document_queue = RabbitMQQueue(
            channel, queue_name="tributary-docs", max_retries=3
        )
        chunk_queue = RabbitMQQueue(
            channel, queue_name="tributary-chunks", max_retries=3
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
