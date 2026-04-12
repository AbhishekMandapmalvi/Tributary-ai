"""Distributed pipeline using Redis as the message broker.

Prerequisites
-------------
- Redis running locally:
    docker run -d -p 6379:6379 redis:7
- OPENAI_API_KEY set in your environment (or swap in CustomEmbedder).

How it scales
-------------
Run this script on multiple machines pointing at the same Redis instance and
each process becomes another pool of workers drawing from the shared queues.
SIGINT/SIGTERM triggers a clean drain — the producer stops publishing,
workers finish their in-flight messages, then the process exits.
"""
import asyncio
import os

import redis.asyncio as aioredis

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.redis_queue import RedisQueue


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    document_queue = RedisQueue(
        redis_client, queue_name="tributary:docs", max_retries=3
    )
    chunk_queue = RedisQueue(
        redis_client, queue_name="tributary:chunks", max_retries=3
    )

    pipeline = DistributedPipeline(
        source=LocalSource(directory="./docs", extensions=[".txt", ".md"]),
        document_queue=document_queue,
        chunk_queue=chunk_queue,
        extractor=TextExtractor(),
        chunker=FixedChunker(chunk_size=500, overlap=50),
        embedder=OpenAIEmbedder(api_key=os.getenv("OPENAI_API_KEY")),
        destination=JSONDestination("./output.jsonl"),
        n_extraction_workers=4,
        n_embedding_workers=2,
        poll_timeout=1.0,
    )
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
