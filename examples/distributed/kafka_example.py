"""Distributed pipeline using Apache Kafka as the message broker.

Prerequisites
-------------
- Kafka running locally (zookeeper-less KRaft mode):
    docker run -d -p 9092:9092 --name kafka \\
        -e KAFKA_ENABLE_KRAFT=yes \\
        -e KAFKA_CFG_PROCESS_ROLES=broker,controller \\
        -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \\
        -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \\
        -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \\
        -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \\
        -e KAFKA_CFG_NODE_ID=1 \\
        bitnami/kafka:latest
- Two topics created in advance:
    kafka-topics.sh --create --topic tributary-docs --bootstrap-server localhost:9092
    kafka-topics.sh --create --topic tributary-chunks --bootstrap-server localhost:9092
- OPENAI_API_KEY set in your environment.

Notes
-----
Kafka's retry pattern is different from the other brokers: nack republishes
the message to the same topic with an updated retry_count header, then commits
the original offset. This means retry order is not guaranteed within a
partition — if ordering matters for your use case, pick a different backend.
Messages over max_retries are routed to a `{topic}-dlq` topic (create this
in advance if you want to keep DLQ messages).
"""
import asyncio
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.kafka_queue import KafkaQueue


async def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    doc_consumer = AIOKafkaConsumer(
        "tributary-docs",
        bootstrap_servers=bootstrap,
        group_id="tributary-extraction",
        enable_auto_commit=False,
    )
    chunk_consumer = AIOKafkaConsumer(
        "tributary-chunks",
        bootstrap_servers=bootstrap,
        group_id="tributary-embedding",
        enable_auto_commit=False,
    )

    await producer.start()
    await doc_consumer.start()
    await chunk_consumer.start()

    try:
        document_queue = KafkaQueue(
            producer=producer,
            consumer=doc_consumer,
            topic="tributary-docs",
            max_retries=3,
        )
        chunk_queue = KafkaQueue(
            producer=producer,
            consumer=chunk_consumer,
            topic="tributary-chunks",
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
    finally:
        await producer.stop()
        await doc_consumer.stop()
        await chunk_consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
