"""Distributed pipeline using Azure Service Bus as the message broker.

Prerequisites
-------------
- A Service Bus namespace with two queues (document + chunk). Create via
  the Azure portal or CLI:
    az servicebus queue create \\
        --resource-group <rg> \\
        --namespace-name <ns> \\
        --name tributary-docs
    az servicebus queue create \\
        --resource-group <rg> \\
        --namespace-name <ns> \\
        --name tributary-chunks
- SERVICE_BUS_CONNECTION_STRING set in your environment (from
  "Shared access policies" in the Azure portal).
- OPENAI_API_KEY set in your environment.

Notes
-----
Service Bus has first-class support for delivery tracking via delivery_count
and a built-in dead-letter subqueue. nack uses abandon_message under the
retry limit and dead_letter_message at/over the limit — messages land in
the queue's native `$DeadLetterQueue`, no separate topic needed.
"""
import asyncio
import os

from azure.servicebus.aio import ServiceBusClient

from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.extractors.text_extractor import TextExtractor
from tributary.sources.local_source import LocalSource
from tributary.workers import DistributedPipeline
from tributary.workers.backends.servicebus_queue import ServiceBusQueue


async def main():
    connection_string = os.environ["SERVICE_BUS_CONNECTION_STRING"]

    async with ServiceBusClient.from_connection_string(connection_string) as client:
        doc_sender = client.get_queue_sender(queue_name="tributary-docs")
        doc_receiver = client.get_queue_receiver(queue_name="tributary-docs")
        chunk_sender = client.get_queue_sender(queue_name="tributary-chunks")
        chunk_receiver = client.get_queue_receiver(queue_name="tributary-chunks")

        async with doc_sender, doc_receiver, chunk_sender, chunk_receiver:
            document_queue = ServiceBusQueue(
                sender=doc_sender, receiver=doc_receiver, max_retries=3
            )
            chunk_queue = ServiceBusQueue(
                sender=chunk_sender, receiver=chunk_receiver, max_retries=3
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
