"""Fan-out destination — sends embeddings to multiple destinations simultaneously.

Usage:
    multi = MultiDestination([
        JSONDestination("backup.jsonl"),
        QdrantDestination(collection_name="docs"),
    ])
    pipeline = Pipeline(..., destination=multi)
"""
from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
import asyncio


class MultiDestination(BaseDestination):
    def __init__(self, destinations: list[BaseDestination]) -> None:
        if not destinations:
            raise ValueError("MultiDestination requires at least one destination")
        self.destinations = destinations

    async def connect(self) -> None:
        await asyncio.gather(*[d.connect() for d in self.destinations])

    async def store(self, results: list[EmbeddingResult]) -> None:
        await asyncio.gather(*[d.store(results) for d in self.destinations])

    async def close(self) -> None:
        await asyncio.gather(*[d.close() for d in self.destinations])
