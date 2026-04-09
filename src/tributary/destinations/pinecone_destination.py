from __future__ import annotations
from typing import Any
from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
from tributary.utils.lazy_import import lazy_import
import asyncio


class PineconeDestination(BaseDestination):
    def __init__(self, index_name: str, api_key: str | None = None):
        self._index_name = index_name
        self._api_key = api_key
        self.client: Any = None
        self.index: Any = None

    async def connect(self) -> None:
        if self.client is None:
            pinecone = lazy_import("pinecone")
            self.client = pinecone.Pinecone(api_key=self._api_key)
            self.index = self.client.Index(self._index_name)

    async def store(self, results: list[EmbeddingResult]) -> None:
        await self.connect()
        vectors = [
            (
                f"{result.source_name}#{result.chunk_index}",
                result.vector,
                {
                    "chunk_text": result.chunk_text,
                    "source_name": result.source_name,
                    "chunk_index": result.chunk_index,
                    "model_name": result.model_name,
                },
            )
            for result in results
        ]
        await asyncio.to_thread(self.index.upsert, vectors=vectors)  # type: ignore[arg-type]

    async def close(self) -> None:
        self.client = None
        self.index = None
