from __future__ import annotations
from typing import Any
from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
from tributary.utils.lazy_import import lazy_import
import hashlib


class QdrantDestination(BaseDestination):
    def __init__(self, collection_name: str, url: str = "http://localhost:6333", api_key: str | None = None):
        self.collection_name = collection_name
        self._url = url
        self._api_key = api_key
        self.client: Any = None
        self.models: Any = None

    async def connect(self) -> None:
        if self.client is None:
            qdrant_client = lazy_import("qdrant_client", pip_name="qdrant-client")
            self.client = qdrant_client.AsyncQdrantClient(url=self._url, api_key=self._api_key)
            self.models = qdrant_client.models

    async def store(self, results: list[EmbeddingResult]) -> None:
        await self.connect()
        points = [
            self.models.PointStruct(
                id=self._make_id(result.source_name, result.chunk_index),
                vector=result.vector,
                payload={
                    "chunk_text": result.chunk_text,
                    "source_name": result.source_name,
                    "chunk_index": result.chunk_index,
                    "model_name": result.model_name,
                },
            )
            for result in results
        ]
        await self.client.upsert(collection_name=self.collection_name, points=points)

    async def close(self) -> None:
        if self.client:
            await self.client.close()
            self.client = None

    @staticmethod
    def _make_id(source_name: str, chunk_index: int) -> int:
        key = f"{source_name}#{chunk_index}"
        return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16)
