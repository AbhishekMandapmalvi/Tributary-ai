from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
import asyncio


class PineconeDestination(BaseDestination):
    def __init__(self, index_name: str, api_key: str | None = None):
        from pinecone import Pinecone
        self.client = Pinecone(api_key=api_key)
        self.index = self.client.Index(index_name)

    async def store(self, results: list[EmbeddingResult]) -> None:
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
