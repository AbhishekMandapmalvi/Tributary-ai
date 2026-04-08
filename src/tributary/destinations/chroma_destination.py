from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
import asyncio


class ChromaDestination(BaseDestination):
    def __init__(self, collection_name: str, persist_path: str | None = None):
        import chromadb
        if persist_path:
            self.client = chromadb.PersistentClient(path=persist_path)
        else:
            self.client = chromadb.Client()
        self.collection = self.client.get_or_create_collection(name=collection_name)

    async def store(self, results: list[EmbeddingResult]) -> None:
        if not results:
            return
        ids = [f"{r.source_name}#{r.chunk_index}" for r in results]
        embeddings = [r.vector for r in results]
        documents = [r.chunk_text for r in results]
        metadatas = [
            {
                "source_name": r.source_name,
                "chunk_index": r.chunk_index,
                "model_name": r.model_name,
            }
            for r in results
        ]
        await asyncio.to_thread(
            self.collection.upsert,
            ids=ids,
            embeddings=embeddings,  # type: ignore[arg-type]
            documents=documents,
            metadatas=metadatas,  # type: ignore[arg-type]
        )
