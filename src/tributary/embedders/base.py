from abc import ABC, abstractmethod
from tributary.embedders.models import EmbeddingResult


class BaseEmbedder(ABC):
    def __init__(self, model_name: str):
        self.model_name = model_name

    @abstractmethod
    async def embed(self, texts: list[str]) -> list[list[float]]:
        """
        Convert a list of texts into embedding vectors.

        Args:
            texts: List of text strings to embed.

        Returns:
            List of vectors, one per text.
        """
        pass

    async def embed_chunks(self, text_chunks: list[str], source_name: str) -> list[EmbeddingResult]:
        """
        Embed text chunks and wrap results with metadata.

        Args:
            text_chunks: List of text strings to embed.
            source_name: The name of the source document.

        Returns:
            List of EmbeddingResults with vectors and metadata.
        """
        vectors = await self.embed(text_chunks)
        return [
            EmbeddingResult(
                chunk_text=chunk,
                vector=vector,
                source_name=source_name,
                chunk_index=i,
                model_name=self.model_name,
            )
            for i, (chunk, vector) in enumerate(zip(text_chunks, vectors))
        ]

    async def embed_chunk(self, chunk_text: str, source_name: str, chunk_index: int) -> EmbeddingResult:
        """
        Embed a single text chunk. Convenience wrapper around embed.
        """
        vectors = await self.embed([chunk_text])
        return EmbeddingResult(
            chunk_text=chunk_text,
            vector=vectors[0],
            source_name=source_name,
            chunk_index=chunk_index,
            model_name=self.model_name,
        )
