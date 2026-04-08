from abc import ABC, abstractmethod
from collections import OrderedDict
from tributary.embedders.models import EmbeddingResult
import hashlib


class BaseEmbedder(ABC):
    def __init__(self, model_name: str, max_cache_size: int = 1024):
        self.model_name = model_name
        self._cache: OrderedDict[str, list[float]] = OrderedDict()
        self._max_cache_size = max_cache_size

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
        Uses an LRU cache to avoid re-embedding duplicate texts.
        """
        hashes = [hashlib.sha256(t.encode()).hexdigest() for t in text_chunks]

        # Separate cached from uncached
        cached_vectors: dict[int, list[float]] = {}
        uncached: list[tuple[int, str]] = []

        for i, h in enumerate(hashes):
            if h in self._cache:
                self._cache.move_to_end(h)
                cached_vectors[i] = self._cache[h]
            else:
                uncached.append((i, text_chunks[i]))

        # Embed only uncached texts
        if uncached:
            uncached_indices, uncached_texts = zip(*uncached)
            fresh_vectors = await self.embed(list(uncached_texts))

            for idx, vector in zip(uncached_indices, fresh_vectors):
                h = hashes[idx]
                self._cache[h] = vector
                self._cache.move_to_end(h)
                if len(self._cache) > self._max_cache_size:
                    self._cache.popitem(last=False)
                cached_vectors[idx] = vector

        # Build results in original order
        return [
            EmbeddingResult(
                chunk_text=text_chunks[i],
                vector=cached_vectors[i],
                source_name=source_name,
                chunk_index=i,
                model_name=self.model_name,
            )
            for i in range(len(text_chunks))
        ]

    async def embed_chunk(self, chunk_text: str, source_name: str, chunk_index: int) -> EmbeddingResult:
        """
        Embed a single text chunk. Convenience wrapper around embed_chunks.
        """
        results = await self.embed_chunks([chunk_text], source_name)
        result = results[0]
        result.chunk_index = chunk_index
        return result
