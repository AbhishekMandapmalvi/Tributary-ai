"""Pipeline middleware — inject custom logic between stages.

Usage:
    hooks = PipelineHooks()

    @hooks.after_extract
    def log_extraction(extraction, source_name):
        print(f"Extracted {extraction.char_count} chars from {source_name}")
        return extraction  # return None to skip this document

    @hooks.after_chunk
    def filter_short_chunks(chunks, source_name):
        return [c for c in chunks if c.char_count > 50]

    pipeline = Pipeline(..., hooks=hooks)
"""
from __future__ import annotations
from collections.abc import Callable
from tributary.extractors.models import ExtractionResult
from tributary.chunkers.models import ChunkResult
from tributary.embedders.models import EmbeddingResult


class PipelineHooks:
    def __init__(self) -> None:
        self._after_extract: list[Callable] = []
        self._after_chunk: list[Callable] = []
        self._before_embed: list[Callable] = []
        self._after_embed: list[Callable] = []

    def after_extract(self, fn: Callable) -> Callable:
        """Register a hook that runs after text extraction.

        fn(extraction: ExtractionResult, source_name: str) -> ExtractionResult | None
        Return the (possibly modified) result, or None to skip this document.
        """
        self._after_extract.append(fn)
        return fn

    def after_chunk(self, fn: Callable) -> Callable:
        """Register a hook that runs after chunking.

        fn(chunks: list[ChunkResult], source_name: str) -> list[ChunkResult]
        Return the (possibly filtered/modified) chunks list.
        """
        self._after_chunk.append(fn)
        return fn

    def before_embed(self, fn: Callable) -> Callable:
        """Register a hook that runs before embedding a batch.

        fn(texts: list[str], source_name: str) -> list[str]
        Return the (possibly transformed) texts list.
        """
        self._before_embed.append(fn)
        return fn

    def after_embed(self, fn: Callable) -> Callable:
        """Register a hook that runs after embedding, before storage.

        fn(embeddings: list[EmbeddingResult], source_name: str) -> list[EmbeddingResult]
        Return the (possibly filtered/modified) embeddings list.
        """
        self._after_embed.append(fn)
        return fn

    def run_after_extract(self, extraction: ExtractionResult, source_name: str) -> ExtractionResult | None:
        for hook in self._after_extract:
            extraction = hook(extraction, source_name)
            if extraction is None:
                return None
        return extraction

    def run_after_chunk(self, chunks: list[ChunkResult], source_name: str) -> list[ChunkResult]:
        for hook in self._after_chunk:
            chunks = hook(chunks, source_name)
        return chunks

    def run_before_embed(self, texts: list[str], source_name: str) -> list[str]:
        for hook in self._before_embed:
            texts = hook(texts, source_name)
        return texts

    def run_after_embed(self, embeddings: list[EmbeddingResult], source_name: str) -> list[EmbeddingResult]:
        for hook in self._after_embed:
            embeddings = hook(embeddings, source_name)
        return embeddings
