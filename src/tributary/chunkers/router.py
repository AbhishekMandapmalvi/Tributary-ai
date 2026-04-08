"""Conditional routing — pick a chunker based on file type.

Usage:
    router = ChunkerRouter(
        default=FixedChunker(chunk_size=500),
        rules={
            ".pdf": RecursiveChunker(chunk_size=800),
            ".md": SentenceChunker(sentences_per_chunk=5),
        },
    )
    pipeline = Pipeline(..., chunker=router)
"""
from pathlib import Path
from tributary.chunkers.base import BaseChunker
from tributary.chunkers.models import ChunkResult
from collections.abc import Callable
import structlog

logger = structlog.get_logger(__name__)


class ChunkerRouter(BaseChunker):
    def __init__(
        self,
        default: BaseChunker,
        rules: dict[str, BaseChunker] | None = None,
        custom_rule: Callable[[str], BaseChunker | None] | None = None,
    ) -> None:
        self.default = default
        self.rules = {k.lower(): v for k, v in (rules or {}).items()}
        self.custom_rule = custom_rule

    def get_chunker(self, source_name: str) -> BaseChunker:
        """Resolve the chunker for a given file name."""
        # Custom rule takes priority
        if self.custom_rule:
            result = self.custom_rule(source_name)
            if result is not None:
                return result

        # Extension-based routing
        ext = Path(source_name).suffix.lower()
        if ext in self.rules:
            return self.rules[ext]

        return self.default

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        chunker = self.get_chunker(source_name)
        logger.debug("Routing to chunker", source_name=source_name, chunker=type(chunker).__name__)
        return chunker.chunk(text, source_name)
