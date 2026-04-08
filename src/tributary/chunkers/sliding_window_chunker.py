from tributary.chunkers.models import ChunkResult
from tributary.chunkers.base import BaseChunker


class SlidingWindowChunker(BaseChunker):
    def __init__(self, window_size: int = 500, step_size: int = 100):
        if step_size <= 0:
            raise ValueError(f"step_size ({step_size}) must be positive")
        if step_size > window_size:
            raise ValueError(f"step_size ({step_size}) must not exceed window_size ({window_size})")
        self.window_size = window_size
        self.step_size = step_size

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        if not text:
            return []

        chunks: list[ChunkResult] = []
        start = 0

        while start < len(text):
            chunk_text = text[start:start + self.window_size]
            chunks.append(ChunkResult(
                text=chunk_text,
                source_name=source_name,
                chunk_index=len(chunks),
                start_char=start,
                end_char=start + len(chunk_text),
            ))
            start += self.step_size

        return chunks