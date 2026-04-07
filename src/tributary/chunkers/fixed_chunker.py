from tributary.chunkers.models import ChunkResult
from tributary.chunkers.base import BaseChunker


class FixedChunker(BaseChunker):
    def __init__(self, chunk_size: int = 500, overlap: int = 50):
        if overlap >= chunk_size:
            raise ValueError(f"overlap ({overlap}) must be less than chunk_size ({chunk_size})")
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        if not text:
            return []

        chunks = []
        step = self.chunk_size - self.overlap
        start = 0
        chunk_index = 0

        while start < len(text):
            end = start + self.chunk_size
            chunk_text = text[start:end]
            chunks.append(ChunkResult(
                text=chunk_text,
                source_name=source_name,
                chunk_index=chunk_index,
                start_char=start,
                end_char=start + len(chunk_text),
            ))
            chunk_index += 1
            start += step

        return chunks
