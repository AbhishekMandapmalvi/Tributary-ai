from tributary.chunkers.models import ChunkResult
from tributary.chunkers.base import BaseChunker

DEFAULT_SEPARATORS = ["\n\n", "\n", ". ", " "]


class RecursiveChunker(BaseChunker):
    def __init__(self, chunk_size: int = 500, overlap: int = 50, separators: list[str] | None = None):
        if overlap >= chunk_size:
            raise ValueError(f"overlap ({overlap}) must be less than chunk_size ({chunk_size})")
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.separators = separators or DEFAULT_SEPARATORS

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        if not text:
            return []

        pieces = self._split_text(text, self.separators)

        chunks = []
        search_from = 0
        for i, piece in enumerate(pieces):
            offset = text.find(piece, search_from)
            chunks.append(ChunkResult(
                text=piece,
                source_name=source_name,
                chunk_index=i,
                start_char=offset,
                end_char=offset + len(piece),
            ))
            search_from = offset + len(piece)

        return chunks

    def _split_text(self, text: str, separators: list[str]) -> list[str]:
        if len(text) <= self.chunk_size:
            return [text]

        if not separators:
            # Base case: hard-cut like fixed chunker
            pieces = []
            step = self.chunk_size - self.overlap
            start = 0
            while start < len(text):
                pieces.append(text[start:start + self.chunk_size])
                start += step
            return pieces

        separator = separators[0]
        remaining_separators = separators[1:]
        splits = text.split(separator)

        pieces = []
        current = ""

        for split in splits:
            # What would the combined text look like?
            candidate = current + separator + split if current else split

            if len(candidate) <= self.chunk_size:
                current = candidate
            else:
                # Flush current if it has content
                if current:
                    pieces.append(current)

                # This split alone might be too big — recurse with finer separators
                if len(split) > self.chunk_size:
                    pieces.extend(self._split_text(split, remaining_separators))
                else:
                    current = split

        if current:
            pieces.append(current)

        return pieces
