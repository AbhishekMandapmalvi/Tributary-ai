from tributary.chunkers.models import ChunkResult
from tributary.chunkers.base import BaseChunker
from tributary.utils.lazy_import import lazy_import


class SentenceChunker(BaseChunker):
    def __init__(self, sentences_per_chunk: int = 5, overlap_sentences: int = 1):
        if overlap_sentences >= sentences_per_chunk:
            raise ValueError(
                f"overlap_sentences ({overlap_sentences}) must be less than sentences_per_chunk ({sentences_per_chunk})"
            )
        self.sentences_per_chunk = sentences_per_chunk
        self.overlap_sentences = overlap_sentences
        nltk = lazy_import("nltk")
        nltk.download('punkt_tab', quiet=True)
        self._sent_tokenize = nltk.tokenize.sent_tokenize

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        if not text:
            return []

        sentences = self._sent_tokenize(text)
        if not sentences:
            return []

        chunks: list[ChunkResult] = []
        step = self.sentences_per_chunk - self.overlap_sentences
        search_from = 0

        for i in range(0, len(sentences), step):
            group = sentences[i:i + self.sentences_per_chunk]
            chunk_text = " ".join(group)
            offset = text.find(group[0], search_from)
            end = text.find(group[-1], offset) + len(group[-1])
            chunk_text = text[offset:end]

            chunks.append(ChunkResult(
                text=chunk_text,
                source_name=source_name,
                chunk_index=len(chunks),
                start_char=offset,
                end_char=end,
            ))
            # Advance search_from past the non-overlapping sentences
            non_overlap = sentences[i:i + step]
            search_from = offset + len(non_overlap[0]) if len(non_overlap) == 1 else text.find(non_overlap[-1], offset) + len(non_overlap[-1])

        return chunks
