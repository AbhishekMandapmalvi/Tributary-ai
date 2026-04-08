from tributary.chunkers.models import ChunkResult
from tributary.chunkers.base import BaseChunker
from collections.abc import Callable

TIKTOKEN_ENCODINGS = {"cl100k_base", "o200k_base", "p50k_base", "r50k_base"}


def _load_tokenizer(tokenizer: str) -> tuple[Callable[[str], list[int]], Callable[[list[int]], str]]:
    if tokenizer in TIKTOKEN_ENCODINGS:
        import tiktoken
        enc = tiktoken.get_encoding(tokenizer)
        return enc.encode, enc.decode

    try:
        from transformers import AutoTokenizer
        hf_tokenizer = AutoTokenizer.from_pretrained(tokenizer)
        return (
            lambda text: hf_tokenizer.encode(text, add_special_tokens=False),
            lambda tokens: hf_tokenizer.decode(tokens, skip_special_tokens=True),
        )
    except Exception:
        pass

    raise ValueError(
        f"Unknown tokenizer: '{tokenizer}'. "
        f"Use a tiktoken encoding ({', '.join(sorted(TIKTOKEN_ENCODINGS))}) "
        f"or a Hugging Face model name (requires transformers installed)."
    )


class TokenBasedChunker(BaseChunker):
    def __init__(
        self,
        chunk_size: int = 500,
        overlap: int = 50,
        tokenizer: str | None = None,
        encode_fn: Callable[[str], list[int]] | None = None,
        decode_fn: Callable[[list[int]], str] | None = None,
    ):
        if overlap >= chunk_size:
            raise ValueError(f"overlap ({overlap}) must be less than chunk_size ({chunk_size})")
        if (encode_fn is None) != (decode_fn is None):
            raise ValueError("encode_fn and decode_fn must both be provided, or neither")
        if encode_fn and tokenizer:
            raise ValueError("Provide either tokenizer or encode_fn/decode_fn, not both")

        self.chunk_size = chunk_size
        self.overlap = overlap

        if encode_fn and decode_fn:
            self.encode_fn = encode_fn
            self.decode_fn = decode_fn
        else:
            self.encode_fn, self.decode_fn = _load_tokenizer(tokenizer or "cl100k_base")

    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        if not text:
            return []

        tokens = self.encode_fn(text)
        if not tokens:
            return []

        chunks: list[ChunkResult] = []
        step = self.chunk_size - self.overlap
        start = 0
        char_offset = 0

        while start < len(tokens):
            token_slice = tokens[start:start + self.chunk_size]
            chunk_text = self.decode_fn(token_slice)

            offset = text.find(chunk_text, char_offset)
            if offset == -1:
                offset = char_offset

            chunks.append(ChunkResult(
                text=chunk_text,
                source_name=source_name,
                chunk_index=len(chunks),
                start_char=offset,
                end_char=offset + len(chunk_text),
            ))

            if start + self.chunk_size >= len(tokens):
                break
            char_offset = offset + len(self.decode_fn(tokens[start:start + step]))
            start += step

        return chunks
