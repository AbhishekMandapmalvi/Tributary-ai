from tributary.chunkers.base import BaseChunker
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.chunkers.recursive_chunker import RecursiveChunker
from tributary.chunkers.sentence_chunker import SentenceChunker
from tributary.chunkers.token_based_chunker import TokenBasedChunker
from tributary.chunkers.sliding_window_chunker import SlidingWindowChunker

_REGISTRY = {
    "fixed": FixedChunker,
    "recursive": RecursiveChunker,
    "sentence": SentenceChunker,
    "token": TokenBasedChunker,
    "sliding_window": SlidingWindowChunker,
}


def get_chunker(strategy: str, **kwargs) -> BaseChunker:
    chunker_cls = _REGISTRY.get(strategy)
    if chunker_cls is None:
        raise ValueError(
            f"Unknown chunking strategy: '{strategy}'. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return chunker_cls(**kwargs)
