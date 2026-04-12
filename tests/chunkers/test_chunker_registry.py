import pytest
from tributary.chunkers import get_chunker
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.chunkers.recursive_chunker import RecursiveChunker
from tributary.chunkers.sentence_chunker import SentenceChunker
from tributary.chunkers.token_based_chunker import TokenBasedChunker
from tributary.chunkers.sliding_window_chunker import SlidingWindowChunker


def test_fixed():
    assert isinstance(get_chunker("fixed"), FixedChunker)


def test_recursive():
    assert isinstance(get_chunker("recursive"), RecursiveChunker)


def test_sentence():
    assert isinstance(get_chunker("sentence"), SentenceChunker)


def test_token():
    assert isinstance(get_chunker("token"), TokenBasedChunker)


def test_sliding_window():
    assert isinstance(get_chunker("sliding_window"), SlidingWindowChunker)


def test_kwargs_passed_through():
    chunker = get_chunker("fixed", chunk_size=200, overlap=20)
    assert chunker.chunk_size == 200
    assert chunker.overlap == 20


def test_unknown_strategy_raises():
    with pytest.raises(ValueError, match="Unknown chunking strategy"):
        get_chunker("nonexistent")
