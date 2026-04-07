import pytest
from tributary.chunkers.fixed_chunker import FixedChunker


def test_basic_chunking():
    chunker = FixedChunker(chunk_size=500, overlap=0)
    text = "a" * 1000
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 2
    assert all(c.char_count == 500 for c in chunks)
    assert chunks[0].start_char == 0
    assert chunks[0].end_char == 500
    assert chunks[1].start_char == 500
    assert chunks[1].end_char == 1000


def test_overlap():
    chunker = FixedChunker(chunk_size=100, overlap=20)
    text = "a" * 200
    chunks = chunker.chunk(text, "test.txt")
    for i in range(len(chunks) - 1):
        overlap_start = chunks[i + 1].start_char
        overlap_end = chunks[i].end_char
        assert overlap_end - overlap_start == 20


def test_empty_text():
    chunker = FixedChunker(chunk_size=500, overlap=50)
    chunks = chunker.chunk("", "test.txt")
    assert chunks == []


def test_text_shorter_than_chunk_size():
    chunker = FixedChunker(chunk_size=500, overlap=50)
    text = "hello world"
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 1
    assert chunks[0].text == "hello world"
    assert chunks[0].start_char == 0
    assert chunks[0].end_char == len("hello world")
    assert chunks[0].chunk_index == 0


def test_overlap_equal_to_chunk_size_raises():
    with pytest.raises(ValueError, match="overlap .* must be less than chunk_size"):
        FixedChunker(chunk_size=100, overlap=100)


def test_overlap_greater_than_chunk_size_raises():
    with pytest.raises(ValueError, match="overlap .* must be less than chunk_size"):
        FixedChunker(chunk_size=100, overlap=150)
