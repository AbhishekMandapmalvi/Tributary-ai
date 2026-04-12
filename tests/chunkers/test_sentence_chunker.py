import pytest
from tributary.chunkers.sentence_chunker import SentenceChunker


def test_basic_grouping():
    chunker = SentenceChunker(sentences_per_chunk=2, overlap_sentences=0)
    text = "First sentence. Second sentence. Third sentence. Fourth sentence."
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 2
    assert "First sentence" in chunks[0].text
    assert "Second sentence" in chunks[0].text
    assert "Third sentence" in chunks[1].text
    assert "Fourth sentence" in chunks[1].text


def test_overlap_sentences():
    chunker = SentenceChunker(sentences_per_chunk=3, overlap_sentences=1)
    text = "One. Two. Three. Four. Five."
    chunks = chunker.chunk(text, "test.txt")
    # With 3 per chunk and 1 overlap, step is 2
    # Chunk 0: One. Two. Three.
    # Chunk 1: Three. Four. Five.
    assert len(chunks) >= 2
    # The overlap sentence "Three." should appear in both chunks
    assert "Three" in chunks[0].text
    assert "Three" in chunks[1].text


def test_abbreviations_not_split():
    chunker = SentenceChunker(sentences_per_chunk=2, overlap_sentences=0)
    text = "Dr. Smith went to Washington. He arrived at 3.14 p.m."
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 1
    assert "Dr. Smith" in chunks[0].text


def test_empty_text():
    chunker = SentenceChunker(sentences_per_chunk=3, overlap_sentences=0)
    chunks = chunker.chunk("", "test.txt")
    assert chunks == []


def test_single_sentence():
    chunker = SentenceChunker(sentences_per_chunk=3, overlap_sentences=0)
    chunks = chunker.chunk("Just one sentence.", "test.txt")
    assert len(chunks) == 1
    assert chunks[0].text == "Just one sentence."


def test_offsets_map_to_original_text():
    chunker = SentenceChunker(sentences_per_chunk=2, overlap_sentences=0)
    text = "First sentence. Second sentence. Third sentence."
    chunks = chunker.chunk(text, "test.txt")
    for c in chunks:
        assert text[c.start_char:c.end_char] == c.text


def test_invalid_overlap_raises():
    with pytest.raises(ValueError, match="overlap_sentences .* must be less than sentences_per_chunk"):
        SentenceChunker(sentences_per_chunk=3, overlap_sentences=3)
