import pytest
from tributary.chunkers.recursive_chunker import RecursiveChunker


def test_text_within_chunk_size():
    chunker = RecursiveChunker(chunk_size=100, overlap=0)
    chunks = chunker.chunk("short text", "test.txt")
    assert len(chunks) == 1
    assert chunks[0].text == "short text"


def test_splits_on_paragraphs():
    chunker = RecursiveChunker(chunk_size=20, overlap=0)
    text = "First paragraph.\n\nSecond paragraph."
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 2
    assert chunks[0].text == "First paragraph."
    assert chunks[1].text == "Second paragraph."


def test_splits_on_newlines_when_paragraph_too_big():
    chunker = RecursiveChunker(chunk_size=30, overlap=0)
    text = "Line one.\nLine two.\nLine three."
    chunks = chunker.chunk(text, "test.txt")
    assert all(len(c.text) <= 30 for c in chunks)
    full_text = "".join(c.text for c in chunks)
    assert "Line one." in full_text
    assert "Line two." in full_text
    assert "Line three." in full_text


def test_splits_on_sentences():
    chunker = RecursiveChunker(chunk_size=30, overlap=0)
    text = "Hello world. This is a test. Goodbye."
    chunks = chunker.chunk(text, "test.txt")
    assert all(len(c.text) <= 30 for c in chunks)


def test_hard_cut_fallback():
    chunker = RecursiveChunker(chunk_size=10, overlap=0)
    text = "abcdefghijklmnopqrstuvwxyz"  # 26 chars, no separators
    chunks = chunker.chunk(text, "test.txt")
    assert all(len(c.text) <= 10 for c in chunks)
    assert "".join(c.text for c in chunks) == text


def test_empty_text():
    chunker = RecursiveChunker(chunk_size=100, overlap=0)
    chunks = chunker.chunk("", "test.txt")
    assert chunks == []


def test_overlap_invalid():
    with pytest.raises(ValueError, match="overlap .* must be less than chunk_size"):
        RecursiveChunker(chunk_size=100, overlap=100)


def test_chunk_results_have_correct_metadata():
    chunker = RecursiveChunker(chunk_size=20, overlap=0)
    text = "First paragraph.\n\nSecond paragraph."
    chunks = chunker.chunk(text, "doc.md")
    assert chunks[0].source_name == "doc.md"
    assert chunks[0].chunk_index == 0
    assert chunks[1].chunk_index == 1
    assert chunks[0].char_count == len(chunks[0].text)


def test_offsets_account_for_separators():
    chunker = RecursiveChunker(chunk_size=20, overlap=0)
    text = "First paragraph.\n\nSecond paragraph."
    chunks = chunker.chunk(text, "test.txt")
    # "First paragraph." is at 0..16, then "\n\n" is 2 chars
    assert chunks[0].start_char == 0
    assert chunks[0].end_char == 16
    # "Second paragraph." starts at 18
    assert chunks[1].start_char == 18
    assert chunks[1].end_char == 35
    # Verify by slicing the original text
    assert text[chunks[0].start_char:chunks[0].end_char] == chunks[0].text
    assert text[chunks[1].start_char:chunks[1].end_char] == chunks[1].text
