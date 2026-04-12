import pytest
from tributary.chunkers.sliding_window_chunker import SlidingWindowChunker


def test_basic_sliding():
    chunker = SlidingWindowChunker(window_size=500, step_size=100)
    text = "a" * 600
    chunks = chunker.chunk(text, "test.txt")
    # Windows at 0, 100, 200, 300, 400, 500 → 6 windows
    assert len(chunks) == 6
    assert chunks[0].start_char == 0
    assert chunks[1].start_char == 100
    assert chunks[0].char_count == 500
    assert chunks[-1].char_count == 100


def test_overlap_between_windows():
    chunker = SlidingWindowChunker(window_size=10, step_size=3)
    text = "abcdefghijklmnop"  # 16 chars
    chunks = chunker.chunk(text, "test.txt")
    # Consecutive windows should overlap by window_size - step_size = 7
    for i in range(len(chunks) - 1):
        overlap = chunks[i].end_char - chunks[i + 1].start_char
        assert overlap == 7 or chunks[i].end_char <= chunks[i + 1].start_char + len(chunks[i + 1].text)


def test_step_equals_window_no_overlap():
    chunker = SlidingWindowChunker(window_size=5, step_size=5)
    text = "abcdefghij"  # 10 chars
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 2
    assert chunks[0].text == "abcde"
    assert chunks[1].text == "fghij"


def test_text_shorter_than_window():
    chunker = SlidingWindowChunker(window_size=100, step_size=50)
    chunks = chunker.chunk("hello", "test.txt")
    assert len(chunks) == 1
    assert chunks[0].text == "hello"


def test_empty_text():
    chunker = SlidingWindowChunker(window_size=100, step_size=50)
    assert chunker.chunk("", "test.txt") == []


def test_zero_step_raises():
    with pytest.raises(ValueError, match="step_size .* must be positive"):
        SlidingWindowChunker(window_size=100, step_size=0)


def test_step_exceeds_window_raises():
    with pytest.raises(ValueError, match="step_size .* must not exceed window_size"):
        SlidingWindowChunker(window_size=100, step_size=200)


def test_offsets_map_to_original_text():
    chunker = SlidingWindowChunker(window_size=10, step_size=4)
    text = "The quick brown fox jumps"
    chunks = chunker.chunk(text, "test.txt")
    for c in chunks:
        assert text[c.start_char:c.end_char] == c.text
