import pytest
from tributary.chunkers.token_based_chunker import TokenBasedChunker


def test_basic_token_chunking():
    chunker = TokenBasedChunker(chunk_size=5, overlap=0)
    text = "The quick brown fox jumps over the lazy dog"
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) >= 2
    assert all(c.source_name == "test.txt" for c in chunks)


def test_overlap_shares_tokens():
    chunker = TokenBasedChunker(chunk_size=4, overlap=2)
    text = "one two three four five six seven eight"
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) >= 2
    # Overlapping chunks should share some text
    for i in range(len(chunks) - 1):
        # The end of one chunk and the start of the next should have common words
        words_current = set(chunks[i].text.split())
        words_next = set(chunks[i + 1].text.split())
        assert words_current & words_next, "Overlapping chunks should share words"


def test_select_tiktoken_encoding():
    chunker = TokenBasedChunker(chunk_size=5, overlap=0, tokenizer="o200k_base")
    text = "The quick brown fox jumps over the lazy dog"
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) >= 2


def test_unknown_tokenizer_raises():
    with pytest.raises(ValueError, match="Unknown tokenizer"):
        TokenBasedChunker(chunk_size=10, overlap=0, tokenizer="not_a_real_tokenizer")


def test_empty_text():
    chunker = TokenBasedChunker(chunk_size=10, overlap=0)
    chunks = chunker.chunk("", "test.txt")
    assert chunks == []


def test_text_within_chunk_size():
    chunker = TokenBasedChunker(chunk_size=100, overlap=0)
    text = "Short text."
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 1
    assert chunks[0].text == "Short text."


def test_invalid_overlap_raises():
    with pytest.raises(ValueError, match="overlap .* must be less than chunk_size"):
        TokenBasedChunker(chunk_size=10, overlap=10)


def test_chunk_index_increments():
    chunker = TokenBasedChunker(chunk_size=3, overlap=0)
    text = "The quick brown fox jumps over the lazy dog"
    chunks = chunker.chunk(text, "test.txt")
    for i, c in enumerate(chunks):
        assert c.chunk_index == i


def test_custom_encode_decode():
    words_list = "one two three four five six".split()

    def encode(text: str) -> list[int]:
        return list(range(len(text.split())))

    def decode(tokens: list[int]) -> str:
        return " ".join(words_list[t] for t in tokens if t < len(words_list))

    chunker = TokenBasedChunker(chunk_size=3, overlap=0, encode_fn=encode, decode_fn=decode)
    text = "one two three four five six"
    chunks = chunker.chunk(text, "test.txt")
    assert len(chunks) == 2
    assert chunks[0].text == "one two three"
    assert chunks[1].text == "four five six"


def test_only_encode_fn_raises():
    with pytest.raises(ValueError, match="encode_fn and decode_fn must both be provided"):
        TokenBasedChunker(chunk_size=10, overlap=0, encode_fn=lambda t: [])


def test_only_decode_fn_raises():
    with pytest.raises(ValueError, match="encode_fn and decode_fn must both be provided"):
        TokenBasedChunker(chunk_size=10, overlap=0, decode_fn=lambda t: "")


def test_tokenizer_and_custom_fn_raises():
    with pytest.raises(ValueError, match="Provide either tokenizer or encode_fn/decode_fn"):
        TokenBasedChunker(
            chunk_size=10, overlap=0,
            tokenizer="cl100k_base",
            encode_fn=lambda t: [],
            decode_fn=lambda t: "",
        )
