import pytest
from tributary.embedders import get_embedder
from tributary.embedders.base import BaseEmbedder
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.embedders.cohere_embedder import CohereEmbedder


def fake_embed(texts: list[str]) -> list[list[float]]:
    return [[float(len(t))] * 3 for t in texts]


@pytest.mark.asyncio
async def test_embed_returns_vectors():
    embedder = CustomEmbedder(embed_fn=fake_embed)
    vectors = await embedder.embed(["hello", "world!"])
    assert vectors == [[5.0, 5.0, 5.0], [6.0, 6.0, 6.0]]


@pytest.mark.asyncio
async def test_embed_chunks_returns_embedding_results():
    embedder = CustomEmbedder(embed_fn=fake_embed, model_name="test-model")
    results = await embedder.embed_chunks(["hello", "world!"], source_name="doc.txt")
    assert len(results) == 2

    r0 = results[0]
    assert r0.chunk_text == "hello"
    assert r0.vector == [5.0, 5.0, 5.0]
    assert r0.source_name == "doc.txt"
    assert r0.chunk_index == 0
    assert r0.model_name == "test-model"

    r1 = results[1]
    assert r1.chunk_text == "world!"
    assert r1.chunk_index == 1


@pytest.mark.asyncio
async def test_embed_chunk_single():
    embedder = CustomEmbedder(embed_fn=fake_embed, model_name="test-model")
    result = await embedder.embed_chunk("hello", source_name="doc.txt", chunk_index=5)
    assert result.chunk_text == "hello"
    assert result.vector == [5.0, 5.0, 5.0]
    assert result.source_name == "doc.txt"
    assert result.chunk_index == 5
    assert result.model_name == "test-model"


@pytest.mark.asyncio
async def test_async_embed_fn():
    async def async_fake_embed(texts: list[str]) -> list[list[float]]:
        return [[1.0, 2.0] for _ in texts]

    embedder = CustomEmbedder(embed_fn=async_fake_embed)
    vectors = await embedder.embed(["hello"])
    assert vectors == [[1.0, 2.0]]


def test_registry_openai():
    assert isinstance(get_embedder("openai", api_key="fake"), OpenAIEmbedder)


def test_registry_cohere():
    assert isinstance(get_embedder("cohere", api_key="fake"), CohereEmbedder)


def test_registry_custom():
    assert isinstance(get_embedder("custom", embed_fn=fake_embed), CustomEmbedder)


def test_unknown_provider_raises():
    with pytest.raises(ValueError, match="Unknown embedder provider"):
        get_embedder("unknown")


@pytest.mark.asyncio
async def test_cache_avoids_duplicate_embeds():
    call_count = {"n": 0}

    def counting_embed(texts):
        call_count["n"] += len(texts)
        return [[float(len(t))] * 3 for t in texts]

    embedder = CustomEmbedder(embed_fn=counting_embed)

    # First call — all texts are new
    await embedder.embed_chunks(["hello", "world"], source_name="a.txt")
    assert call_count["n"] == 2

    # Second call — same texts, should come from cache
    results = await embedder.embed_chunks(["hello", "world"], source_name="b.txt")
    assert call_count["n"] == 2  # no new embed calls
    assert results[0].vector == [5.0, 5.0, 5.0]
    assert results[1].vector == [5.0, 5.0, 5.0]


@pytest.mark.asyncio
async def test_cache_partial_hit():
    call_count = {"n": 0}

    def counting_embed(texts):
        call_count["n"] += len(texts)
        return [[float(len(t))] * 3 for t in texts]

    embedder = CustomEmbedder(embed_fn=counting_embed)

    await embedder.embed_chunks(["hello"], source_name="a.txt")
    assert call_count["n"] == 1

    # "hello" cached, "new" is not
    results = await embedder.embed_chunks(["hello", "new"], source_name="b.txt")
    assert call_count["n"] == 2  # only 1 new embed call
    assert results[0].vector == [5.0, 5.0, 5.0]
    assert results[1].vector == [3.0, 3.0, 3.0]


@pytest.mark.asyncio
async def test_cache_eviction():
    call_count = {"n": 0}

    def counting_embed(texts):
        call_count["n"] += len(texts)
        return [[float(len(t))] * 3 for t in texts]

    embedder = CustomEmbedder(embed_fn=counting_embed, max_cache_size=2)

    await embedder.embed_chunks(["aa", "bb"], source_name="a.txt")
    assert call_count["n"] == 2

    # This should evict "aa" (oldest)
    await embedder.embed_chunks(["cc"], source_name="a.txt")
    assert call_count["n"] == 3
    assert len(embedder._cache) == 2

    # "bb" should still be cached, "aa" should not
    await embedder.embed_chunks(["bb"], source_name="a.txt")
    assert call_count["n"] == 3  # cache hit

    await embedder.embed_chunks(["aa"], source_name="a.txt")
    assert call_count["n"] == 4  # cache miss — was evicted
