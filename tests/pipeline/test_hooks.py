import pytest
import json
from tributary.pipeline.hooks import PipelineHooks
from tributary.extractors.models import ExtractionResult
from tributary.chunkers.models import ChunkResult
from tributary.embedders.models import EmbeddingResult


# --- Unit tests for PipelineHooks ---

def test_after_extract_modifies():
    hooks = PipelineHooks()

    @hooks.after_extract
    def uppercase(extraction, source_name):
        extraction.text = extraction.text.upper()
        return extraction

    result = ExtractionResult(text="hello", source_name="a.txt", content_type="text", extraction_time_ms=0)
    modified = hooks.run_after_extract(result, "a.txt")
    assert modified.text == "HELLO"


def test_after_extract_returns_none_skips():
    hooks = PipelineHooks()

    @hooks.after_extract
    def skip_small(extraction, source_name):
        if extraction.char_count < 10:
            return None
        return extraction

    small = ExtractionResult(text="hi", source_name="a.txt", content_type="text", extraction_time_ms=0)
    assert hooks.run_after_extract(small, "a.txt") is None

    big = ExtractionResult(text="hello world!", source_name="b.txt", content_type="text", extraction_time_ms=0)
    assert hooks.run_after_extract(big, "b.txt") is not None


def test_after_chunk_filters():
    hooks = PipelineHooks()

    @hooks.after_chunk
    def remove_short(chunks, source_name):
        return [c for c in chunks if c.char_count > 5]

    chunks = [
        ChunkResult(text="hi", source_name="a.txt", chunk_index=0, start_char=0, end_char=2),
        ChunkResult(text="hello world", source_name="a.txt", chunk_index=1, start_char=3, end_char=14),
    ]
    filtered = hooks.run_after_chunk(chunks, "a.txt")
    assert len(filtered) == 1
    assert filtered[0].text == "hello world"


def test_before_embed_transforms():
    hooks = PipelineHooks()

    @hooks.before_embed
    def lowercase(texts, source_name):
        return [t.lower() for t in texts]

    result = hooks.run_before_embed(["HELLO", "WORLD"], "a.txt")
    assert result == ["hello", "world"]


def test_after_embed_filters():
    hooks = PipelineHooks()

    @hooks.after_embed
    def drop_first(embeddings, source_name):
        return embeddings[1:]

    embeddings = [
        EmbeddingResult(chunk_text="a", vector=[0.1], source_name="a.txt", chunk_index=0, model_name="test"),
        EmbeddingResult(chunk_text="b", vector=[0.2], source_name="a.txt", chunk_index=1, model_name="test"),
    ]
    result = hooks.run_after_embed(embeddings, "a.txt")
    assert len(result) == 1
    assert result[0].chunk_text == "b"


def test_multiple_hooks_chain():
    hooks = PipelineHooks()

    @hooks.after_chunk
    def add_prefix(chunks, source_name):
        for c in chunks:
            c.text = "PREFIX:" + c.text
        return chunks

    @hooks.after_chunk
    def remove_short(chunks, source_name):
        return [c for c in chunks if len(c.text) > 15]

    chunks = [
        ChunkResult(text="short", source_name="a.txt", chunk_index=0, start_char=0, end_char=5),
        ChunkResult(text="this is longer text", source_name="a.txt", chunk_index=1, start_char=6, end_char=25),
    ]
    result = hooks.run_after_chunk(chunks, "a.txt")
    # "PREFIX:short" = 12 chars → filtered out
    # "PREFIX:this is longer text" = 26 chars → kept
    assert len(result) == 1
    assert result[0].text.startswith("PREFIX:")


def test_decorator_returns_function():
    hooks = PipelineHooks()

    @hooks.after_extract
    def my_hook(extraction, source_name):
        return extraction

    # Decorator should return the original function
    assert callable(my_hook)


def test_no_hooks_passthrough():
    hooks = PipelineHooks()
    extraction = ExtractionResult(text="hello", source_name="a.txt", content_type="text", extraction_time_ms=0)
    assert hooks.run_after_extract(extraction, "a.txt") is extraction

    chunks = [ChunkResult(text="hello", source_name="a.txt", chunk_index=0, start_char=0, end_char=5)]
    assert hooks.run_after_chunk(chunks, "a.txt") is chunks

    texts = ["hello"]
    assert hooks.run_before_embed(texts, "a.txt") is texts


# --- Pipeline integration ---

@pytest.mark.asyncio
async def test_pipeline_with_chunk_filter_hook(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.txt").write_text("Hello world. This is a test. Short. Another longer sentence here.")
    output = tmp_path / "output.jsonl"

    hooks = PipelineHooks()

    @hooks.after_chunk
    def filter_short_chunks(chunks, source_name):
        return [c for c in chunks if c.char_count > 10]

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=15, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        hooks=hooks,
    )
    result = await pipeline.run()

    assert result.successful == 1
    lines = output.read_text().strip().split("\n")
    for line in lines:
        assert len(json.loads(line)["chunk_text"]) > 10


@pytest.mark.asyncio
async def test_pipeline_with_skip_document_hook(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "keep.txt").write_text("This document should be kept and processed.")
    (docs / "skip.txt").write_text("x")
    output = tmp_path / "output.jsonl"

    hooks = PipelineHooks()

    @hooks.after_extract
    def skip_tiny(extraction, source_name):
        if extraction.char_count < 5:
            return None
        return extraction

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        hooks=hooks,
    )
    result = await pipeline.run()

    assert result.successful == 2  # both "succeed" — one just produces 0 chunks
    lines = output.read_text().strip().split("\n")
    sources = {json.loads(l)["source_name"] for l in lines}
    assert "keep.txt" in sources
    assert "skip.txt" not in sources


@pytest.mark.asyncio
async def test_pipeline_without_hooks_unchanged(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.txt").write_text("Hello world content.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()
    assert result.successful == 1
