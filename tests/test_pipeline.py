import pytest
import json
from tributary.sources.local_source import LocalSource
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline


def fake_embed(texts):
    return [[0.1, 0.2, 0.3] for _ in texts]


@pytest.mark.asyncio
async def test_full_pipeline(tmp_path):
    (tmp_path / "a.txt").write_text("Hello world. This is a test document with enough text to chunk.")
    (tmp_path / "b.txt").write_text("Another document for the pipeline to process end to end.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=30, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    assert result.total_documents == 2
    assert result.successful == 2
    assert result.failed == 0
    assert result.time_ms > 0

    lines = output.read_text().strip().split("\n")
    assert len(lines) > 0
    for line in lines:
        row = json.loads(line)
        assert row["vector"] == [0.1, 0.2, 0.3]
        assert "chunk_text" in row
        assert "source_name" in row


@pytest.mark.asyncio
async def test_mixed_file_types(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "plain.txt").write_text("Plain text content for extraction.")
    (docs / "rich.md").write_text("# Heading\n\n**Bold** markdown content.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs)),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    assert result.total_documents == 2
    assert result.successful == 2
    assert result.failed == 0

    lines = output.read_text().strip().split("\n")
    source_names = {json.loads(l)["source_name"] for l in lines}
    assert "plain.txt" in source_names
    assert "rich.md" in source_names


@pytest.mark.asyncio
async def test_bad_file_among_good(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "good.txt").write_text("This file is fine.")
    (docs / "bad.xyz").write_bytes(b"no extractor for this")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs)),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    assert result.total_documents == 2
    assert result.successful == 1
    assert result.failed == 1
    assert len(result.failures) == 1
    assert result.failures[0].source_name == "bad.xyz"


@pytest.mark.asyncio
async def test_empty_source(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(empty_dir)),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    assert result.total_documents == 0
    assert result.successful == 0
    assert result.failed == 0
    assert not output.exists() or output.read_text() == ""


@pytest.mark.asyncio
async def test_pipeline_result_metadata(tmp_path):
    for i in range(5):
        (tmp_path / f"doc_{i}.txt").write_text(f"Document number {i} with some content.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    assert result.total_documents == 5
    assert result.successful == 5
    assert result.failed == 0
    assert result.failures == []
    assert result.time_ms > 0


@pytest.mark.asyncio
async def test_events_emitted(tmp_path):
    (tmp_path / "a.txt").write_text("Hello world document.")
    (tmp_path / "b.txt").write_text("Another document here.")
    output = tmp_path / "output.jsonl"
    events = []

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
        on_event=lambda e: events.append(e),
    )
    await pipeline.run()

    types = [e.event_type for e in events]
    assert types[0] == "pipeline_started"
    assert types[-1] == "pipeline_completed"
    assert types.count("document_started") == 2
    assert types.count("document_completed") == 2

    # Each document_started is followed by its document_completed
    for e in events:
        if e.event_type == "document_completed":
            assert e.source_name is not None
            assert e.chunks_count is not None

    # pipeline_completed has summary
    final = events[-1]
    assert final.total_documents == 2
    assert final.successful == 2
    assert final.failed == 0
    assert final.time_ms > 0


@pytest.mark.asyncio
async def test_events_on_failure(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "good.txt").write_text("Good content.")
    (docs / "bad.xyz").write_bytes(b"bad")
    output = tmp_path / "output.jsonl"
    events = []

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs)),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
        on_event=lambda e: events.append(e),
    )
    await pipeline.run()

    types = [e.event_type for e in events]
    assert "document_failed" in types

    failed = [e for e in events if e.event_type == "document_failed"]
    assert len(failed) == 1
    assert failed[0].source_name == "bad.xyz"
    assert failed[0].stage is not None
    assert failed[0].error is not None


@pytest.mark.asyncio
async def test_async_event_callback(tmp_path):
    (tmp_path / "doc.txt").write_text("Some content.")
    output = tmp_path / "output.jsonl"
    events = []

    async def async_handler(event):
        events.append(event)

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
        on_event=async_handler,
    )
    await pipeline.run()

    types = [e.event_type for e in events]
    assert "pipeline_started" in types
    assert "document_started" in types
    assert "document_completed" in types
    assert "pipeline_completed" in types


@pytest.mark.asyncio
async def test_metrics_collected(tmp_path):
    (tmp_path / "a.txt").write_text("First document with some text content here.")
    (tmp_path / "b.txt").write_text("Second document with different content.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=20, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()

    m = result.metrics
    assert "extraction" in m
    assert "chunking" in m
    assert "embedding" in m
    assert "storage" in m
    assert "chunks" in m
    assert "cache" in m

    # 2 documents = 2 extraction and chunking calls
    assert m["extraction"]["count"] == 2
    assert m["chunking"]["count"] == 2

    # Timing stats present and positive
    for stage in ["extraction", "chunking", "embedding", "storage"]:
        assert m[stage]["total_ms"] >= 0
        assert m[stage]["avg_ms"] >= 0
        assert m[stage]["min_ms"] >= 0
        assert m[stage]["max_ms"] >= 0

    # Chunk stats
    assert m["chunks"]["total"] > 0
    assert m["chunks"]["avg_per_doc"] > 0

    # Cache stats
    assert m["cache"]["hits"] + m["cache"]["misses"] > 0
