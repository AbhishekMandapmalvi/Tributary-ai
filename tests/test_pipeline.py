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
    (tmp_path / "plain.txt").write_text("Plain text content for extraction.")
    (tmp_path / "rich.md").write_text("# Heading\n\n**Bold** markdown content.")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path)),
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
    (tmp_path / "good.txt").write_text("This file is fine.")
    (tmp_path / "bad.xyz").write_bytes(b"no extractor for this")
    output = tmp_path / "output.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path)),
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
