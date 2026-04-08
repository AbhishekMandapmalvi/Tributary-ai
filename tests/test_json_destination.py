import pytest
import json
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.models import EmbeddingResult


def _make_result(chunk_text="hello", chunk_index=0):
    return EmbeddingResult(
        chunk_text=chunk_text,
        vector=[0.1, 0.2, 0.3],
        source_name="doc.txt",
        chunk_index=chunk_index,
        model_name="test-model",
    )


@pytest.mark.asyncio
async def test_store_writes_jsonl(tmp_path):
    path = tmp_path / "output.jsonl"
    async with JSONDestination(str(path)) as dest:
        await dest.store([_make_result("hello", 0), _make_result("world", 1)])

    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    row0 = json.loads(lines[0])
    assert row0["chunk_text"] == "hello"
    assert row0["vector"] == [0.1, 0.2, 0.3]
    assert row0["source_name"] == "doc.txt"
    assert row0["chunk_index"] == 0
    assert row0["model_name"] == "test-model"


@pytest.mark.asyncio
async def test_store_appends(tmp_path):
    path = tmp_path / "output.jsonl"
    dest = JSONDestination(str(path))
    await dest.store([_make_result("first", 0)])
    await dest.store([_make_result("second", 1)])

    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["chunk_text"] == "first"
    assert json.loads(lines[1])["chunk_text"] == "second"


@pytest.mark.asyncio
async def test_store_empty_batch(tmp_path):
    path = tmp_path / "output.jsonl"
    async with JSONDestination(str(path)) as dest:
        await dest.store([])

    assert path.read_text() == ""


@pytest.mark.asyncio
async def test_each_line_is_valid_json(tmp_path):
    path = tmp_path / "output.jsonl"
    results = [_make_result(f"chunk_{i}", i) for i in range(5)]
    async with JSONDestination(str(path)) as dest:
        await dest.store(results)

    lines = path.read_text().strip().split("\n")
    assert len(lines) == 5
    for line in lines:
        parsed = json.loads(line)
        assert "chunk_text" in parsed
        assert "vector" in parsed


@pytest.mark.asyncio
async def test_context_manager(tmp_path):
    path = tmp_path / "output.jsonl"
    async with JSONDestination(str(path)) as dest:
        await dest.store([_make_result()])
    # Should not raise — close() is a no-op but must work
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 1
