import pytest
import json
from tributary.destinations.multi_destination import MultiDestination
from tributary.destinations.json_destination import JSONDestination
from tributary.embedders.models import EmbeddingResult


def _make_results(n=3):
    return [
        EmbeddingResult(
            chunk_text=f"chunk_{i}",
            vector=[0.1, 0.2],
            source_name="doc.txt",
            chunk_index=i,
            model_name="test",
        )
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_stores_to_all_destinations(tmp_path):
    out1 = tmp_path / "out1.jsonl"
    out2 = tmp_path / "out2.jsonl"

    multi = MultiDestination([
        JSONDestination(str(out1)),
        JSONDestination(str(out2)),
    ])

    async with multi:
        await multi.store(_make_results())

    lines1 = out1.read_text().strip().split("\n")
    lines2 = out2.read_text().strip().split("\n")
    assert len(lines1) == 3
    assert len(lines2) == 3
    assert json.loads(lines1[0])["chunk_text"] == "chunk_0"
    assert json.loads(lines2[0])["chunk_text"] == "chunk_0"


@pytest.mark.asyncio
async def test_connect_and_close_all(tmp_path):
    out1 = tmp_path / "out1.jsonl"
    out2 = tmp_path / "out2.jsonl"
    d1 = JSONDestination(str(out1))
    d2 = JSONDestination(str(out2))

    multi = MultiDestination([d1, d2])
    await multi.connect()

    # Both should have file handles open
    assert d1._file is not None
    assert d2._file is not None

    await multi.close()
    assert d1._file is None
    assert d2._file is None


@pytest.mark.asyncio
async def test_empty_destinations_raises():
    with pytest.raises(ValueError, match="at least one destination"):
        MultiDestination([])


@pytest.mark.asyncio
async def test_multiple_store_calls(tmp_path):
    out1 = tmp_path / "out1.jsonl"
    out2 = tmp_path / "out2.jsonl"

    multi = MultiDestination([
        JSONDestination(str(out1)),
        JSONDestination(str(out2)),
    ])

    async with multi:
        await multi.store(_make_results(2))
        await multi.store(_make_results(3))

    assert len(out1.read_text().strip().split("\n")) == 5
    assert len(out2.read_text().strip().split("\n")) == 5


@pytest.mark.asyncio
async def test_pipeline_with_multi_destination(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.txt").write_text("Hello world document content here.")
    out1 = tmp_path / "primary.jsonl"
    out2 = tmp_path / "backup.jsonl"

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=MultiDestination([
            JSONDestination(str(out1)),
            JSONDestination(str(out2)),
        ]),
    )
    result = await pipeline.run()

    assert result.successful == 1
    assert out1.exists()
    assert out2.exists()
    lines1 = out1.read_text().strip().split("\n")
    lines2 = out2.read_text().strip().split("\n")
    assert len(lines1) == len(lines2)
    assert len(lines1) > 0


def test_yaml_multi_destination(tmp_path):
    """Test that YAML config with list of destinations works."""
    import yaml
    from click.testing import CliRunner
    from tributary.cli import cli
    from unittest.mock import patch
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.pipeline.orchestrator import Pipeline
    from tributary.destinations.multi_destination import MultiDestination

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.txt").write_text("Hello world.")
    out1 = tmp_path / "out1.jsonl"
    out2 = tmp_path / "out2.jsonl"

    cfg = {
        "source": {"type": "local", "params": {"directory": str(docs), "extensions": [".txt"]}},
        "chunker": {"strategy": "fixed", "params": {"chunk_size": 50, "overlap": 0}},
        "embedder": {"provider": "custom", "params": {}},
        "destination": [
            {"type": "json", "params": {"file_path": str(out1)}},
            {"type": "json", "params": {"file_path": str(out2)}},
        ],
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(cfg))

    def patched_build(cfg_dict, on_event=None):
        from tributary.sources import get_source
        from tributary.chunkers import get_chunker
        from tributary.destinations import get_destination

        source = get_source(cfg_dict["source"]["type"], **cfg_dict["source"].get("params", {}))
        chunker = get_chunker(cfg_dict["chunker"]["strategy"], **cfg_dict["chunker"].get("params", {}))
        embedder = CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t])
        dests = [get_destination(d["type"], **d.get("params", {})) for d in cfg_dict["destination"]]
        destination = MultiDestination(dests)
        return Pipeline(source=source, chunker=chunker, embedder=embedder, destination=destination, on_event=on_event)

    runner = CliRunner()
    with patch("tributary.cli._build_pipeline", side_effect=patched_build):
        result = runner.invoke(cli, ["run", "--config", str(config_path)])

    assert result.exit_code == 0
    assert out1.exists()
    assert out2.exists()
