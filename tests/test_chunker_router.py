import pytest
from tributary.chunkers.router import ChunkerRouter
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.chunkers.recursive_chunker import RecursiveChunker
from tributary.chunkers.sliding_window_chunker import SlidingWindowChunker


def test_routes_by_extension():
    router = ChunkerRouter(
        default=FixedChunker(chunk_size=100, overlap=0),
        rules={
            ".pdf": RecursiveChunker(chunk_size=200, overlap=0),
            ".md": SlidingWindowChunker(window_size=150, step_size=50),
        },
    )
    assert isinstance(router.get_chunker("doc.pdf"), RecursiveChunker)
    assert isinstance(router.get_chunker("readme.md"), SlidingWindowChunker)
    assert isinstance(router.get_chunker("data.txt"), FixedChunker)


def test_extension_case_insensitive():
    router = ChunkerRouter(
        default=FixedChunker(chunk_size=100, overlap=0),
        rules={".PDF": RecursiveChunker(chunk_size=200, overlap=0)},
    )
    assert isinstance(router.get_chunker("doc.pdf"), RecursiveChunker)
    assert isinstance(router.get_chunker("doc.PDF"), RecursiveChunker)


def test_falls_back_to_default():
    router = ChunkerRouter(
        default=FixedChunker(chunk_size=100, overlap=0),
        rules={".pdf": RecursiveChunker(chunk_size=200, overlap=0)},
    )
    assert isinstance(router.get_chunker("data.csv"), FixedChunker)
    assert isinstance(router.get_chunker("unknown"), FixedChunker)


def test_custom_rule_takes_priority():
    special = SlidingWindowChunker(window_size=300, step_size=100)

    def my_rule(source_name: str):
        if "special" in source_name:
            return special
        return None

    router = ChunkerRouter(
        default=FixedChunker(chunk_size=100, overlap=0),
        rules={".txt": RecursiveChunker(chunk_size=200, overlap=0)},
        custom_rule=my_rule,
    )
    # Custom rule matches
    assert router.get_chunker("special_doc.txt") is special
    # Custom rule returns None → falls to extension rule
    assert isinstance(router.get_chunker("normal.txt"), RecursiveChunker)
    # Neither matches → default
    assert isinstance(router.get_chunker("data.csv"), FixedChunker)


def test_chunk_delegates_correctly():
    small = FixedChunker(chunk_size=10, overlap=0)
    big = FixedChunker(chunk_size=100, overlap=0)

    router = ChunkerRouter(default=small, rules={".md": big})

    text = "Hello world, this is a test document."

    # .md gets the big chunker → fewer chunks
    md_chunks = router.chunk(text, "readme.md")
    # .txt gets the small chunker → more chunks
    txt_chunks = router.chunk(text, "readme.txt")

    assert len(txt_chunks) > len(md_chunks)


def test_no_rules_always_uses_default():
    default = FixedChunker(chunk_size=50, overlap=0)
    router = ChunkerRouter(default=default)

    assert router.get_chunker("any.pdf") is default
    assert router.get_chunker("any.md") is default


@pytest.mark.asyncio
async def test_pipeline_with_router(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "small.txt").write_text("Short text. " * 10)
    (docs / "big.md").write_text("Markdown content. " * 100)
    output = tmp_path / "output.jsonl"

    router = ChunkerRouter(
        default=FixedChunker(chunk_size=50, overlap=0),
        rules={".md": FixedChunker(chunk_size=500, overlap=0)},
    )

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs)),
        chunker=router,
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
    )
    result = await pipeline.run()
    assert result.successful == 2
    assert result.failed == 0


def test_yaml_config_with_routing(tmp_path):
    import yaml
    from click.testing import CliRunner
    from tributary.cli import cli
    from unittest.mock import patch
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.pipeline.orchestrator import Pipeline
    from tributary.chunkers.router import ChunkerRouter

    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.txt").write_text("Hello world content.")
    output = tmp_path / "output.jsonl"

    cfg = {
        "source": {"type": "local", "params": {"directory": str(docs), "extensions": [".txt"]}},
        "chunker": {
            "strategy": "fixed",
            "params": {"chunk_size": 50, "overlap": 0},
            "routing": {
                ".md": {"strategy": "recursive", "params": {"chunk_size": 200, "overlap": 0}},
            },
        },
        "embedder": {"provider": "custom", "params": {}},
        "destination": {"type": "json", "params": {"file_path": str(output)}},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(cfg))

    def patched_build(cfg_dict, on_event=None):
        from tributary.sources import get_source
        from tributary.chunkers import get_chunker
        from tributary.destinations import get_destination

        source = get_source(cfg_dict["source"]["type"], **cfg_dict["source"].get("params", {}))

        chunker_cfg = cfg_dict["chunker"]
        default = get_chunker(chunker_cfg["strategy"], **chunker_cfg.get("params", {}))
        routing = chunker_cfg.get("routing", {})
        rules = {ext: get_chunker(r["strategy"], **r.get("params", {})) for ext, r in routing.items()}
        chunker = ChunkerRouter(default=default, rules=rules) if rules else default

        embedder = CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t])
        destination = get_destination(cfg_dict["destination"]["type"], **cfg_dict["destination"].get("params", {}))
        return Pipeline(source=source, chunker=chunker, embedder=embedder, destination=destination, on_event=on_event)

    runner = CliRunner()
    with patch("tributary.cli._build_pipeline", side_effect=patched_build):
        result = runner.invoke(cli, ["run", "--config", str(config_path)])

    assert result.exit_code == 0
    assert output.exists()
