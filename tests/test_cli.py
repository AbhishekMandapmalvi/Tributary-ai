import pytest
import yaml
from click.testing import CliRunner
from tributary.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Tributary" in result.output


def test_run_help(runner):
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0
    assert "--config" in result.output


def test_run_missing_config(runner):
    result = runner.invoke(cli, ["run"])
    assert result.exit_code != 0
    assert "Missing" in result.output or "required" in result.output.lower()


def test_run_nonexistent_config(runner):
    result = runner.invoke(cli, ["run", "--config", "nonexistent.yaml"])
    assert result.exit_code != 0


def test_run_full_pipeline(runner, tmp_path):
    # Create test documents
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "a.txt").write_text("Hello world document content.")
    (docs_dir / "b.txt").write_text("Another document for testing.")
    output = tmp_path / "output.jsonl"

    config = {
        "source": {
            "type": "local",
            "params": {"directory": str(docs_dir), "extensions": [".txt"]},
        },
        "chunker": {
            "strategy": "fixed",
            "params": {"chunk_size": 50, "overlap": 0},
        },
        "embedder": {
            "provider": "custom",
            "params": {},
        },
        "destination": {
            "type": "json",
            "params": {"file_path": str(output)},
        },
    }

    # Custom embedder needs embed_fn which can't go in YAML,
    # so we patch _build_pipeline to inject it
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))

    from unittest.mock import patch
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.pipeline.orchestrator import Pipeline

    original_build = None

    def patched_build(cfg, on_event=None):
        from tributary.sources import get_source
        from tributary.chunkers import get_chunker
        from tributary.destinations import get_destination

        source = get_source(cfg["source"]["type"], **cfg["source"].get("params", {}))
        chunker = get_chunker(cfg["chunker"]["strategy"], **cfg["chunker"].get("params", {}))
        embedder = CustomEmbedder(embed_fn=lambda texts: [[0.1] * 3 for _ in texts])
        destination = get_destination(cfg["destination"]["type"], **cfg["destination"].get("params", {}))
        return Pipeline(source=source, chunker=chunker, embedder=embedder, destination=destination, on_event=on_event)

    with patch("tributary.cli._build_pipeline", side_effect=patched_build):
        result = runner.invoke(cli, ["run", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Successful" in result.output
    assert output.exists()


def _write_config(tmp_path, cfg):
    import yaml
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return str(path)


def test_validate_valid_config(runner, tmp_path):
    cfg = {
        "source": {"type": "local", "params": {"directory": "."}},
        "chunker": {"strategy": "fixed"},
        "embedder": {"provider": "openai"},
        "destination": {"type": "json", "params": {"file_path": "out.jsonl"}},
    }
    result = runner.invoke(cli, ["validate", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code == 0
    assert "Config is valid" in result.output


def test_validate_missing_section(runner, tmp_path):
    cfg = {
        "source": {"type": "local"},
        "chunker": {"strategy": "fixed"},
    }
    result = runner.invoke(cli, ["validate", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code != 0
    assert "'embedder' is a required property" in result.output
    assert "'destination' is a required property" in result.output


def test_validate_missing_key(runner, tmp_path):
    cfg = {
        "source": {},
        "chunker": {"strategy": "fixed"},
        "embedder": {"provider": "openai"},
        "destination": {"type": "json"},
    }
    result = runner.invoke(cli, ["validate", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code != 0
    assert "'type' is a required property" in result.output


def test_validate_unknown_registry_value(runner, tmp_path):
    cfg = {
        "source": {"type": "ftp"},
        "chunker": {"strategy": "fixed"},
        "embedder": {"provider": "openai"},
        "destination": {"type": "json"},
    }
    result = runner.invoke(cli, ["validate", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code != 0
    assert "Unknown source type: 'ftp'" in result.output


# --- inspect command ---

def test_inspect_shows_config(runner, tmp_path):
    cfg = {
        "source": {"type": "local", "params": {"directory": str(tmp_path), "extensions": [".txt"]}},
        "chunker": {"strategy": "recursive", "params": {"chunk_size": 500, "overlap": 50}},
        "embedder": {"provider": "openai", "params": {"api_key": "sk-secret"}},
        "destination": {"type": "json", "params": {"file_path": "output.jsonl"}},
        "pipeline": {"max_workers": 5},
    }
    result = runner.invoke(cli, ["inspect", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code == 0
    assert "local" in result.output
    assert "recursive" in result.output
    assert "openai" in result.output
    assert "json" in result.output
    # API key should be masked
    assert "sk-secret" not in result.output
    assert "***" in result.output


def test_inspect_invalid_config_fails(runner, tmp_path):
    cfg = {"source": {"type": "ftp"}}
    result = runner.invoke(cli, ["inspect", "-c", _write_config(tmp_path, cfg)])
    assert result.exit_code != 0


# --- benchmark command ---

def test_benchmark_runs(runner, tmp_path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "a.txt").write_text("Hello world benchmark content for testing throughput.")
    (docs_dir / "b.txt").write_text("Another document to measure processing speed.")

    result = runner.invoke(cli, ["benchmark", "-d", str(docs_dir), "--chunk-size", "30", "--workers", "2"])
    assert result.exit_code == 0
    assert "docs/sec" in result.output
    assert "chunks/sec" in result.output


# --- init command ---

def test_init_creates_config(runner, tmp_path):
    output_path = str(tmp_path / "generated.yaml")
    # Simulate interactive input: local, ./docs, all, recursive, 500, 50, openai, (empty key), json, ./output.jsonl, 3, 256
    inputs = "local\n./docs\nall\nrecursive\n500\n50\nopenai\n\njson\n./output.jsonl\n3\n256\n"
    result = runner.invoke(cli, ["init", "-o", output_path], input=inputs)
    assert result.exit_code == 0
    assert "Config written to" in result.output

    with open(output_path) as f:
        cfg = yaml.safe_load(f)
    assert cfg["source"]["type"] == "local"
    assert cfg["chunker"]["strategy"] == "recursive"
    assert cfg["embedder"]["provider"] == "openai"
    assert cfg["destination"]["type"] == "json"
