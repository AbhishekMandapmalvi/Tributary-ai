import pytest
from tributary.pipeline.correlation import new_correlation_id, get_correlation_id, add_correlation_id
from tributary.pipeline.cost_estimator import estimate_cost, CostEstimate
from tributary.pipeline.otel_exporter import TributaryMetricsExporter
from tributary.pipeline.events import PipelineEvent


# --- Correlation ID tests ---

def test_new_correlation_id_generates_unique():
    id1 = new_correlation_id()
    id2 = new_correlation_id()
    assert id1 != id2
    assert len(id1) == 12
    assert len(id2) == 12


def test_get_correlation_id_returns_current():
    cid = new_correlation_id()
    assert get_correlation_id() == cid


def test_add_correlation_id_processor():
    cid = new_correlation_id()
    event_dict = {"event": "test"}
    result = add_correlation_id(None, "info", event_dict)
    assert result["correlation_id"] == cid


def test_add_correlation_id_without_context():
    from tributary.pipeline.correlation import _correlation_id
    _correlation_id.set(None)
    event_dict = {"event": "test"}
    result = add_correlation_id(None, "info", event_dict)
    assert "correlation_id" not in result


# --- Cost estimator tests ---

def test_estimate_cost_basic():
    texts = ["hello world"] * 100  # 100 chunks, 11 chars each
    est = estimate_cost(texts, model_name="text-embedding-3-small")
    assert isinstance(est, CostEstimate)
    assert est.total_characters == 1100
    assert est.estimated_tokens == 275  # 1100 / 4.0
    assert est.model_name == "text-embedding-3-small"
    assert est.estimated_cost_usd > 0
    assert est.document_count == 100


def test_estimate_cost_custom_pricing():
    texts = ["a" * 4000]  # 4000 chars = 1000 tokens
    est = estimate_cost(texts, model_name="custom-model", price_per_million_tokens=1.0)
    assert est.estimated_tokens == 1000
    assert est.estimated_cost_usd == 0.001  # 1000 / 1M * $1.0


def test_estimate_cost_unknown_model():
    texts = ["hello"]
    est = estimate_cost(texts, model_name="unknown-model")
    assert est.price_per_million_tokens == 0.0
    assert est.estimated_cost_usd == 0.0


def test_estimate_cost_empty():
    est = estimate_cost([], model_name="text-embedding-3-small")
    assert est.total_characters == 0
    assert est.estimated_tokens == 0
    assert est.estimated_cost_usd == 0.0


# --- OTel exporter tests (without opentelemetry installed) ---

def test_otel_exporter_graceful_without_sdk():
    """Exporter should not crash if opentelemetry is not installed."""
    exporter = TributaryMetricsExporter()
    # Should no-op gracefully
    exporter.on_event(PipelineEvent(event_type="document_completed", source_name="test.txt", chunks_count=5))
    exporter.on_event(PipelineEvent(event_type="document_failed", source_name="bad.txt", stage="extraction", error="fail"))
    exporter.on_event(PipelineEvent(event_type="pipeline_completed", total_documents=1, successful=1, failed=0, time_ms=100.0))


# --- Correlation IDs in pipeline ---

@pytest.mark.asyncio
async def test_correlation_id_in_pipeline(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline
    from tributary.pipeline.correlation import get_correlation_id

    (tmp_path / "a.txt").write_text("Hello world content.")
    output = tmp_path / "output.jsonl"

    seen_cids: list[str | None] = []

    def capture_cid(event: PipelineEvent) -> None:
        if event.event_type == "document_started":
            seen_cids.append(get_correlation_id())

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        on_event=capture_cid,
    )
    await pipeline.run()

    assert len(seen_cids) == 1
    assert seen_cids[0] is not None
    assert len(seen_cids[0]) == 12


# --- Cost estimate CLI ---

def test_cost_estimate_cli(tmp_path):
    from click.testing import CliRunner
    from tributary.cli import cli

    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "a.txt").write_text("Hello world. " * 100)

    runner = CliRunner()
    result = runner.invoke(cli, ["cost-estimate", "-d", str(docs_dir), "-m", "text-embedding-3-small"])
    assert result.exit_code == 0
    assert "Estimated cost" in result.output
    assert "$" in result.output
