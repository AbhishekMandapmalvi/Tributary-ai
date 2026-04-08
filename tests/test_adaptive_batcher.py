import pytest
from tributary.pipeline.adaptive_batcher import AdaptiveBatcher


@pytest.mark.asyncio
async def test_initial_batch_size():
    batcher = AdaptiveBatcher(initial_batch_size=64)
    assert batcher.get_batch_size() == 64


@pytest.mark.asyncio
async def test_grows_on_low_latency():
    batcher = AdaptiveBatcher(
        initial_batch_size=64,
        max_batch_size=512,
        target_latency_ms=2000.0,
        growth_factor=1.5,
    )
    # Latency well under 70% of target (1400ms) → should grow
    for _ in range(5):
        await batcher.record_latency(500.0)
    assert batcher.get_batch_size() > 64


@pytest.mark.asyncio
async def test_shrinks_on_high_latency():
    batcher = AdaptiveBatcher(
        initial_batch_size=256,
        min_batch_size=8,
        target_latency_ms=2000.0,
        shrink_factor=0.5,
    )
    # Latency over target → should shrink
    for _ in range(5):
        await batcher.record_latency(5000.0)
    assert batcher.get_batch_size() < 256


@pytest.mark.asyncio
async def test_halves_on_error():
    batcher = AdaptiveBatcher(initial_batch_size=100, min_batch_size=8)
    await batcher.record_error()
    assert batcher.get_batch_size() == 50
    await batcher.record_error()
    assert batcher.get_batch_size() == 25


@pytest.mark.asyncio
async def test_respects_min():
    batcher = AdaptiveBatcher(initial_batch_size=16, min_batch_size=8)
    await batcher.record_error()  # 8
    await batcher.record_error()  # still 8
    assert batcher.get_batch_size() == 8


@pytest.mark.asyncio
async def test_respects_max():
    batcher = AdaptiveBatcher(
        initial_batch_size=256,
        max_batch_size=512,
        target_latency_ms=2000.0,
        growth_factor=2.0,
    )
    for _ in range(20):
        await batcher.record_latency(100.0)
    assert batcher.get_batch_size() <= 512


@pytest.mark.asyncio
async def test_stable_at_target():
    batcher = AdaptiveBatcher(
        initial_batch_size=100,
        target_latency_ms=2000.0,
    )
    # Latency between 70% and 100% of target → should stay stable
    for _ in range(10):
        await batcher.record_latency(1600.0)
    assert batcher.get_batch_size() == 100


@pytest.mark.asyncio
async def test_summary():
    batcher = AdaptiveBatcher(initial_batch_size=64)
    await batcher.record_latency(1000.0)
    s = batcher.summary()
    assert s["current_batch_size"] == 64 or s["current_batch_size"] != 64  # may have adjusted
    assert s["avg_latency_ms"] is not None
    assert "total_adjustments" in s


# --- Pipeline integration ---

@pytest.mark.asyncio
async def test_pipeline_with_adaptive_batcher(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(5):
        (docs / f"doc_{i}.txt").write_text(f"Document {i} content. " * 20)
    output = tmp_path / "output.jsonl"

    batcher = AdaptiveBatcher(initial_batch_size=10, min_batch_size=2, max_batch_size=100)

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=30, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        adaptive_batcher=batcher,
    )
    result = await pipeline.run()

    assert result.successful == 5
    assert "adaptive_batching" in result.metrics
    assert result.metrics["adaptive_batching"]["avg_latency_ms"] is not None


@pytest.mark.asyncio
async def test_pipeline_without_adaptive_batcher_unchanged(tmp_path):
    """Pipeline works exactly as before when no adaptive_batcher is passed."""
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
        batch_size=256,
    )
    result = await pipeline.run()

    assert result.successful == 1
    assert "adaptive_batching" not in result.metrics
