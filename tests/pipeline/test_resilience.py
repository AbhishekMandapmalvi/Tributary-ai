import pytest
import json
from tributary.sources.local_source import LocalSource
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline
from tributary.pipeline.state_store import StateStore
from tributary.pipeline.retry import RetryPolicy, DeadLetterQueue


def fake_embed(texts):
    return [[0.1, 0.2, 0.3] for _ in texts]


# --- StateStore tests ---

@pytest.mark.asyncio
async def test_state_store_mark_and_check(tmp_path):
    store = StateStore(path=str(tmp_path / "state.json"))
    assert not store.is_processed("abc123")

    await store.mark_processed("abc123", "doc.txt")
    assert store.is_processed("abc123")
    assert store.processed_count == 1


@pytest.mark.asyncio
async def test_state_store_checkpoint_and_reload(tmp_path):
    path = str(tmp_path / "state.json")
    store = StateStore(path=path)
    await store.mark_processed("hash1", "a.txt")
    await store.mark_processed("hash2", "b.txt")
    await store.checkpoint()

    # Reload from disk
    store2 = StateStore(path=path)
    assert store2.is_processed("hash1")
    assert store2.is_processed("hash2")
    assert store2.processed_count == 2


@pytest.mark.asyncio
async def test_state_store_clear(tmp_path):
    store = StateStore(path=str(tmp_path / "state.json"))
    await store.mark_processed("hash1", "a.txt")
    await store.checkpoint()
    await store.clear()
    assert store.processed_count == 0
    assert not (tmp_path / "state.json").exists()


def test_state_store_hash_content():
    h1 = StateStore.hash_content(b"hello")
    h2 = StateStore.hash_content(b"hello")
    h3 = StateStore.hash_content(b"world")
    assert h1 == h2
    assert h1 != h3


# --- RetryPolicy tests ---

def test_retry_policy_delay():
    policy = RetryPolicy(max_retries=3, base_delay=1.0, max_delay=10.0)
    assert policy.delay_for_attempt(0) == 1.0
    assert policy.delay_for_attempt(1) == 2.0
    assert policy.delay_for_attempt(2) == 4.0
    assert policy.delay_for_attempt(5) == 10.0  # capped


def test_retry_policy_should_retry():
    policy = RetryPolicy(max_retries=3)
    assert policy.should_retry(0)
    assert policy.should_retry(1)
    assert policy.should_retry(2)
    assert not policy.should_retry(3)


# --- DeadLetterQueue tests ---

@pytest.mark.asyncio
async def test_dlq_push_and_read(tmp_path):
    dlq = DeadLetterQueue(path=str(tmp_path / "dlq.jsonl"))
    await dlq.push("bad.txt", "extraction", "decode error", 2)
    await dlq.push("worse.txt", "embedding", "rate limit", 3)

    entries = dlq.read_all()
    assert len(entries) == 2
    assert entries[0]["source_name"] == "bad.txt"
    assert entries[0]["attempts"] == 3
    assert entries[1]["source_name"] == "worse.txt"


@pytest.mark.asyncio
async def test_dlq_clear(tmp_path):
    dlq = DeadLetterQueue(path=str(tmp_path / "dlq.jsonl"))
    await dlq.push("file.txt", "storage", "timeout", 1)
    assert dlq.count == 1
    await dlq.clear()
    assert dlq.count == 0


# --- Pipeline integration: deduplication ---

@pytest.mark.asyncio
async def test_deduplication_skips_processed(tmp_path):
    (tmp_path / "a.txt").write_text("Hello world.")
    (tmp_path / "b.txt").write_text("Different content.")
    output = tmp_path / "output.jsonl"
    state_path = str(tmp_path / "state.json")

    # First run — processes both
    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
        state_store=StateStore(path=state_path),
    )
    result1 = await pipeline.run()
    assert result1.successful == 2

    # Second run — same files, should skip both
    output2 = tmp_path / "output2.jsonl"
    pipeline2 = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output2)),
        state_store=StateStore(path=state_path),
    )
    result2 = await pipeline2.run()
    assert result2.total_documents == 0
    assert result2.successful == 0


# --- Pipeline integration: retry ---

@pytest.mark.asyncio
async def test_retry_on_transient_failure(tmp_path):
    (tmp_path / "doc.txt").write_text("Some content here.")
    output = tmp_path / "output.jsonl"

    call_count = {"n": 0}

    def flaky_embed(texts):
        call_count["n"] += 1
        if call_count["n"] <= 1:
            raise ConnectionError("Transient network error")
        return [[0.1, 0.2, 0.3] for _ in texts]

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=flaky_embed),
        destination=JSONDestination(str(output)),
        retry_policy=RetryPolicy(max_retries=2, base_delay=0.01),
    )
    result = await pipeline.run()

    assert result.successful == 1
    assert result.failed == 0


# --- Pipeline integration: dead-letter queue ---

@pytest.mark.asyncio
async def test_permanent_failure_goes_to_dlq(tmp_path):
    (tmp_path / "doc.txt").write_text("Content.")
    output = tmp_path / "output.jsonl"
    dlq_path = str(tmp_path / "dlq.jsonl")

    def always_fail(texts):
        raise RuntimeError("Permanent embed failure")

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=always_fail),
        destination=JSONDestination(str(output)),
        retry_policy=RetryPolicy(max_retries=1, base_delay=0.01),
        dead_letter_queue=DeadLetterQueue(path=dlq_path),
    )
    result = await pipeline.run()

    assert result.failed == 1
    dlq = DeadLetterQueue(path=dlq_path)
    entries = dlq.read_all()
    assert len(entries) == 1
    assert entries[0]["source_name"] == "doc.txt"
    assert entries[0]["attempts"] == 2  # 1 initial + 1 retry


# --- Pipeline integration: checkpoint ---

@pytest.mark.asyncio
async def test_checkpoint_saves_state(tmp_path):
    for i in range(5):
        (tmp_path / f"doc_{i}.txt").write_text(f"Content {i}.")
    output = tmp_path / "output.jsonl"
    state_path = str(tmp_path / "state.json")

    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=fake_embed),
        destination=JSONDestination(str(output)),
        state_store=StateStore(path=state_path),
        checkpoint_interval=2,
    )
    await pipeline.run()

    # State file should exist with all 5 docs marked processed
    store = StateStore(path=state_path)
    assert store.processed_count == 5
