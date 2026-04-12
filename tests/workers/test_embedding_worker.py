import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, PropertyMock
from tributary.workers.embedding_worker import EmbeddingWorker
from tributary.workers.messages import ChunkMessage
from tributary.embedders.models import EmbeddingResult


def _make_chunk(**kwargs):
    defaults = dict(
        message_id="chunk-1", created_at=1000.0,
        document_id="doc-1", text="hello world",
        source_name="a.pdf", source_path="/tmp/a.pdf",
        chunk_index=0, start_char=0, end_char=11, char_count=11,
    )
    defaults.update(kwargs)
    return ChunkMessage(**defaults)


@pytest.fixture
def embedder_mock():
    mock = AsyncMock()
    mock.model_name = "test-model"
    mock.embed.return_value = [[0.1, 0.2, 0.3]]
    return mock


@pytest.fixture
def destination_mock():
    return AsyncMock()


@pytest.fixture
def queue_mock():
    return AsyncMock()


@pytest.fixture
def stop_event():
    return asyncio.Event()


@pytest.fixture
def worker(queue_mock, embedder_mock, destination_mock, stop_event):
    return EmbeddingWorker(
        chunk_queue=queue_mock,
        embedder=embedder_mock,
        destination=destination_mock,
        stop_event=stop_event,
    )


@pytest.mark.asyncio
async def test_process_embeds_and_stores(worker, embedder_mock, destination_mock):
    chunk = _make_chunk()
    await worker._process(chunk)

    embedder_mock.embed.assert_awaited_once_with(["hello world"])
    destination_mock.store.assert_awaited_once()
    stored = destination_mock.store.call_args[0][0]
    assert len(stored) == 1
    assert isinstance(stored[0], EmbeddingResult)
    assert stored[0].vector == [0.1, 0.2, 0.3]
    assert stored[0].source_name == "a.pdf"
    assert stored[0].chunk_index == 0
    assert stored[0].model_name == "test-model"


@pytest.mark.asyncio
async def test_run_acks_on_success(worker, queue_mock, stop_event):
    chunk = _make_chunk()
    call_count = 0

    async def poll_once(timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return chunk
        stop_event.set()
        return None

    queue_mock.poll.side_effect = poll_once
    await worker.run()

    queue_mock.ack.assert_awaited_once_with(chunk)
    queue_mock.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_nacks_on_failure(worker, queue_mock, embedder_mock, stop_event):
    chunk = _make_chunk()
    embedder_mock.embed.side_effect = RuntimeError("embed failed")
    call_count = 0

    async def poll_once(timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return chunk
        stop_event.set()
        return None

    queue_mock.poll.side_effect = poll_once
    await worker.run()

    queue_mock.nack.assert_awaited_once_with(chunk)
    queue_mock.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_skips_none_polls(worker, queue_mock, stop_event):
    call_count = 0

    async def poll_none(timeout):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            stop_event.set()
        return None

    queue_mock.poll.side_effect = poll_none
    await worker.run()

    assert call_count >= 3
    queue_mock.ack.assert_not_awaited()
    queue_mock.nack.assert_not_awaited()
