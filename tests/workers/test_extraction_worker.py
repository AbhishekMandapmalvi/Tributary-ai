import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from tributary.workers.extraction_worker import ExtractionWorker
from tributary.workers.messages import DocumentMessage, ChunkMessage


def _make_document(**kwargs):
    defaults = dict(
        message_id="doc-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


@pytest.fixture
def documents_queue_mock():
    return AsyncMock()


@pytest.fixture
def chunks_queue_mock():
    return AsyncMock()


@pytest.fixture
def extractor_mock():
    mock = AsyncMock()
    mock.extract.return_value = "extracted text content"
    return mock


@pytest.fixture
def chunker_mock():
    mock = MagicMock()  # chunker.chunk() is sync
    chunk1 = MagicMock(text="hello", start_char=0, end_char=5, char_count=5)
    chunk2 = MagicMock(text="world", start_char=5, end_char=10, char_count=5)
    mock.chunk.return_value = [chunk1, chunk2]
    return mock


@pytest.fixture
def stop_event():
    return asyncio.Event()


@pytest.fixture
def worker(documents_queue_mock, chunks_queue_mock, extractor_mock, chunker_mock, stop_event):
    return ExtractionWorker(
        documents_queue=documents_queue_mock,
        chunks_queue=chunks_queue_mock,
        extractor=extractor_mock,
        chunker=chunker_mock,
        stop_event=stop_event,
    )


@pytest.mark.asyncio
async def test_process_extracts_and_pushes_chunks(
    worker, extractor_mock, chunker_mock, chunks_queue_mock
):
    doc = _make_document()
    # Attach document_id attribute since the worker reads it
    doc.document_id = "doc-1"

    await worker._process(doc)

    extractor_mock.extract.assert_awaited_once_with("/tmp/a.txt")
    chunker_mock.chunk.assert_called_once_with("extracted text content")
    assert chunks_queue_mock.push.await_count == 2


@pytest.mark.asyncio
async def test_process_pushes_chunk_messages(
    worker, chunks_queue_mock
):
    doc = _make_document()
    doc.document_id = "doc-1"

    await worker._process(doc)

    pushed = [call.args[0] for call in chunks_queue_mock.push.await_args_list]
    assert all(isinstance(c, ChunkMessage) for c in pushed)
    assert pushed[0].chunk_index == 0
    assert pushed[1].chunk_index == 1
    assert pushed[0].text == "hello"
    assert pushed[1].text == "world"
    assert pushed[0].source_name == "a.txt"
    assert pushed[0].source_path == "/tmp/a.txt"


@pytest.mark.asyncio
async def test_process_skips_when_extractor_returns_none(
    worker, extractor_mock, chunker_mock, chunks_queue_mock
):
    extractor_mock.extract.return_value = None
    doc = _make_document()
    doc.document_id = "doc-1"

    await worker._process(doc)

    chunker_mock.chunk.assert_not_called()
    chunks_queue_mock.push.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_acks_on_success(worker, documents_queue_mock, stop_event):
    doc = _make_document()
    doc.document_id = "doc-1"
    call_count = 0

    async def poll_once(timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return doc
        stop_event.set()
        return None

    documents_queue_mock.poll.side_effect = poll_once
    await worker.run()

    documents_queue_mock.ack.assert_awaited_once_with(doc)
    documents_queue_mock.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_nacks_on_failure(
    worker, documents_queue_mock, extractor_mock, stop_event
):
    doc = _make_document()
    doc.document_id = "doc-1"
    extractor_mock.extract.side_effect = RuntimeError("extract failed")
    call_count = 0

    async def poll_once(timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return doc
        stop_event.set()
        return None

    documents_queue_mock.poll.side_effect = poll_once
    await worker.run()

    documents_queue_mock.nack.assert_awaited_once_with(doc)
    documents_queue_mock.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_skips_none_polls(worker, documents_queue_mock, stop_event):
    call_count = 0

    async def poll_none(timeout):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            stop_event.set()
        return None

    documents_queue_mock.poll.side_effect = poll_none
    await worker.run()

    assert call_count >= 3
    documents_queue_mock.ack.assert_not_awaited()
    documents_queue_mock.nack.assert_not_awaited()
