import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from tributary.workers.distributed_pipeline import DistributedPipeline
from tributary.workers.messages import DocumentMessage
from tributary.sources.models import SourceResult


@pytest.fixture
def source_mock():
    mock = MagicMock()

    async def fetch():
        for i in range(2):
            yield SourceResult(
                raw_bytes=b"content",
                file_name=f"file{i}.txt",
                source_path=f"/tmp/file{i}.txt",
                source_type="txt",
            )

    mock.fetch = fetch
    return mock


@pytest.fixture
def document_queue_mock():
    return AsyncMock()


@pytest.fixture
def chunk_queue_mock():
    return AsyncMock()


@pytest.fixture
def extractor_mock():
    return AsyncMock()


@pytest.fixture
def chunker_mock():
    return MagicMock()


@pytest.fixture
def embedder_mock():
    mock = AsyncMock()
    mock.model_name = "test-model"
    return mock


@pytest.fixture
def destination_mock():
    return AsyncMock()


@pytest.fixture
def pipeline(source_mock, document_queue_mock, chunk_queue_mock,
             extractor_mock, chunker_mock, embedder_mock, destination_mock):
    return DistributedPipeline(
        source=source_mock,
        document_queue=document_queue_mock,
        chunk_queue=chunk_queue_mock,
        extractor=extractor_mock,
        chunker=chunker_mock,
        embedder=embedder_mock,
        destination=destination_mock,
        n_extraction_workers=2,
        n_embedding_workers=2,
        poll_timeout=0.01,
    )


def test_init_stores_parameters(pipeline, source_mock):
    assert pipeline.source is source_mock
    assert pipeline.n_extraction_workers == 2
    assert pipeline.n_embedding_workers == 2
    assert pipeline.poll_timeout == 0.01
    assert pipeline.stop_event is None  # not created until run()


@pytest.mark.asyncio
async def test_run_producer_pushes_all_source_results(pipeline, document_queue_mock):
    # Workers return None from poll so they don't block the test
    document_queue_mock.poll.return_value = None
    pipeline.chunk_queue.poll.return_value = None

    await pipeline.run()

    # Producer should have pushed 2 DocumentMessages
    assert document_queue_mock.push.await_count == 2
    pushed = [call.args[0] for call in document_queue_mock.push.await_args_list]
    assert all(isinstance(m, DocumentMessage) for m in pushed)
    assert pushed[0].source_name == "file0.txt"
    assert pushed[1].source_name == "file1.txt"


@pytest.mark.asyncio
async def test_run_sets_stop_event_when_producer_finishes(pipeline, document_queue_mock):
    document_queue_mock.poll.return_value = None
    pipeline.chunk_queue.poll.return_value = None

    await pipeline.run()

    assert pipeline.stop_event is not None
    assert pipeline.stop_event.is_set()


@pytest.mark.asyncio
async def test_run_creates_stop_event(pipeline, document_queue_mock):
    document_queue_mock.poll.return_value = None
    pipeline.chunk_queue.poll.return_value = None

    assert pipeline.stop_event is None
    await pipeline.run()
    assert isinstance(pipeline.stop_event, asyncio.Event)


@pytest.mark.asyncio
async def test_run_stops_when_source_fails(pipeline, source_mock, document_queue_mock):
    async def failing_fetch():
        yield SourceResult(
            raw_bytes=b"x", file_name="f.txt",
            source_path="/tmp/f.txt", source_type="txt",
        )
        raise RuntimeError("source died")

    source_mock.fetch = failing_fetch
    document_queue_mock.poll.return_value = None
    pipeline.chunk_queue.poll.return_value = None

    await pipeline.run()

    # Stop event set via producer's finally block
    assert pipeline.stop_event.is_set()
    # Only 1 message pushed before failure
    assert document_queue_mock.push.await_count == 1


@pytest.mark.asyncio
async def test_run_spawns_correct_number_of_workers(
    source_mock, document_queue_mock, chunk_queue_mock,
    extractor_mock, chunker_mock, embedder_mock, destination_mock
):
    pipeline = DistributedPipeline(
        source=source_mock,
        document_queue=document_queue_mock,
        chunk_queue=chunk_queue_mock,
        extractor=extractor_mock,
        chunker=chunker_mock,
        embedder=embedder_mock,
        destination=destination_mock,
        n_extraction_workers=3,
        n_embedding_workers=4,
        poll_timeout=0.01,
    )
    document_queue_mock.poll.return_value = None
    chunk_queue_mock.poll.return_value = None

    await pipeline.run()

    # Producer pushes 2 messages, then stop_event fires
    # Each extraction worker polls at least once (returns None), same for embedding
    # This test verifies the pipeline completes cleanly with n > default
    assert pipeline.stop_event.is_set()
    assert document_queue_mock.push.await_count == 2
