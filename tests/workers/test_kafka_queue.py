import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

_mock_aiokafka = MagicMock()

with patch("tributary.utils.lazy_import.lazy_import", return_value=_mock_aiokafka):
    from tributary.workers.backends.kafka_queue import KafkaQueue

from tributary.workers.messages import BaseMessage, DocumentMessage


TOPIC = "test-topic"


@pytest.fixture
def producer_mock():
    return AsyncMock()


@pytest.fixture
def consumer_mock():
    return AsyncMock()


@pytest.fixture
def queue(producer_mock, consumer_mock):
    return KafkaQueue(producer_mock, consumer_mock, TOPIC, max_retries=3)


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


def _make_record(message, headers=None):
    record = MagicMock()
    record.value = message.serialize().encode("utf-8")
    record.headers = headers or []
    return record


@pytest.mark.asyncio
async def test_push(queue, producer_mock):
    msg = _make_message()
    await queue.push(msg)
    producer_mock.send_and_wait.assert_awaited_once_with(
        TOPIC,
        value=msg.serialize().encode("utf-8"),
        key=b"msg-1",
    )


@pytest.mark.asyncio
async def test_push_uses_message_id_as_key(queue, producer_mock):
    msg = _make_message(message_id="custom-key")
    await queue.push(msg)
    call_kwargs = producer_mock.send_and_wait.call_args.kwargs
    assert call_kwargs["key"] == b"custom-key"


@pytest.mark.asyncio
async def test_poll_returns_none_on_timeout(queue, consumer_mock):
    async def never_return(*args, **kwargs):
        await asyncio.sleep(10)

    consumer_mock.getone.side_effect = never_return
    result = await queue.poll(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_poll_deserializes_message(queue, consumer_mock):
    msg = _make_message()
    record = _make_record(msg)
    consumer_mock.getone.return_value = record

    result = await queue.poll(timeout=5.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result._kafka_record is record
    assert result.retry_count == 0


@pytest.mark.asyncio
async def test_poll_reads_retry_count_from_headers(queue, consumer_mock):
    msg = _make_message()
    record = _make_record(msg, headers=[(b"retry_count", b"2")])
    consumer_mock.getone.return_value = record

    result = await queue.poll(timeout=5.0)
    assert result.retry_count == 2


@pytest.mark.asyncio
async def test_ack_commits_offset(queue, consumer_mock):
    msg = _make_message()
    msg._kafka_record = MagicMock()
    await queue.ack(msg)
    consumer_mock.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_ack_no_op_without_record(queue, consumer_mock):
    msg = _make_message()
    await queue.ack(msg)
    consumer_mock.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_retries_under_max(queue, producer_mock, consumer_mock):
    msg = _make_message()
    msg._kafka_record = MagicMock()

    await queue.nack(msg)

    assert msg.retry_count == 1
    producer_mock.send_and_wait.assert_awaited_once()
    call_kwargs = producer_mock.send_and_wait.call_args.kwargs
    assert producer_mock.send_and_wait.call_args[0][0] == TOPIC
    assert call_kwargs["headers"] == [("retry_count", b"1")]
    consumer_mock.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_nack_sends_to_dlq_at_max_retries(queue, producer_mock, consumer_mock):
    msg = _make_message()
    msg._kafka_record = MagicMock()
    msg.retry_count = 2  # increment to 3 == max_retries

    await queue.nack(msg)

    assert msg.retry_count == 3
    producer_mock.send_and_wait.assert_awaited_once()
    assert producer_mock.send_and_wait.call_args[0][0] == f"{TOPIC}-dlq"
    consumer_mock.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_nack_no_op_without_record(queue, producer_mock, consumer_mock):
    msg = _make_message()
    await queue.nack(msg)
    producer_mock.send_and_wait.assert_not_awaited()
    consumer_mock.commit.assert_not_awaited()
