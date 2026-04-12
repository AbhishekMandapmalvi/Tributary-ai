import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

# Patch lazy_import before RabbitMQQueue module loads aio_pika
_mock_aio_pika = MagicMock()
_mock_aio_pika.DeliveryMode.PERSISTENT = 2
_mock_aio_pika.exceptions.QueueEmpty = type("QueueEmpty", (Exception,), {})

with patch("tributary.utils.lazy_import.lazy_import", return_value=_mock_aio_pika):
    from tributary.workers.backends.rabbitmq_queue import RabbitMQQueue

from tributary.workers.messages import BaseMessage, DocumentMessage


@pytest.fixture
def channel_mock():
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    return channel


@pytest.fixture
def queue(channel_mock):
    return RabbitMQQueue(channel_mock, "test_queue", max_retries=3)


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


def _make_incoming(message):
    incoming = AsyncMock()
    incoming.body = message.serialize().encode("utf-8")
    incoming.delivery_tag = "dtag-123"
    return incoming


@pytest.mark.asyncio
async def test_push(queue, channel_mock):
    msg = _make_message()
    await queue.push(msg)
    channel_mock.default_exchange.publish.assert_awaited_once()
    call_kwargs = channel_mock.default_exchange.publish.call_args
    assert call_kwargs.kwargs["routing_key"] == "test_queue"


@pytest.mark.asyncio
async def test_push_persistent_delivery(queue, channel_mock):
    msg = _make_message()
    _mock_aio_pika.Message.reset_mock()
    await queue.push(msg)
    _mock_aio_pika.Message.assert_called_once_with(
        body=msg.serialize().encode("utf-8"),
        delivery_mode=2,
    )


@pytest.mark.asyncio
async def test_poll_declares_queue_once(queue, channel_mock):
    declared_queue = AsyncMock()
    declared_queue.get.side_effect = asyncio.TimeoutError
    channel_mock.declare_queue.return_value = declared_queue

    await queue.poll(timeout=0.01)
    await queue.poll(timeout=0.01)

    channel_mock.declare_queue.assert_awaited_once_with("test_queue", durable=True)


@pytest.mark.asyncio
async def test_poll_returns_none_on_timeout(queue, channel_mock):
    declared_queue = AsyncMock()
    declared_queue.get.side_effect = asyncio.TimeoutError
    channel_mock.declare_queue.return_value = declared_queue

    result = await queue.poll(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_poll_returns_none_on_queue_empty(queue, channel_mock):
    declared_queue = AsyncMock()
    declared_queue.get.side_effect = Exception("Queue is empty")
    channel_mock.declare_queue.return_value = declared_queue

    result = await queue.poll(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_poll_reraises_non_empty_exceptions(queue, channel_mock):
    declared_queue = AsyncMock()
    declared_queue.get.side_effect = RuntimeError("connection lost")
    channel_mock.declare_queue.return_value = declared_queue

    with pytest.raises(RuntimeError, match="connection lost"):
        await queue.poll(timeout=0.01)


@pytest.mark.asyncio
async def test_poll_deserializes_message(queue, channel_mock):
    msg = _make_message()
    incoming = _make_incoming(msg)
    declared_queue = AsyncMock()
    declared_queue.get.return_value = incoming
    channel_mock.declare_queue.return_value = declared_queue

    result = await queue.poll(timeout=5.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result._delivery_tag == "dtag-123"
    assert result._incoming is incoming


@pytest.mark.asyncio
async def test_poll_uses_no_ack_false(queue, channel_mock):
    declared_queue = AsyncMock()
    declared_queue.get.side_effect = asyncio.TimeoutError
    channel_mock.declare_queue.return_value = declared_queue

    await queue.poll(timeout=0.01)
    declared_queue.get.assert_awaited_once_with(no_ack=False)


@pytest.mark.asyncio
async def test_ack_calls_incoming_ack(queue):
    msg = _make_message()
    incoming = AsyncMock()
    msg._incoming = incoming

    await queue.ack(msg)
    incoming.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_ack_no_op_without_incoming(queue):
    msg = _make_message()
    await queue.ack(msg)  # should not raise


@pytest.mark.asyncio
async def test_nack_requeues_under_max_retries(queue):
    msg = _make_message()
    incoming = AsyncMock()
    msg._incoming = incoming

    await queue.nack(msg)

    assert msg.retry_count == 1
    incoming.nack.assert_awaited_once_with(requeue=True)
    incoming.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_rejects_at_max_retries(queue):
    msg = _make_message()
    msg.retry_count = 2  # increment to 3 == max_retries, triggers reject
    incoming = AsyncMock()
    msg._incoming = incoming

    await queue.nack(msg)

    assert msg.retry_count == 3
    incoming.reject.assert_awaited_once_with(requeue=False)
    incoming.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_no_op_without_incoming(queue):
    msg = _make_message()
    await queue.nack(msg)  # should not raise
