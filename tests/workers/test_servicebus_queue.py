import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

_mock_servicebus = MagicMock()

with patch("tributary.utils.lazy_import.lazy_import", return_value=_mock_servicebus):
    from tributary.workers.backends.servicebus_queue import ServiceBusQueue

from tributary.workers.messages import BaseMessage, DocumentMessage


@pytest.fixture
def sender_mock():
    return AsyncMock()


@pytest.fixture
def receiver_mock():
    return AsyncMock()


@pytest.fixture
def queue(sender_mock, receiver_mock):
    return ServiceBusQueue(sender_mock, receiver_mock, max_retries=3)


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


def _make_sb_message(message, delivery_count=0):
    sb_msg = MagicMock()
    body_bytes = message.serialize().encode("utf-8")
    sb_msg.body = [body_bytes]
    sb_msg.delivery_count = delivery_count
    return sb_msg


@pytest.mark.asyncio
async def test_push(queue, sender_mock):
    _mock_servicebus.ServiceBusMessage.reset_mock()
    msg = _make_message()
    await queue.push(msg)
    _mock_servicebus.ServiceBusMessage.assert_called_once_with(
        body=msg.serialize().encode("utf-8"),
    )
    sender_mock.send_messages.assert_awaited_once()


@pytest.mark.asyncio
async def test_poll_returns_none_when_empty(queue, receiver_mock):
    receiver_mock.receive_messages.return_value = []
    result = await queue.poll(timeout=5.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_passes_params(queue, receiver_mock):
    receiver_mock.receive_messages.return_value = []
    await queue.poll(timeout=10.0)
    receiver_mock.receive_messages.assert_awaited_once_with(
        max_message_count=1,
        max_wait_time=10.0,
    )


@pytest.mark.asyncio
async def test_poll_deserializes_message(queue, receiver_mock):
    msg = _make_message()
    sb_msg = _make_sb_message(msg, delivery_count=0)
    receiver_mock.receive_messages.return_value = [sb_msg]

    result = await queue.poll(timeout=5.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result._sb_message is sb_msg
    assert result.retry_count == 0


@pytest.mark.asyncio
async def test_poll_reads_delivery_count(queue, receiver_mock):
    msg = _make_message()
    sb_msg = _make_sb_message(msg, delivery_count=2)
    receiver_mock.receive_messages.return_value = [sb_msg]

    result = await queue.poll(timeout=5.0)
    assert result.retry_count == 2


@pytest.mark.asyncio
async def test_ack_completes_message(queue, receiver_mock):
    msg = _make_message()
    sb_msg = MagicMock()
    msg._sb_message = sb_msg

    await queue.ack(msg)
    receiver_mock.complete_message.assert_awaited_once_with(sb_msg)


@pytest.mark.asyncio
async def test_ack_no_op_without_sb_message(queue, receiver_mock):
    msg = _make_message()
    await queue.ack(msg)
    receiver_mock.complete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_abandons_under_max_retries(queue, receiver_mock):
    msg = _make_message()
    sb_msg = MagicMock()
    sb_msg.delivery_count = 1
    msg._sb_message = sb_msg

    await queue.nack(msg)

    receiver_mock.abandon_message.assert_awaited_once_with(sb_msg)
    receiver_mock.dead_letter_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_dead_letters_at_max_retries(queue, receiver_mock):
    msg = _make_message()
    sb_msg = MagicMock()
    sb_msg.delivery_count = 3  # == max_retries
    msg._sb_message = sb_msg

    await queue.nack(msg)

    receiver_mock.dead_letter_message.assert_awaited_once_with(
        sb_msg, reason="Max retries exceeded",
    )
    receiver_mock.abandon_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_no_op_without_sb_message(queue, receiver_mock):
    msg = _make_message()
    await queue.nack(msg)
    receiver_mock.abandon_message.assert_not_awaited()
    receiver_mock.dead_letter_message.assert_not_awaited()
