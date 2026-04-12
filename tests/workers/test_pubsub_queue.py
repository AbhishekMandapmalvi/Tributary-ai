import base64
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

_mock_pubsub = MagicMock()

with patch("tributary.utils.lazy_import.lazy_import", return_value=_mock_pubsub):
    from tributary.workers.backends.pubsub_queue import PubSubQueue

from tributary.workers.messages import BaseMessage, DocumentMessage


TOPIC = "projects/my-project/topics/test-topic"
SUBSCRIPTION = "projects/my-project/subscriptions/test-sub"


@pytest.fixture
def publisher_mock():
    return AsyncMock()


@pytest.fixture
def subscriber_mock():
    return AsyncMock()


@pytest.fixture
def queue(publisher_mock, subscriber_mock):
    return PubSubQueue(
        publisher_mock, subscriber_mock,
        TOPIC, SUBSCRIPTION, max_retries=3,
    )


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


def _make_pull_response(message, ack_id="ack-123", retry_count=0):
    encoded = base64.b64encode(message.serialize().encode("utf-8")).decode("utf-8")
    return {
        "receivedMessages": [{
            "ackId": ack_id,
            "message": {
                "data": encoded,
                "attributes": {"retry_count": str(retry_count)},
            },
        }]
    }


@pytest.mark.asyncio
async def test_push(queue, publisher_mock):
    _mock_pubsub.PubsubMessage.reset_mock()
    msg = _make_message()
    await queue.push(msg)
    publisher_mock.publish.assert_awaited_once()
    call_args = publisher_mock.publish.call_args
    assert call_args[0][0] == TOPIC


@pytest.mark.asyncio
async def test_poll_returns_none_when_empty(queue, subscriber_mock):
    subscriber_mock.pull.return_value = {"receivedMessages": []}
    result = await queue.poll(timeout=5.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_returns_none_when_key_missing(queue, subscriber_mock):
    subscriber_mock.pull.return_value = {}
    result = await queue.poll(timeout=5.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_deserializes_message(queue, subscriber_mock):
    msg = _make_message()
    subscriber_mock.pull.return_value = _make_pull_response(msg)

    result = await queue.poll(timeout=5.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result._ack_id == "ack-123"
    assert result.retry_count == 0


@pytest.mark.asyncio
async def test_poll_reads_retry_count(queue, subscriber_mock):
    msg = _make_message()
    subscriber_mock.pull.return_value = _make_pull_response(msg, retry_count=2)

    result = await queue.poll(timeout=5.0)
    assert result.retry_count == 2


@pytest.mark.asyncio
async def test_poll_passes_timeout(queue, subscriber_mock):
    subscriber_mock.pull.return_value = {"receivedMessages": []}
    await queue.poll(timeout=10.0)
    subscriber_mock.pull.assert_awaited_once_with(
        SUBSCRIPTION, max_messages=1, timeout=10.0,
    )


@pytest.mark.asyncio
async def test_ack_acknowledges(queue, subscriber_mock):
    msg = _make_message()
    msg._ack_id = "ack-456"
    await queue.ack(msg)
    subscriber_mock.acknowledge.assert_awaited_once_with(
        SUBSCRIPTION, ["ack-456"],
    )


@pytest.mark.asyncio
async def test_ack_no_op_without_ack_id(queue, subscriber_mock):
    msg = _make_message()
    await queue.ack(msg)
    subscriber_mock.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_redelivers_under_max_retries(queue, subscriber_mock):
    msg = _make_message()
    msg._ack_id = "ack-789"
    msg.retry_count = 0

    await queue.nack(msg)

    assert msg.retry_count == 1
    subscriber_mock.modify_ack_deadline.assert_awaited_once_with(
        SUBSCRIPTION, ["ack-789"], 0,
    )
    subscriber_mock.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_sends_to_dlq_at_max_retries(queue, publisher_mock, subscriber_mock):
    msg = _make_message()
    msg._ack_id = "ack-dlq"
    msg.retry_count = 2  # increment to 3 == max_retries

    _mock_pubsub.PubsubMessage.reset_mock()
    await queue.nack(msg)

    assert msg.retry_count == 3
    subscriber_mock.acknowledge.assert_awaited_once_with(
        SUBSCRIPTION, ["ack-dlq"],
    )
    publisher_mock.publish.assert_awaited_once()
    dlq_call = publisher_mock.publish.call_args
    assert dlq_call[0][0] == f"{TOPIC}-dlq"


@pytest.mark.asyncio
async def test_nack_no_op_without_ack_id(queue, subscriber_mock):
    msg = _make_message()
    await queue.nack(msg)
    subscriber_mock.modify_ack_deadline.assert_not_awaited()
    subscriber_mock.acknowledge.assert_not_awaited()
