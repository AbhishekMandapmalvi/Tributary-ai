import pytest
from unittest.mock import AsyncMock
from tributary.workers.backends.sqs_queue import SQSQueue
from tributary.workers.messages import BaseMessage, DocumentMessage


QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"


@pytest.fixture
def sqs_mock():
    return AsyncMock()


@pytest.fixture
def queue(sqs_mock):
    return SQSQueue(sqs_mock, QUEUE_URL, max_retries=3)


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


@pytest.mark.asyncio
async def test_push(queue, sqs_mock):
    msg = _make_message()
    await queue.push(msg)
    sqs_mock.send_message.assert_awaited_once_with(
        QueueUrl=QUEUE_URL,
        MessageBody=msg.serialize(),
    )


@pytest.mark.asyncio
async def test_poll_returns_none_when_no_messages(queue, sqs_mock):
    sqs_mock.receive_message.return_value = {"Messages": []}
    result = await queue.poll(timeout=5.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_returns_none_when_key_missing(queue, sqs_mock):
    sqs_mock.receive_message.return_value = {}
    result = await queue.poll(timeout=5.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_deserializes_message(queue, sqs_mock):
    msg = _make_message()
    sqs_mock.receive_message.return_value = {
        "Messages": [{
            "Body": msg.serialize(),
            "ReceiptHandle": "receipt-123",
            "Attributes": {"ApproximateReceiveCount": "1"},
        }]
    }
    result = await queue.poll(timeout=10.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result.source_type == "txt"
    assert result._receipt_handle == "receipt-123"
    assert result.retry_count == 0  # first delivery: 1 - 1 = 0


@pytest.mark.asyncio
async def test_poll_sets_retry_count_from_receive_count(queue, sqs_mock):
    msg = _make_message()
    sqs_mock.receive_message.return_value = {
        "Messages": [{
            "Body": msg.serialize(),
            "ReceiptHandle": "receipt-456",
            "Attributes": {"ApproximateReceiveCount": "4"},
        }]
    }
    result = await queue.poll(timeout=5.0)
    assert result.retry_count == 3  # 4 - 1 = 3


@pytest.mark.asyncio
async def test_poll_requests_approximate_receive_count(queue, sqs_mock):
    sqs_mock.receive_message.return_value = {"Messages": []}
    await queue.poll(timeout=5.0)
    sqs_mock.receive_message.assert_awaited_once_with(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5,
        AttributeNames=["ApproximateReceiveCount"],
    )


@pytest.mark.asyncio
async def test_ack_deletes_message(queue, sqs_mock):
    msg = _make_message()
    msg._receipt_handle = "receipt-789"
    await queue.ack(msg)
    sqs_mock.delete_message.assert_awaited_once_with(
        QueueUrl=QUEUE_URL,
        ReceiptHandle="receipt-789",
    )


@pytest.mark.asyncio
async def test_ack_no_op_without_receipt_handle(queue, sqs_mock):
    msg = _make_message()
    await queue.ack(msg)
    sqs_mock.delete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_nack_resets_visibility(queue, sqs_mock):
    msg = _make_message()
    msg._receipt_handle = "receipt-abc"
    await queue.nack(msg)
    sqs_mock.change_message_visibility.assert_awaited_once_with(
        QueueUrl=QUEUE_URL,
        ReceiptHandle="receipt-abc",
        VisibilityTimeout=0,
    )


@pytest.mark.asyncio
async def test_nack_no_op_without_receipt_handle(queue, sqs_mock):
    msg = _make_message()
    await queue.nack(msg)
    sqs_mock.change_message_visibility.assert_not_awaited()
