import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock
from tributary.workers.messages import BaseMessage, DocumentMessage
from tributary.workers.backends.redis_queue import RedisQueue


@pytest.fixture
def redis_mock():
    mock = AsyncMock()
    mock.register_script = MagicMock(return_value=AsyncMock())
    return mock


@pytest.fixture
def queue(redis_mock):
    return RedisQueue(redis_mock, "test_queue", max_retries=3)


def _make_message(**kwargs):
    defaults = dict(
        message_id="msg-1", created_at=1000.0,
        source_type="txt", source_path="/tmp/a.txt", source_name="a.txt",
    )
    defaults.update(kwargs)
    return DocumentMessage(**defaults)


@pytest.mark.asyncio
async def test_push(queue, redis_mock):
    msg = _make_message()
    await queue.push(msg)
    redis_mock.rpush.assert_awaited_once_with("test_queue:pending", msg.serialize())


@pytest.mark.asyncio
async def test_poll_returns_none_on_timeout(queue, redis_mock):
    redis_mock.blpop.return_value = None
    result = await queue.poll(timeout=1.0)
    assert result is None


@pytest.mark.asyncio
async def test_poll_deserializes_and_adds_to_inflight(queue, redis_mock):
    msg = _make_message()
    serialized = msg.serialize().encode("utf-8")
    redis_mock.blpop.return_value = (b"test_queue:pending", serialized)

    result = await queue.poll(timeout=5.0)

    assert isinstance(result, DocumentMessage)
    assert result.message_id == "msg-1"
    assert result.source_type == "txt"
    redis_mock.zadd.assert_awaited_once()
    call_args = redis_mock.zadd.call_args
    assert call_args[0][0] == "test_queue:inflight"


@pytest.mark.asyncio
async def test_ack_removes_from_inflight(queue, redis_mock):
    msg = _make_message()
    msg._raw = b"raw_data"
    await queue.ack(msg)
    redis_mock.zrem.assert_awaited_once_with("test_queue:inflight", b"raw_data")
    redis_mock.hdel.assert_awaited_once_with("test_queue:retry_counts", "msg-1")


@pytest.mark.asyncio
async def test_ack_falls_back_to_serialize(queue, redis_mock):
    msg = _make_message()
    await queue.ack(msg)
    redis_mock.zrem.assert_awaited_once_with("test_queue:inflight", msg.serialize())


@pytest.mark.asyncio
async def test_nack_requeues_under_max_retries(queue, redis_mock):
    msg = _make_message()
    msg._raw = b"raw_data"
    redis_mock.hincrby.return_value = 1

    await queue.nack(msg)

    redis_mock.zrem.assert_awaited_once_with("test_queue:inflight", b"raw_data")
    redis_mock.hincrby.assert_awaited_once_with("test_queue:retry_counts", "msg-1", 1)
    redis_mock.rpush.assert_awaited_once()
    assert redis_mock.rpush.call_args[0][0] == "test_queue:pending"


@pytest.mark.asyncio
async def test_nack_routes_to_dlq_over_max_retries(queue, redis_mock):
    msg = _make_message()
    redis_mock.hincrby.return_value = 4  # > max_retries=3

    await queue.nack(msg)

    redis_mock.rpush.assert_awaited_once()
    assert redis_mock.rpush.call_args[0][0] == "test_queue:dlq"
    redis_mock.hdel.assert_awaited_with("test_queue:retry_counts", "msg-1")


@pytest.mark.asyncio
async def test_requeue_expired_retries(queue, redis_mock):
    msg = _make_message()
    serialized = msg.serialize().encode("utf-8")
    queue._pop_expired_script.return_value = [serialized]
    redis_mock.hincrby.return_value = 1

    count = await queue.requeue_expired()

    assert count == 1
    redis_mock.rpush.assert_awaited_once()
    assert redis_mock.rpush.call_args[0][0] == "test_queue:pending"


@pytest.mark.asyncio
async def test_requeue_expired_routes_to_dlq(queue, redis_mock):
    msg = _make_message()
    serialized = msg.serialize().encode("utf-8")
    queue._pop_expired_script.return_value = [serialized]
    redis_mock.hincrby.return_value = 4  # > max_retries=3

    count = await queue.requeue_expired()

    assert count == 1
    redis_mock.rpush.assert_awaited_once()
    assert redis_mock.rpush.call_args[0][0] == "test_queue:dlq"


@pytest.mark.asyncio
async def test_requeue_expired_empty(queue, redis_mock):
    queue._pop_expired_script.return_value = []
    count = await queue.requeue_expired()
    assert count == 0
    redis_mock.rpush.assert_not_awaited()
