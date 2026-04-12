"""Tests for the queue factory — lazy dispatch by backend name."""
import sys
import types
import pytest
from unittest.mock import MagicMock, patch

from tributary.workers.factory import get_queue, _QUEUE_REGISTRY


@pytest.fixture
def stub_redis_module(monkeypatch):
    """Install a fake `redis.asyncio` in sys.modules so the factory can import it."""
    redis_mod = types.ModuleType("redis")
    redis_async = types.ModuleType("redis.asyncio")
    redis_async.from_url = MagicMock(return_value=MagicMock(name="redis_client"))
    redis_mod.asyncio = redis_async
    monkeypatch.setitem(sys.modules, "redis", redis_mod)
    monkeypatch.setitem(sys.modules, "redis.asyncio", redis_async)
    return redis_async


def test_unknown_backend_raises():
    with pytest.raises(ValueError, match="Unknown queue backend"):
        get_queue("nonexistent", queue_name="x")


def test_unknown_backend_lists_available():
    with pytest.raises(ValueError) as exc:
        get_queue("foo")
    msg = str(exc.value)
    for backend in ("redis", "sqs", "rabbitmq", "pubsub", "servicebus", "kafka"):
        assert backend in msg


def test_registry_contains_all_six_backends():
    assert set(_QUEUE_REGISTRY) == {
        "redis", "sqs", "rabbitmq", "pubsub", "servicebus", "kafka",
    }


def test_redis_builder_wires_client_and_queue(stub_redis_module):
    with patch(
        "tributary.workers.backends.redis_queue.RedisQueue"
    ) as mock_redis_queue:
        get_queue(
            "redis",
            url="redis://other-host:6380",
            queue_name="docs",
            max_retries=7,
            inflight_timeout=120.0,
        )
        stub_redis_module.from_url.assert_called_once_with(
            "redis://other-host:6380"
        )
        mock_redis_queue.assert_called_once_with(
            stub_redis_module.from_url.return_value,
            queue_name="docs",
            max_retries=7,
            inflight_timeout=120.0,
        )


def test_redis_builder_uses_defaults(stub_redis_module):
    with patch("tributary.workers.backends.redis_queue.RedisQueue") as mock_rq:
        get_queue("redis")
        stub_redis_module.from_url.assert_called_once_with("redis://localhost:6379")
        kwargs = mock_rq.call_args.kwargs
        assert kwargs["queue_name"] == "tributary"
        assert kwargs["max_retries"] == 5


def test_rabbitmq_builder_raises_not_implemented():
    # RabbitMQ requires an active channel; factory returns NotImplementedError
    # so users know to wire it up manually inside an async context.
    with pytest.raises(NotImplementedError, match="active channel"):
        get_queue("rabbitmq", url="amqp://localhost/", queue_name="docs")


def test_factory_does_not_import_backends_until_called():
    """Importing the factory module must not pull in optional deps."""
    import sys
    # Before any get_queue() call, none of the backend modules should be
    # loaded as a side effect of importing the factory.
    # (They may already be loaded from other tests, so this test only checks
    # that factory.py itself does not import them at module level.)
    import importlib
    import tributary.workers.factory as factory_mod
    importlib.reload(factory_mod)
    # The factory module should not have pulled in redis/aioboto3/aio_pika/etc.
    # at import time. We check by inspecting the module namespace.
    assert not hasattr(factory_mod, "aioredis")
    assert not hasattr(factory_mod, "aioboto3")
    assert not hasattr(factory_mod, "aio_pika")
