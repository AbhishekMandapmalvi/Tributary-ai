"""Factory for constructing queue backends from a config dict.

Each backend needs a live client (Redis connection, SQS client, etc.) that
can't be serialized into YAML directly. This module maps a backend name plus
a small connection config to a fully wired queue instance, importing the
backend module lazily so unused backends don't pull in their optional deps.

Example:
    queue = get_queue("redis", url="redis://localhost:6379", queue_name="docs")
    queue = get_queue("sqs", queue_url="https://sqs.us-east-1.../docs")
"""
from __future__ import annotations

from typing import Any

from tributary.workers.queue import BaseQueue


def _build_redis(
    url: str = "redis://localhost:6379",
    queue_name: str = "tributary",
    max_retries: int = 5,
    inflight_timeout: float = 300.0,
    **client_kwargs: Any,
) -> BaseQueue:
    import redis.asyncio as aioredis
    from tributary.workers.backends.redis_queue import RedisQueue

    client = aioredis.from_url(url, **client_kwargs)
    return RedisQueue(
        client,
        queue_name=queue_name,
        max_retries=max_retries,
        inflight_timeout=inflight_timeout,
    )


def _build_sqs(
    queue_url: str,
    region_name: str | None = None,
    max_retries: int = 5,
    **client_kwargs: Any,
) -> BaseQueue:
    import aioboto3  # type: ignore[import-not-found]
    from tributary.workers.backends.sqs_queue import SQSQueue

    session = aioboto3.Session()
    client_ctx = session.client("sqs", region_name=region_name, **client_kwargs)
    # client_ctx is an async context manager; caller must manage lifecycle.
    # For factory use, we resolve it eagerly via __aenter__ on first use.
    return SQSQueue(client_ctx, queue_url=queue_url, max_retries=max_retries)


def _build_rabbitmq(
    url: str = "amqp://guest:guest@localhost/",
    queue_name: str = "tributary",
    max_retries: int = 5,
    **_: Any,
) -> BaseQueue:
    # RabbitMQ requires an already-open channel, which can only be created
    # inside an async context. The factory can't build one synchronously,
    # so we raise early with a pointer to the manual path.
    raise NotImplementedError(
        "RabbitMQ factory requires an active channel. Instantiate "
        "`RabbitMQQueue(channel, queue_name, max_retries)` directly after "
        "calling `await aio_pika.connect_robust(url)` and `await conn.channel()`."
    )


def _build_pubsub(
    project: str,
    topic: str,
    subscription: str,
    max_retries: int = 5,
    **_: Any,
) -> BaseQueue:
    from gcloud.aio.pubsub import PublisherClient, SubscriberClient  # type: ignore[import-not-found]
    from tributary.workers.backends.pubsub_queue import PubSubQueue

    publisher = PublisherClient()
    subscriber = SubscriberClient()
    topic_path = f"projects/{project}/topics/{topic}"
    subscription_path = f"projects/{project}/subscriptions/{subscription}"
    return PubSubQueue(
        publisher=publisher,
        subscriber=subscriber,
        topic_path=topic_path,
        subscription_path=subscription_path,
        max_retries=max_retries,
    )


def _build_servicebus(
    connection_string: str,
    queue_name: str,
    max_retries: int = 5,
    **_: Any,
) -> BaseQueue:
    from azure.servicebus.aio import ServiceBusClient  # type: ignore[import-not-found]
    from tributary.workers.backends.servicebus_queue import ServiceBusQueue

    client = ServiceBusClient.from_connection_string(connection_string)
    sender = client.get_queue_sender(queue_name=queue_name)
    receiver = client.get_queue_receiver(queue_name=queue_name)
    return ServiceBusQueue(sender=sender, receiver=receiver, max_retries=max_retries)


def _build_kafka(
    bootstrap_servers: str,
    topic: str,
    group_id: str = "tributary",
    max_retries: int = 5,
    **_: Any,
) -> BaseQueue:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-not-found]
    from tributary.workers.backends.kafka_queue import KafkaQueue

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap_servers, group_id=group_id
    )
    return KafkaQueue(
        producer=producer, consumer=consumer, topic=topic, max_retries=max_retries
    )


_QUEUE_REGISTRY = {
    "redis": _build_redis,
    "sqs": _build_sqs,
    "rabbitmq": _build_rabbitmq,
    "pubsub": _build_pubsub,
    "servicebus": _build_servicebus,
    "kafka": _build_kafka,
}


def get_queue(backend: str, **params: Any) -> BaseQueue:
    """Construct a queue backend by name from a params dict.

    Parameters
    ----------
    backend:
        One of: ``redis``, ``sqs``, ``rabbitmq``, ``pubsub``, ``servicebus``,
        ``kafka``.
    **params:
        Backend-specific connection parameters. Unknown keys are passed
        through to the underlying client constructor where supported.

    Raises
    ------
    ValueError
        If ``backend`` is not registered.
    """
    builder = _QUEUE_REGISTRY.get(backend)
    if builder is None:
        raise ValueError(
            f"Unknown queue backend: '{backend}'. "
            f"Available: {', '.join(sorted(_QUEUE_REGISTRY))}"
        )
    return builder(**params)
