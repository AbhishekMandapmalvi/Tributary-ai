import asyncio

from tributary.utils.lazy_import import lazy_import
from tributary.workers.messages import BaseMessage
from tributary.workers.queue import BaseQueue

aio_pika = lazy_import("aio_pika")


class RabbitMQQueue(BaseQueue):
    """RabbitMQ-based implementation of the message queue using aio-pika."""

    def __init__(self, channel, queue_name: str, max_retries: int = 5):
        self.channel = channel
        self.queue_name = queue_name
        self.max_retries = max_retries
        self._queue = None

    async def _ensure_queue(self):
        if self._queue is None:
            self._queue = await self.channel.declare_queue(
                self.queue_name, durable=True
            )
        return self._queue

    async def push(self, message: BaseMessage) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=message.serialize().encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self.queue_name,
        )

    async def poll(self, timeout: float) -> BaseMessage | None:
        queue = await self._ensure_queue()
        try:
            incoming = await asyncio.wait_for(
                queue.get(no_ack=False), timeout=timeout
            )
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            if "empty" in str(e).lower():
                return None
            raise
        message = BaseMessage.deserialize(incoming.body)
        message._delivery_tag = incoming.delivery_tag
        message._incoming = incoming
        return message

    async def ack(self, message: BaseMessage) -> None:
        incoming = getattr(message, "_incoming", None)
        if incoming:
            await incoming.ack()

    async def nack(self, message: BaseMessage) -> None:
        incoming = getattr(message, "_incoming", None)
        if not incoming:
            return
        message.increment_retry()
        if message.retry_count >= self.max_retries:
            await incoming.reject(requeue=False)
        else:
            await incoming.nack(requeue=True)
