import asyncio

from tributary.utils.lazy_import import lazy_import
from tributary.workers.messages import BaseMessage
from tributary.workers.queue import BaseQueue

# pip install aiokafka
aiokafka = lazy_import("aiokafka")


class KafkaQueue(BaseQueue):
    """Apache Kafka implementation of the message queue using aiokafka.

    Note: nack republishes the message to the same topic and commits the
    original offset. This is the standard Kafka retry pattern for task
    queues, but retry order is not guaranteed — a retried message may be
    processed after later messages in the partition.
    """

    def __init__(
        self,
        producer,
        consumer,
        topic: str,
        max_retries: int = 5,
    ):
        self.producer = producer
        self.consumer = consumer
        self.topic = topic
        self.max_retries = max_retries
        self.dlq_topic = f"{topic}-dlq"

    async def push(self, message: BaseMessage) -> None:
        await self.producer.send_and_wait(
            self.topic,
            value=message.serialize().encode("utf-8"),
            key=message.message_id.encode("utf-8"),
        )

    async def poll(self, timeout: float) -> BaseMessage | None:
        try:
            record = await asyncio.wait_for(
                self.consumer.getone(), timeout=timeout
            )
        except asyncio.TimeoutError:
            return None
        message = BaseMessage.deserialize(record.value)
        message._kafka_record = record
        headers = dict(record.headers) if record.headers else {}
        message.retry_count = int(
            headers.get(b"retry_count", b"0").decode("utf-8")
        )
        return message

    async def ack(self, message: BaseMessage) -> None:
        record = getattr(message, "_kafka_record", None)
        if not record:
            return
        await self.consumer.commit()

    async def nack(self, message: BaseMessage) -> None:
        record = getattr(message, "_kafka_record", None)
        if not record:
            return
        message.increment_retry()
        if message.retry_count >= self.max_retries:
            await self.producer.send_and_wait(
                self.dlq_topic,
                value=message.serialize().encode("utf-8"),
                key=message.message_id.encode("utf-8"),
                headers=[("retry_count", str(message.retry_count).encode("utf-8"))],
            )
            await self.consumer.commit()
        else:
            await self.producer.send_and_wait(
                self.topic,
                value=message.serialize().encode("utf-8"),
                key=message.message_id.encode("utf-8"),
                headers=[("retry_count", str(message.retry_count).encode("utf-8"))],
            )
            await self.consumer.commit()
