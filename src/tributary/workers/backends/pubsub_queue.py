import base64

from tributary.utils.lazy_import import lazy_import
from tributary.workers.messages import BaseMessage
from tributary.workers.queue import BaseQueue

# pip install gcloud-aio-pubsub
pubsub = lazy_import("gcloud.aio.pubsub")


class PubSubQueue(BaseQueue):
    """GCP Pub/Sub implementation of the message queue using gcloud-aio-pubsub."""

    def __init__(
        self,
        publisher,
        subscriber,
        topic_path: str,
        subscription_path: str,
        max_retries: int = 5,
    ):
        self.publisher = publisher
        self.subscriber = subscriber
        self.topic_path = topic_path
        self.subscription_path = subscription_path
        self.max_retries = max_retries

    async def push(self, message: BaseMessage) -> None:
        await self.publisher.publish(
            self.topic_path,
            [pubsub.PubsubMessage(message.serialize().encode("utf-8"))],
        )

    async def poll(self, timeout: float) -> BaseMessage | None:
        response = await self.subscriber.pull(
            self.subscription_path,
            max_messages=1,
            timeout=timeout,
        )
        messages = response.get("receivedMessages", [])
        if not messages:
            return None
        received = messages[0]
        ack_id = received["ackId"]
        raw_data = received["message"]["data"]
        data = base64.b64decode(raw_data).decode("utf-8")
        message = BaseMessage.deserialize(data)
        message._ack_id = ack_id
        attributes = received["message"].get("attributes", {})
        message.retry_count = int(attributes.get("retry_count", 0))
        return message

    async def ack(self, message: BaseMessage) -> None:
        ack_id = getattr(message, "_ack_id", None)
        if not ack_id:
            return
        await self.subscriber.acknowledge(
            self.subscription_path, [ack_id]
        )

    async def nack(self, message: BaseMessage) -> None:
        ack_id = getattr(message, "_ack_id", None)
        if not ack_id:
            return
        message.increment_retry()
        if message.retry_count >= self.max_retries:
            await self.subscriber.acknowledge(
                self.subscription_path, [ack_id]
            )
            await self._send_to_dlq(message)
        else:
            await self.subscriber.modify_ack_deadline(
                self.subscription_path, [ack_id], 0
            )

    async def _send_to_dlq(self, message: BaseMessage) -> None:
        dlq_topic = f"{self.topic_path}-dlq"
        await self.publisher.publish(
            dlq_topic,
            [pubsub.PubsubMessage(
                data=message.serialize().encode("utf-8"),
                attributes={"retry_count": str(message.retry_count)},
            )],
        )
