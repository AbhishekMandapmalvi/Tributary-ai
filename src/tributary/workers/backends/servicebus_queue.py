from tributary.utils.lazy_import import lazy_import
from tributary.workers.messages import BaseMessage
from tributary.workers.queue import BaseQueue

# pip install azure-servicebus
servicebus = lazy_import("azure.servicebus")
servicebus_aio = lazy_import("azure.servicebus.aio")


class ServiceBusQueue(BaseQueue):
    """Azure Service Bus implementation of the message queue."""

    def __init__(
        self,
        sender,
        receiver,
        max_retries: int = 5,
    ):
        self.sender = sender
        self.receiver = receiver
        self.max_retries = max_retries

    async def push(self, message: BaseMessage) -> None:
        sb_message = servicebus.ServiceBusMessage(
            body=message.serialize().encode("utf-8"),
        )
        await self.sender.send_messages(sb_message)

    async def poll(self, timeout: float) -> BaseMessage | None:
        received = await self.receiver.receive_messages(
            max_message_count=1,
            max_wait_time=timeout,
        )
        if not received:
            return None
        sb_message = received[0]
        message = BaseMessage.deserialize(b"".join(sb_message.body))
        message._sb_message = sb_message
        message.retry_count = sb_message.delivery_count
        return message

    async def ack(self, message: BaseMessage) -> None:
        sb_message = getattr(message, "_sb_message", None)
        if not sb_message:
            return
        await self.receiver.complete_message(sb_message)

    async def nack(self, message: BaseMessage) -> None:
        sb_message = getattr(message, "_sb_message", None)
        if not sb_message:
            return
        if sb_message.delivery_count >= self.max_retries:
            await self.receiver.dead_letter_message(
                sb_message, reason="Max retries exceeded"
            )
        else:
            await self.receiver.abandon_message(sb_message)
