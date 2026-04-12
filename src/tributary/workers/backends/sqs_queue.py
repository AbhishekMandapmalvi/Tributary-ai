from tributary.workers.queue import BaseQueue
from tributary.workers.messages import BaseMessage


class SQSQueue(BaseQueue):
    def __init__(self, sqs_client, queue_url: str, max_retries: int = 5):
        self.sqs_client = sqs_client
        self.queue_url = queue_url
        self.max_retries = max_retries
    
    async def push(self, message: BaseMessage) -> None:
        """Enqueue a message to the SQS queue."""
        await self.sqs_client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message.serialize()
        )

    async def poll(self, timeout: float) -> BaseMessage | None:
        """Dequeue a message from the SQS queue with a timeout."""
        response = await self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=int(timeout),
            AttributeNames=["ApproximateReceiveCount"]
        )
        messages = response.get("Messages", [])
        if not messages:
            return None
        msg = messages[0]
        message = BaseMessage.deserialize(msg["Body"])
        message._receipt_handle = msg["ReceiptHandle"]
        attributes = msg.get("Attributes", {})
        message.retry_count = int(attributes.get("ApproximateReceiveCount", 1)) - 1
        return message
    
    async def ack(self, message: BaseMessage) -> None:
        """Acknowledge successful processing of a message."""
        receipt_handle = getattr(message, '_receipt_handle', None)
        if not receipt_handle:
            return
        await self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=message._receipt_handle
        )

    async def nack(self, message: BaseMessage) -> None:
        receipt_handle = getattr(message, '_receipt_handle', None)
        if receipt_handle:
            await self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=0
            )