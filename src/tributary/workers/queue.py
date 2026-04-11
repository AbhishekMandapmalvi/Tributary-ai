from .messages import BaseMessage
from abc import ABC, abstractmethod

class BaseQueue(ABC):
    """Base class for message queues."""
    
    @abstractmethod
    async def push(self, message: BaseMessage) -> None:
        """Enqueue a message to the queue."""
        pass
    
    @abstractmethod
    async def poll(self, timeout: float) -> BaseMessage | None:
        """Dequeue a message from the queue."""
        pass

    @abstractmethod
    async def ack(self, message_id: str) -> None:
        """Acknowledge successful processing of a message."""
        pass

    @abstractmethod
    async def nack(self, message: BaseMessage) -> None:
        """Acknowledge failed processing of a message, allowing for retry."""
        pass