from tributary.workers.messages import BaseMessage
from tributary.workers.queue import BaseQueue
import time

_POP_EXPIRED_LUA = """
local expired = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1])
for i, v in ipairs(expired) do
    redis.call('zrem', KEYS[1], v)
end
return expired
"""

class RedisQueue(BaseQueue):
    """Redis-based implementation of the message queue."""

    def __init__(self, redis_client, queue_name: str, max_retries: int = 5):
        self._pop_expired_script = redis_client.register_script(_POP_EXPIRED_LUA)
        self.redis_client = redis_client
        self.pending_key = f"{queue_name}:pending"
        self.inflight_key = f"{queue_name}:inflight"
        self.retry_key = f"{queue_name}:retry_counts"
        self.dlq_key = f"{queue_name}:dlq"
        self.max_retries = max_retries
        
    async def push(self, message: BaseMessage) -> None:
        """Enqueue a message to the Redis queue."""
        await self.redis_client.rpush(self.pending_key, message.serialize())

    async def poll(self, timeout: float) -> BaseMessage | None:
        """Dequeue a message from the Redis queue with a timeout."""
        result = await self.redis_client.blpop(self.pending_key, timeout=timeout)
        if result is None:
            return None
        _, message_data = result
        deserialize_message = BaseMessage.deserialize(message_data)
        deserialize_message._raw = message_data  # Store raw data for ack/nack reference
        deadline = time.time() + timeout
        await self.redis_client.zadd(self.inflight_key, {message_data: deadline})
        return deserialize_message
        
    async def ack(self, message: BaseMessage) -> None:
        """Acknowledge successful processing of a message (no-op for Redis)."""
        raw = getattr(message, "_raw", message.serialize())
        await self.redis_client.zrem(self.inflight_key, raw)
        await self.redis_client.hdel(self.retry_key, message.message_id)

    async def nack(self, message: BaseMessage) -> None:
        """Negative-ack: remove from inflight, retry or route to DLQ."""
        raw = getattr(message, "_raw", message.serialize())
        await self.redis_client.zrem(self.inflight_key, raw)
        retry_count = await self.redis_client.hincrby(self.retry_key, message.message_id, 1)
        if retry_count > self.max_retries:
            await self.redis_client.rpush(self.dlq_key, message.serialize())
            await self.redis_client.hdel(self.retry_key, message.message_id)
        else:
            message.retry_count = retry_count
            await self.redis_client.rpush(self.pending_key, message.serialize())

    async def requeue_expired(self) -> int:
        """Requeue messages whose inflight timeout has expired (atomic pop)."""
        expired = await self._pop_expired_script(
            keys=[self.inflight_key], args=[time.time()]
        )
        requeued = 0
        for raw in expired:
            message = BaseMessage.deserialize(raw)
            retry_count = await self.redis_client.hincrby(
                self.retry_key, message.message_id, 1
            )
            if retry_count > self.max_retries:
                await self.redis_client.rpush(self.dlq_key, message.serialize())
                await self.redis_client.hdel(self.retry_key, message.message_id)
            else:
                message.retry_count = retry_count
                await self.redis_client.rpush(self.pending_key, message.serialize())
            requeued += 1
        return requeued