import asyncio
import logging

from tributary.destinations.base import BaseDestination
from tributary.embedders.base import BaseEmbedder
from tributary.embedders.models import EmbeddingResult
from tributary.workers.messages import ChunkMessage
from tributary.workers.queue import BaseQueue


class EmbeddingWorker:
    def __init__(
        self,
        chunk_queue: BaseQueue,
        embedder: BaseEmbedder,
        destination: BaseDestination,
        stop_event: asyncio.Event,
        poll_timeout: float = 1.0,
    ):
        self.chunk_queue = chunk_queue
        self.embedder = embedder
        self.destination = destination
        self.stop_event = stop_event
        self.poll_timeout = poll_timeout
        self.logger = logging.getLogger(__name__)

    async def run(self):
        while not self.stop_event.is_set():
            message = await self.chunk_queue.poll(timeout=self.poll_timeout)
            if message is None:
                continue
            try:
                await self._process(message)
                await self.chunk_queue.ack(message)
            except Exception as e:
                await self.chunk_queue.nack(message)
                self.logger.error(f"Failed to process chunk {message.message_id}: {e}")

    async def _process(self, message: ChunkMessage):
        vectors = await self.embedder.embed([message.text])
        result = EmbeddingResult(
            chunk_text=message.text,
            vector=vectors[0],
            source_name=message.source_name,
            chunk_index=message.chunk_index,
            model_name=self.embedder.model_name,
        )
        await self.destination.store([result])
