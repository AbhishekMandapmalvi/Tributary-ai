import asyncio
import logging
from tributary.workers.queue import BaseQueue
from tributary.extractors import BaseExtractor, get_extractor_for_extension
from tributary.chunkers import BaseChunker
from tributary.workers.messages import ChunkMessage

class ExtractionWorker:
    def __init__(self, documents_queue: BaseQueue,
                 chunks_queue: BaseQueue,
                 extractor: BaseExtractor | None,
                 chunker: BaseChunker,
                 stop_event: asyncio.Event,
                 poll_timeout: int = 5):
        """Extraction worker for the distributed pipeline.

        If ``extractor`` is ``None``, the extractor is auto-detected per
        file via ``get_extractor_for_extension(message.source_path)``. This
        matches the single-process Pipeline's behavior — a single worker pool
        can handle mixed file types without being pinned to one extractor.
        """
        self.documents_queue = documents_queue
        self.chunks_queue = chunks_queue
        self.extractor = extractor
        self.chunker = chunker
        self.stop_event = stop_event
        self.poll_timeout = poll_timeout
        self.logger = logging.getLogger(__name__)

    async def run(self):
        while not self.stop_event.is_set():
            message = await self.documents_queue.poll(timeout=self.poll_timeout)
            if message is None:
                continue
            try:
                result = await self._process(message)
                await self.documents_queue.ack(message)
                
            except Exception as e:
                await self.documents_queue.nack(message)
                self.logger.error(f"Error in worker run loop: {e}")

    async def _process(self, message):
        self.logger.debug(f"Processing message: {message}")
        extractor = self.extractor or get_extractor_for_extension(message.source_path)
        result = await extractor.extract(message.source_path)
        if result is not None:
            chunks = self.chunker.chunk(result)
            for i, chunk in enumerate(chunks):
                await self.chunks_queue.push(ChunkMessage(
                    document_id=message.document_id,
                    chunk_index=i,
                    text=chunk.text if hasattr(chunk, 'text') else chunk,
                    source_name=message.source_name,
                    source_path=message.source_path,
                    start_char=getattr(chunk, 'start_char', 0),
                    end_char=getattr(chunk, 'end_char', 0),
                    char_count=getattr(chunk, 'char_count', 0)
                ))
        return result