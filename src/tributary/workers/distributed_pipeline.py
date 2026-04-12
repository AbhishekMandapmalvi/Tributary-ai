import asyncio
import logging
import signal

from tributary.chunkers.base import BaseChunker
from tributary.destinations.base import BaseDestination
from tributary.embedders.base import BaseEmbedder
from tributary.extractors.base import BaseExtractor
from tributary.sources.base import BaseSource
from tributary.workers.embedding_worker import EmbeddingWorker
from tributary.workers.extraction_worker import ExtractionWorker
from tributary.workers.messages import DocumentMessage
from tributary.workers.queue import BaseQueue


class DistributedPipeline:
    """Orchestrates producer + extraction workers + embedding workers
    with shared stop signaling and graceful shutdown.
    """

    def __init__(
        self,
        source: BaseSource,
        document_queue: BaseQueue,
        chunk_queue: BaseQueue,
        extractor: BaseExtractor,
        chunker: BaseChunker,
        embedder: BaseEmbedder,
        destination: BaseDestination,
        n_extraction_workers: int = 2,
        n_embedding_workers: int = 2,
        poll_timeout: float = 1.0,
    ):
        self.source = source
        self.document_queue = document_queue
        self.chunk_queue = chunk_queue
        self.extractor = extractor
        self.chunker = chunker
        self.embedder = embedder
        self.destination = destination
        self.n_extraction_workers = n_extraction_workers
        self.n_embedding_workers = n_embedding_workers
        self.poll_timeout = poll_timeout
        self.stop_event: asyncio.Event | None = None
        self.logger = logging.getLogger(__name__)

    async def run(self):
        self.stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                # add_signal_handler isn't supported on Windows
                pass

        # Spawn extraction workers
        extraction_workers = [
            ExtractionWorker(
                documents_queue=self.document_queue,
                chunks_queue=self.chunk_queue,
                extractor=self.extractor,
                chunker=self.chunker,
                stop_event=self.stop_event,
                poll_timeout=self.poll_timeout,
            )
            for _ in range(self.n_extraction_workers)
        ]

        # Spawn embedding workers
        embedding_workers = [
            EmbeddingWorker(
                chunk_queue=self.chunk_queue,
                embedder=self.embedder,
                destination=self.destination,
                stop_event=self.stop_event,
                poll_timeout=self.poll_timeout,
            )
            for _ in range(self.n_embedding_workers)
        ]

        # Producer: read source, push DocumentMessages
        async def producer():
            try:
                async for result in self.source.fetch():
                    if self.stop_event.is_set():
                        break
                    await self.document_queue.push(DocumentMessage(
                        source_type=result.source_type,
                        source_path=result.source_path,
                        source_name=result.file_name,
                    ))
            except Exception as e:
                self.logger.error(f"Producer failed: {e}")
            finally:
                # Signal workers to stop after draining remaining messages
                self.stop_event.set()

        tasks = [
            asyncio.create_task(producer(), name="producer"),
            *(asyncio.create_task(w.run(), name=f"extraction-{i}")
              for i, w in enumerate(extraction_workers)),
            *(asyncio.create_task(w.run(), name=f"embedding-{i}")
              for i, w in enumerate(embedding_workers)),
        ]

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self.stop_event.set()
            await asyncio.gather(*tasks, return_exceptions=True)
