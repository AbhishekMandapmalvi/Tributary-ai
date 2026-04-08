from tributary.sources.base import BaseSource
from tributary.extractors.base import BaseExtractor
from tributary.extractors import get_extractor_for_extension
from tributary.chunkers.base import BaseChunker
from tributary.embedders.base import BaseEmbedder
from tributary.destinations.base import BaseDestination
from tributary.pipeline.models import PipelineResult, FailedDocument
from time import perf_counter
import asyncio
import structlog

logger = structlog.get_logger(__name__)


class Pipeline:
    def __init__(
        self,
        source: BaseSource,
        chunker: BaseChunker,
        embedder: BaseEmbedder,
        destination: BaseDestination,
        extractor: BaseExtractor | None = None,
        max_workers: int = 3,
        batch_size: int = 10,
    ):
        self.source = source
        self.extractor = extractor
        self.chunker = chunker
        self.embedder = embedder
        self.destination = destination
        self.max_workers = max_workers
        self.batch_size = batch_size

    async def run(self) -> PipelineResult:
        start = perf_counter()
        result = PipelineResult()
        queue = asyncio.Queue()

        producer = asyncio.create_task(self._produce(queue, result))
        workers = [
            asyncio.create_task(self._worker(queue, result))
            for _ in range(self.max_workers)
        ]

        await producer
        await queue.join()

        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

        await self.destination.close()

        result.time_ms = (perf_counter() - start) * 1000
        logger.info(
            "Pipeline complete",
            total=result.total_documents,
            successful=result.successful,
            failed=result.failed,
            time_ms=round(result.time_ms, 2),
        )
        return result

    async def _produce(self, queue: asyncio.Queue, result: PipelineResult):
        async for source_result in self.source.fetch():
            result.total_documents += 1
            await queue.put(source_result)

    async def _worker(self, queue: asyncio.Queue, result: PipelineResult):
        while True:
            source_result = await queue.get()
            if source_result is None:
                queue.task_done()
                break

            file_name = source_result.file_name
            try:
                # Extract
                extractor = self.extractor or get_extractor_for_extension(file_name)
                extraction = await extractor.extract(source_result.raw_bytes, file_name)

                # Chunk
                chunks = self.chunker.chunk(extraction.text, file_name)
                if not chunks:
                    result.successful += 1
                    queue.task_done()
                    continue

                # Embed and store in batches
                for i in range(0, len(chunks), self.batch_size):
                    batch = chunks[i:i + self.batch_size]
                    texts = [c.text for c in batch]
                    embeddings = await self.embedder.embed_chunks(texts, file_name)
                    await self.destination.store(embeddings)

                result.successful += 1
                logger.info("Document processed", file_name=file_name, chunks=len(chunks))

            except Exception as e:
                result.failed += 1
                stage = self._identify_stage(e)
                result.failures.append(FailedDocument(
                    source_name=file_name,
                    stage=stage,
                    error=str(e),
                ))
                logger.error("Document failed", file_name=file_name, stage=stage, error=str(e))

            queue.task_done()

    @staticmethod
    def _identify_stage(error: Exception) -> str:
        msg = str(error).lower()
        if "extractor" in msg or "decode" in msg or "json" in msg:
            return "extraction"
        if "chunk" in msg:
            return "chunking"
        if "embed" in msg:
            return "embedding"
        if "store" in msg or "upsert" in msg or "insert" in msg:
            return "storage"
        return "unknown"
