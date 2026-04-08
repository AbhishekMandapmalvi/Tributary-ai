from tributary.sources.base import BaseSource
from tributary.extractors.base import BaseExtractor
from tributary.extractors import get_extractor_for_extension
from tributary.chunkers.base import BaseChunker
from tributary.embedders.base import BaseEmbedder
from tributary.destinations.base import BaseDestination
from tributary.pipeline.models import PipelineResult, FailedDocument
from tributary.pipeline.events import PipelineEvent
from tributary.pipeline.metrics import MetricsCollector
from collections.abc import Callable
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
        batch_size: int = 256,
        max_concurrent_embeds: int = 3,
        queue_size: int | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ):
        self.source = source
        self.extractor = extractor
        self.chunker = chunker
        self.embedder = embedder
        self.destination = destination
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.max_concurrent_embeds = max_concurrent_embeds
        self.queue_size = queue_size or max_workers * 2
        self.on_event = on_event

    async def _emit(self, event: PipelineEvent):
        if self.on_event is None:
            return
        if asyncio.iscoroutinefunction(self.on_event):
            await self.on_event(event)
        else:
            self.on_event(event)

    async def run(self) -> PipelineResult:
        start = perf_counter()
        result = PipelineResult()
        metrics = MetricsCollector()
        queue = asyncio.Queue(maxsize=self.queue_size)

        await self._emit(PipelineEvent(event_type="pipeline_started"))

        producer = asyncio.create_task(self._produce(queue, result))
        workers = [
            asyncio.create_task(self._worker(queue, result, metrics))
            for _ in range(self.max_workers)
        ]

        await producer
        await queue.join()

        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

        await self.destination.close()

        # Collect final cache stats
        hits, misses = self.embedder.get_cache_stats()
        await metrics.record_cache(hits, misses)

        result.time_ms = (perf_counter() - start) * 1000
        result.metrics = metrics.summary()

        logger.info(
            "Pipeline complete",
            total=result.total_documents,
            successful=result.successful,
            failed=result.failed,
            time_ms=round(result.time_ms, 2),
        )

        await self._emit(PipelineEvent(
            event_type="pipeline_completed",
            total_documents=result.total_documents,
            successful=result.successful,
            failed=result.failed,
            time_ms=result.time_ms,
        ))

        return result

    async def _produce(self, queue: asyncio.Queue, result: PipelineResult):
        async for source_result in self.source.fetch():
            result.total_documents += 1
            await queue.put(source_result)

    async def _worker(self, queue: asyncio.Queue, result: PipelineResult, metrics: MetricsCollector):
        embed_sem = asyncio.Semaphore(self.max_concurrent_embeds)

        while True:
            source_result = await queue.get()
            if source_result is None:
                queue.task_done()
                break

            file_name = source_result.file_name

            await self._emit(PipelineEvent(
                event_type="document_started",
                source_name=file_name,
            ))

            try:
                # Extract
                t0 = perf_counter()
                extractor = self.extractor or get_extractor_for_extension(file_name)
                extraction = await extractor.extract(source_result.raw_bytes, file_name)
                await metrics.record_stage(file_name, "extraction", (perf_counter() - t0) * 1000)

                # Chunk (offloaded to thread — CPU-bound work)
                t0 = perf_counter()
                chunks = await asyncio.to_thread(self.chunker.chunk, extraction.text, file_name)
                await metrics.record_stage(file_name, "chunking", (perf_counter() - t0) * 1000)
                await metrics.record_chunks(file_name, len(chunks))

                if not chunks:
                    result.successful += 1
                    await self._emit(PipelineEvent(
                        event_type="document_completed",
                        source_name=file_name,
                        chunks_count=0,
                    ))
                    queue.task_done()
                    continue

                # Embed and store in concurrent batches
                batches = [
                    chunks[i:i + self.batch_size]
                    for i in range(0, len(chunks), self.batch_size)
                ]

                async def _embed_and_store(batch):
                    async with embed_sem:
                        texts = [c.text for c in batch]

                        t1 = perf_counter()
                        embeddings = await self.embedder.embed_chunks(texts, file_name)
                        await metrics.record_stage(file_name, "embedding", (perf_counter() - t1) * 1000)

                        t1 = perf_counter()
                        await self.destination.store(embeddings)
                        await metrics.record_stage(file_name, "storage", (perf_counter() - t1) * 1000)

                await asyncio.gather(*[_embed_and_store(b) for b in batches])

                result.successful += 1
                logger.info("Document processed", file_name=file_name, chunks=len(chunks))

                await self._emit(PipelineEvent(
                    event_type="document_completed",
                    source_name=file_name,
                    chunks_count=len(chunks),
                ))

            except Exception as e:
                result.failed += 1
                stage = self._identify_stage(e)
                result.failures.append(FailedDocument(
                    source_name=file_name,
                    stage=stage,
                    error=str(e),
                ))
                logger.error("Document failed", file_name=file_name, stage=stage, error=str(e))

                await self._emit(PipelineEvent(
                    event_type="document_failed",
                    source_name=file_name,
                    stage=stage,
                    error=str(e),
                ))

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
