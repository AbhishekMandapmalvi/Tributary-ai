from tributary.sources.base import BaseSource
from tributary.extractors.base import BaseExtractor
from tributary.extractors import get_extractor_for_extension
from tributary.chunkers.base import BaseChunker
from tributary.embedders.base import BaseEmbedder
from tributary.destinations.base import BaseDestination
from tributary.pipeline.models import PipelineResult, FailedDocument
from tributary.pipeline.events import PipelineEvent
from tributary.pipeline.metrics import MetricsCollector
from tributary.pipeline.state_store import StateStore
from tributary.pipeline.retry import RetryPolicy, DeadLetterQueue
from tributary.pipeline.correlation import new_correlation_id
from tributary.pipeline.adaptive_batcher import AdaptiveBatcher
from collections.abc import Callable
from time import perf_counter
import asyncio
import signal
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
        # Resilience
        state_store: StateStore | None = None,
        retry_policy: RetryPolicy | None = None,
        dead_letter_queue: DeadLetterQueue | None = None,
        checkpoint_interval: int = 10,
        adaptive_batcher: AdaptiveBatcher | None = None,
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
        self.state_store = state_store
        self.retry_policy = retry_policy or RetryPolicy(max_retries=0)
        self.dlq = dead_letter_queue
        self.checkpoint_interval = checkpoint_interval
        self.adaptive_batcher = adaptive_batcher
        self._shutdown_event = asyncio.Event()
        self._docs_since_checkpoint = 0

    async def _emit(self, event: PipelineEvent) -> None:
        if self.on_event is None:
            return
        if asyncio.iscoroutinefunction(self.on_event):
            await self.on_event(event)
        else:
            self.on_event(event)

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._request_shutdown)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for SIGTERM
            pass

    def _request_shutdown(self) -> None:
        logger.info("Shutdown requested — finishing current documents...")
        self._shutdown_event.set()

    async def _maybe_checkpoint(self) -> None:
        if self.state_store is None:
            return
        self._docs_since_checkpoint += 1
        if self._docs_since_checkpoint >= self.checkpoint_interval:
            await self.state_store.checkpoint()
            self._docs_since_checkpoint = 0

    async def run(self) -> PipelineResult:
        start = perf_counter()
        result = PipelineResult()
        metrics = MetricsCollector()
        queue: asyncio.Queue[object] = asyncio.Queue(maxsize=self.queue_size)

        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)

        await self.destination.connect()
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

        # Final checkpoint
        if self.state_store:
            await self.state_store.checkpoint()

        # Collect final cache stats
        hits, misses = self.embedder.get_cache_stats()
        await metrics.record_cache(hits, misses)

        result.time_ms = (perf_counter() - start) * 1000
        result.metrics = metrics.summary()
        if self.adaptive_batcher:
            result.metrics["adaptive_batching"] = self.adaptive_batcher.summary()

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

    async def _produce(self, queue: asyncio.Queue, result: PipelineResult) -> None:
        async for source_result in self.source.fetch():
            if self._shutdown_event.is_set():
                logger.info("Shutdown: stopping document fetch")
                break

            # Deduplication — skip already-processed documents
            if self.state_store:
                content_hash = StateStore.hash_content(source_result.raw_bytes)
                if self.state_store.is_processed(content_hash):
                    logger.info("Skipping duplicate", file_name=source_result.file_name)
                    continue

            result.total_documents += 1
            await queue.put(source_result)

    async def _worker(self, queue: asyncio.Queue, result: PipelineResult, metrics: MetricsCollector) -> None:
        embed_sem = asyncio.Semaphore(self.max_concurrent_embeds)

        while True:
            source_result = await queue.get()
            if source_result is None:
                queue.task_done()
                break

            file_name = source_result.file_name
            content_hash = StateStore.hash_content(source_result.raw_bytes) if self.state_store else None
            cid = new_correlation_id()

            await self._emit(PipelineEvent(
                event_type="document_started",
                source_name=file_name,
            ))
            logger.debug("Document assigned", file_name=file_name, correlation_id=cid)

            last_error: Exception | None = None
            last_stage = "unknown"

            for attempt in range(self.retry_policy.max_retries + 1):
                try:
                    if attempt > 0:
                        delay = self.retry_policy.delay_for_attempt(attempt - 1)
                        logger.info("Retrying document", file_name=file_name, attempt=attempt, delay=delay)
                        await asyncio.sleep(delay)

                    chunks_count = await self._process_document(source_result, file_name, metrics, embed_sem)

                    # Success
                    result.successful += 1
                    if self.state_store and content_hash:
                        await self.state_store.mark_processed(content_hash, file_name)
                    await self._maybe_checkpoint()

                    logger.info("Document processed", file_name=file_name, attempt=attempt)
                    await self._emit(PipelineEvent(
                        event_type="document_completed",
                        source_name=file_name,
                        chunks_count=chunks_count,
                    ))
                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    last_stage = self._identify_stage(e)
                    if not self.retry_policy.should_retry(attempt + 1):
                        break
                    logger.warning("Document failed, will retry", file_name=file_name, attempt=attempt, error=str(e))

            if last_error is not None:
                result.failed += 1
                result.failures.append(FailedDocument(
                    source_name=file_name,
                    stage=last_stage,
                    error=str(last_error),
                ))
                logger.error("Document failed permanently", file_name=file_name, stage=last_stage, error=str(last_error))

                if self.dlq:
                    await self.dlq.push(file_name, last_stage, str(last_error), self.retry_policy.max_retries)

                await self._emit(PipelineEvent(
                    event_type="document_failed",
                    source_name=file_name,
                    stage=last_stage,
                    error=str(last_error),
                ))

            queue.task_done()

    async def _process_document(
        self,
        source_result: object,
        file_name: str,
        metrics: MetricsCollector,
        embed_sem: asyncio.Semaphore,
    ) -> int:
        """Extract, chunk, embed, store a single document. Returns chunk count. Raises on failure."""
        # Extract
        t0 = perf_counter()
        extractor = self.extractor or get_extractor_for_extension(file_name)
        extraction = await extractor.extract(source_result.raw_bytes, file_name)  # type: ignore[attr-defined]
        await metrics.record_stage(file_name, "extraction", (perf_counter() - t0) * 1000)

        # Chunk (offloaded to thread — CPU-bound work)
        t0 = perf_counter()
        chunks = await asyncio.to_thread(self.chunker.chunk, extraction.text, file_name)
        await metrics.record_stage(file_name, "chunking", (perf_counter() - t0) * 1000)
        await metrics.record_chunks(file_name, len(chunks))

        if not chunks:
            return 0

        # Determine batch size — adaptive or fixed
        current_batch_size = (
            self.adaptive_batcher.get_batch_size()
            if self.adaptive_batcher
            else self.batch_size
        )

        # Embed and store in concurrent batches
        batches = [
            chunks[i:i + current_batch_size]
            for i in range(0, len(chunks), current_batch_size)
        ]

        async def _embed_and_store(batch: list) -> None:
            async with embed_sem:
                texts = [c.text for c in batch]

                t1 = perf_counter()
                try:
                    embeddings = await self.embedder.embed_chunks(texts, file_name)
                    embed_ms = (perf_counter() - t1) * 1000
                    await metrics.record_stage(file_name, "embedding", embed_ms)

                    if self.adaptive_batcher:
                        await self.adaptive_batcher.record_latency(embed_ms)
                except Exception:
                    if self.adaptive_batcher:
                        await self.adaptive_batcher.record_error()
                    raise

                t1 = perf_counter()
                await self.destination.store(embeddings)
                await metrics.record_stage(file_name, "storage", (perf_counter() - t1) * 1000)

        await asyncio.gather(*[_embed_and_store(b) for b in batches])
        return len(chunks)

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
