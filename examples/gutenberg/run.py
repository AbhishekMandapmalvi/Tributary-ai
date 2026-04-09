"""Ingest Project Gutenberg books into Qdrant with OpenAI embeddings.

Prerequisites:
    pip install tributary-ai[openai,qdrant,web]
    docker run -p 6333:6333 qdrant/qdrant  # start Qdrant locally
    export OPENAI_API_KEY=sk-...

Usage:
    python examples/gutenberg/run.py

Or via CLI:
    tributary run --config examples/gutenberg/config.yaml
"""
import asyncio
from tributary.sources.web_scraper_source import WebScraperSource
from tributary.extractors.text_extractor import TextExtractor
from tributary.chunkers.recursive_chunker import RecursiveChunker
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.destinations.qdrant_destination import QdrantDestination
from tributary.pipeline.orchestrator import Pipeline
from tributary.pipeline.state_store import StateStore
from tributary.pipeline.retry import RetryPolicy, DeadLetterQueue
from tributary.pipeline.adaptive_batcher import AdaptiveBatcher
from tributary.pipeline.hooks import PipelineHooks
from tributary.pipeline.quality import ChunkQualityScorer
from tributary.pipeline.events import PipelineEvent

GUTENBERG_URLS = [
    "https://www.gutenberg.org/cache/epub/1342/pg1342.txt",  # Pride and Prejudice
    "https://www.gutenberg.org/cache/epub/11/pg11.txt",       # Alice in Wonderland
    "https://www.gutenberg.org/cache/epub/84/pg84.txt",       # Frankenstein
]


def on_event(event: PipelineEvent) -> None:
    if event.event_type == "document_started":
        print(f"  -> Processing: {event.source_name}")
    elif event.event_type == "document_completed":
        print(f"  <- Done: {event.source_name} ({event.chunks_count} chunks)")
    elif event.event_type == "document_failed":
        print(f"  !! Failed: {event.source_name} at {event.stage}: {event.error}")


async def main():
    # Quality filter — drop chunks that are Gutenberg headers/footers/garbage
    hooks = PipelineHooks()
    scorer = ChunkQualityScorer(min_score=0.4)
    hooks.after_chunk(scorer.as_chunk_filter())

    pipeline = Pipeline(
        source=WebScraperSource(urls=GUTENBERG_URLS, max_concurrent=3, timeout=30),
        extractor=TextExtractor(),
        chunker=RecursiveChunker(chunk_size=512, overlap=64),
        embedder=OpenAIEmbedder(model_name="text-embedding-3-small"),
        destination=QdrantDestination(collection_name="gutenberg", url="http://localhost:6333"),
        max_workers=3,
        batch_size=256,
        max_concurrent_embeds=3,
        state_store=StateStore(".tributary_state.json"),
        retry_policy=RetryPolicy(max_retries=2, base_delay=1.0),
        dead_letter_queue=DeadLetterQueue(".tributary_dlq.jsonl"),
        adaptive_batcher=AdaptiveBatcher(
            initial_batch_size=64,
            min_batch_size=8,
            max_batch_size=256,
            target_latency_ms=3000,
        ),
        hooks=hooks,
        on_event=on_event,
    )

    print("Ingesting Project Gutenberg books into Qdrant...\n")
    result = await pipeline.run()

    print(f"\nResults:")
    print(f"  Documents: {result.total_documents}")
    print(f"  Successful: {result.successful}")
    print(f"  Failed: {result.failed}")
    print(f"  Time: {result.time_ms:.0f}ms")

    if result.metrics:
        print(f"\nMetrics:")
        for stage in ["extraction", "chunking", "embedding", "storage"]:
            if stage in result.metrics:
                s = result.metrics[stage]
                print(f"  {stage:12s}  avg={s['avg_ms']:.1f}ms  total={s['total_ms']:.0f}ms")
        if "chunks" in result.metrics:
            print(f"  chunks: {result.metrics['chunks']['total']} total")
        if "cache" in result.metrics:
            c = result.metrics["cache"]
            print(f"  cache: {c['hits']} hits, {c['misses']} misses")
        if "adaptive_batching" in result.metrics:
            ab = result.metrics["adaptive_batching"]
            print(f"  batch size: {ab['current_batch_size']} (adjusted {ab['total_adjustments']}x)")


if __name__ == "__main__":
    asyncio.run(main())
