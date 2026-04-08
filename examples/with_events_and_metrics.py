"""Live progress dashboard using event callbacks.

The CLI prints results after the pipeline finishes. This script reacts
to events in real time — useful for long-running pipelines, progress bars,
logging to external systems, or triggering alerts on failures.
"""
import asyncio
import json
from tributary.sources.local_source import LocalSource
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline
from tributary.pipeline.events import PipelineEvent


# Track state across events
progress = {"done": 0, "failed": 0, "total": 0}


async def on_event(event: PipelineEvent) -> None:
    """Async callback — could write to a database, send a webhook, etc."""
    if event.event_type == "document_started":
        progress["total"] += 1
        print(f"  [{progress['done']}/{progress['total']}] Processing {event.source_name}...")

    elif event.event_type == "document_completed":
        progress["done"] += 1
        print(f"  [{progress['done']}/{progress['total']}] Done: {event.source_name} ({event.chunks_count} chunks)")

    elif event.event_type == "document_failed":
        progress["failed"] += 1
        # In production: send alert, write to error queue, etc.
        print(f"  [ALERT] {event.source_name} failed at {event.stage}: {event.error}")

    elif event.event_type == "pipeline_completed":
        print(f"\nFinished: {event.successful}/{event.total_documents} in {event.time_ms:.0f}ms")


async def main():
    pipeline = Pipeline(
        source=LocalSource(directory="./docs"),
        chunker=FixedChunker(chunk_size=500, overlap=50),
        embedder=CustomEmbedder(embed_fn=lambda texts: [[0.0] * 384 for _ in texts]),
        destination=JSONDestination("./output.jsonl"),
        on_event=on_event,  # async callback — awaited by the pipeline
    )
    result = await pipeline.run()

    # Inspect per-stage metrics — useful for finding bottlenecks
    print("\n--- Stage Timing ---")
    for stage in ["extraction", "chunking", "embedding", "storage"]:
        if stage in result.metrics:
            s = result.metrics[stage]
            print(f"  {stage:12s}  avg={s['avg_ms']:.1f}ms  min={s['min_ms']:.1f}ms  max={s['max_ms']:.1f}ms")

    if "cache" in result.metrics:
        c = result.metrics["cache"]
        print(f"\n--- Embedding Cache ---")
        print(f"  Hits: {c['hits']}, Misses: {c['misses']}, Hit rate: {c['hit_rate']:.0%}")


if __name__ == "__main__":
    asyncio.run(main())
