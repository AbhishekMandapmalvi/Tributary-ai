"""Conditional pipeline — retry failed documents with a different extractor.

The CLI runs once and reports failures. This script catches failures,
adjusts the approach, and retries — logic that requires Python.
"""
import asyncio
from tributary.sources.local_source import LocalSource
from tributary.extractors.text_extractor import TextExtractor
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline


def dummy_embed(texts: list[str]) -> list[list[float]]:
    return [[0.0] * 384 for _ in texts]


async def main():
    source_dir = "./docs"
    output = "./output.jsonl"

    # First pass: auto-detect extractors by file extension
    pipeline = Pipeline(
        source=LocalSource(directory=source_dir),
        chunker=FixedChunker(chunk_size=500, overlap=50),
        embedder=CustomEmbedder(embed_fn=dummy_embed),
        destination=JSONDestination(output),
    )
    result = await pipeline.run()
    print(f"Pass 1: {result.successful} succeeded, {result.failed} failed")

    if not result.failures:
        return

    # Second pass: retry failures with TextExtractor as fallback
    # (treats any file as plain text — lossy but better than skipping)
    failed_paths = [f.source_name for f in result.failures]
    print(f"Retrying {len(failed_paths)} failures with TextExtractor fallback...")

    retry_pipeline = Pipeline(
        source=LocalSource(file_paths=failed_paths),
        extractor=TextExtractor(),
        chunker=FixedChunker(chunk_size=500, overlap=50),
        embedder=CustomEmbedder(embed_fn=dummy_embed),
        destination=JSONDestination(output),
    )
    retry_result = await retry_pipeline.run()
    print(f"Pass 2: {retry_result.successful} recovered, {retry_result.failed} still failed")

    total_ok = result.successful + retry_result.successful
    total = result.total_documents
    print(f"Final: {total_ok}/{total} documents processed")


if __name__ == "__main__":
    asyncio.run(main())
