"""Compare chunking strategies on the same documents.

This can't be done with the CLI — you'd need two config files and two runs.
Here we extract once and chunk three different ways to compare the results.
"""
import asyncio
from tributary.sources.local_source import LocalSource
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.chunkers.recursive_chunker import RecursiveChunker
from tributary.chunkers.sentence_chunker import SentenceChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline


def dummy_embed(texts: list[str]) -> list[list[float]]:
    return [[0.0] * 384 for _ in texts]


async def run_with_strategy(name: str, chunker, source_dir: str, output: str):
    pipeline = Pipeline(
        source=LocalSource(directory=source_dir),
        chunker=chunker,
        embedder=CustomEmbedder(embed_fn=dummy_embed),
        destination=JSONDestination(output),
    )
    result = await pipeline.run()
    chunks_total = result.metrics.get("chunks", {}).get("total", 0)
    print(f"  {name:20s} → {chunks_total:4d} chunks, {result.time_ms:.0f}ms")


async def main():
    source_dir = "./docs"

    strategies = [
        ("Fixed (500/50)", FixedChunker(chunk_size=500, overlap=50)),
        ("Recursive (500/50)", RecursiveChunker(chunk_size=500, overlap=50)),
        ("Sentence (5/1)", SentenceChunker(sentences_per_chunk=5, overlap_sentences=1)),
    ]

    print("Comparing chunking strategies:")
    for name, chunker in strategies:
        await run_with_strategy(name, chunker, source_dir, f"./output_{name.split()[0].lower()}.jsonl")


if __name__ == "__main__":
    asyncio.run(main())
