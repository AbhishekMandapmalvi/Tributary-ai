# Tributary

**A lightweight, high-concurrency data ingestion and chunking pipeline for Retrieval-Augmented Generation (RAG) systems.**

---

## The Problem

Building a RAG pipeline means wiring together document loading, text extraction, chunking, embedding, and vector storage — each with different APIs, async patterns, and failure modes. Most teams end up with brittle scripts that process files sequentially, can't handle mixed formats, and break silently when one document fails.

Tributary handles the plumbing so you can focus on your data. It processes documents concurrently, auto-detects file formats, batches embedding API calls, caches duplicate chunks, and reports exactly what failed and why.

---

## Installation

```bash
pip install tributary
```

---

## Quickstart

```python
import asyncio
from tributary.sources.local_source import LocalSource
from tributary.chunkers.fixed_chunker import FixedChunker
from tributary.embedders.custom_embedder import CustomEmbedder
from tributary.destinations.json_destination import JSONDestination
from tributary.pipeline.orchestrator import Pipeline

pipeline = Pipeline(
    source=LocalSource(directory="./docs", extensions=[".txt", ".md", ".pdf"]),
    chunker=FixedChunker(chunk_size=500, overlap=50),
    embedder=CustomEmbedder(embed_fn=lambda texts: [[0.0] * 384 for _ in texts]),
    destination=JSONDestination("output.jsonl"),
)

result = asyncio.run(pipeline.run())
print(f"Processed {result.successful}/{result.total_documents} documents in {result.time_ms:.0f}ms")
print(f"Metrics: {result.metrics}")
```

---

## CLI

```bash
# Validate config without running
tributary validate --config pipeline.yaml

# Run the pipeline
tributary run --config pipeline.yaml
```

Example `pipeline.yaml`:

```yaml
source:
  type: local
  params:
    directory: ./docs
    extensions: [".txt", ".md", ".pdf"]

chunker:
  strategy: recursive
  params:
    chunk_size: 500
    overlap: 50

embedder:
  provider: openai
  params:
    api_key: your-api-key-here

destination:
  type: json
  params:
    file_path: ./output.jsonl

pipeline:
  max_workers: 3
  batch_size: 256
```

For custom embedding functions, event callbacks, or multi-pass logic, use the Python API directly — see [examples/](examples/).

---

## Architecture

```
Source              Extractor          Chunker              Embedder          Destination
──────              ─────────          ───────              ────────          ───────────
LocalSource         TextExtractor      FixedChunker         OpenAIEmbedder    JSONDestination
S3Source            MarkdownExtractor  RecursiveChunker     CohereEmbedder    PineconeDestination
GCSSource           HTMLExtractor      SentenceChunker      CustomEmbedder    QdrantDestination
AzureBlobSource     CSVExtractor       TokenBasedChunker                      ChromaDestination
WebScraperSource    JSONExtractor      SlidingWindowChunker                   PgvectorDestination
                    PDFExtractor

    fetch() ────> extract() ────> chunk() ────> embed() ────> store()
         │             │              │             │             │
     async gen     auto-detect     thread        batched      concurrent
     + backpres    by extension    offloaded     + cached     + locked
```

---

## Feature Matrix

### Sources

| Source | Description | Auth |
|--------|-------------|------|
| `LocalSource` | Local filesystem, files or directories | None |
| `S3Source` | AWS S3 buckets with prefix filtering | AWS credentials |
| `GCSSource` | Google Cloud Storage with pagination | GCP credentials |
| `AzureBlobSource` | Azure Blob Storage containers | Connection string or account URL |
| `WebScraperSource` | HTTP/HTTPS URLs with concurrency control | None |

### Extractors

| Format | Extractor | Library |
|--------|-----------|---------|
| `.txt` | `TextExtractor` | Built-in |
| `.md`, `.markdown` | `MarkdownExtractor` | markdown-it-py |
| `.html`, `.htm` | `HTMLExtractor` | BeautifulSoup4 + lxml |
| `.csv`, `.tsv` | `CSVExtractor` | Built-in csv module |
| `.json` | `JSONExtractor` | Built-in json module |
| `.pdf` | `PDFExtractor` | PyMuPDF |

Extractors are auto-detected by file extension. All handle UTF-8 with Latin-1 fallback and log encoding issues with structlog.

### Chunkers

| Strategy | Class | Key Parameters |
|----------|-------|----------------|
| Fixed-size | `FixedChunker` | `chunk_size`, `overlap` |
| Recursive | `RecursiveChunker` | `chunk_size`, `overlap`, `separators` |
| Sentence | `SentenceChunker` | `sentences_per_chunk`, `overlap_sentences` |
| Token-based | `TokenBasedChunker` | `chunk_size`, `overlap`, `tokenizer` |
| Sliding window | `SlidingWindowChunker` | `window_size`, `step_size` |

### Embedders

| Provider | Class | Default Model |
|----------|-------|---------------|
| OpenAI | `OpenAIEmbedder` | `text-embedding-3-small` |
| Cohere | `CohereEmbedder` | `embed-english-v3.0` |
| Custom function | `CustomEmbedder` | Any sync/async callable |

Need a different provider? `CustomEmbedder` accepts any sync or async function, so you can plug in Vertex AI, Bedrock, Voyage AI, or any other embedding API in one line.

### Destinations

| Destination | Class | Type |
|-------------|-------|------|
| JSON Lines | `JSONDestination` | File |
| Pinecone | `PineconeDestination` | Managed cloud |
| Qdrant | `QdrantDestination` | Self-hosted / cloud |
| ChromaDB | `ChromaDestination` | In-memory / local |
| pgvector | `PgvectorDestination` | PostgreSQL extension |

---

## Performance Features

- **Concurrent workers** — N workers process documents in parallel via asyncio producer-consumer queue
- **Backpressure** — bounded queue (`queue_size`) pauses the producer when workers fall behind
- **Batched embedding** — chunks are grouped into configurable batches (default 256) to minimize API round trips
- **Concurrent embedding** — multiple embedding batches fire simultaneously per worker, controlled by semaphore (`max_concurrent_embeds`)
- **LRU embedding cache** — duplicate chunks (shared headers/footers) are embedded once and cached via `OrderedDict`
- **Thread-offloaded chunking** — CPU-bound chunking runs in `asyncio.to_thread` to avoid blocking the event loop
- **Per-stage metrics** — extraction, chunking, embedding, and storage are individually timed with min/avg/max stats
- **Event callbacks** — sync or async hooks for `pipeline_started`, `document_started`, `document_completed`, `document_failed`, `pipeline_completed`

---

## Examples

The [examples/](examples/) directory shows things the CLI can't do:

| Example | What it demonstrates |
|---------|---------------------|
| [`local_to_json.py`](examples/local_to_json.py) | Compare 3 chunking strategies on the same documents |
| [`pdf_recursive_chunking.py`](examples/pdf_recursive_chunking.py) | Two-pass pipeline with automatic retry on failures |
| [`with_events_and_metrics.py`](examples/with_events_and_metrics.py) | Live progress callbacks and per-stage performance analysis |

---

## Tests

```bash
pytest -v  # 165 tests passing
```

---

## Design Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| **Strategy** | Chunkers, Embedders, Destinations | Swap algorithms without changing the pipeline |
| **Factory + Registry** | `get_source()`, `get_chunker()`, `get_embedder()`, `get_destination()`, `get_extractor_for_extension()` | Create components by name string |
| **Producer-Consumer** | Pipeline orchestrator | Decouple document fetching from processing |
| **Abstract Base Class** | `BaseSource`, `BaseExtractor`, `BaseChunker`, `BaseEmbedder`, `BaseDestination` | Enforce interface contracts |
| **Template Method** | `BaseEmbedder.embed_chunks()` wraps `embed()` | Base class handles caching + metadata, subclass handles vectors |
| **Observer** | `on_event` callback | Monitor pipeline progress without coupling |
