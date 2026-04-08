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
# Scaffold a new config interactively
tributary init --output pipeline.yaml

# Validate config without running
tributary validate --config pipeline.yaml

# Dry run — show what the config would do
tributary inspect --config pipeline.yaml

# Run the pipeline (with progress bar and rich output)
tributary run --config pipeline.yaml

# Benchmark throughput on sample data
tributary benchmark --docs-dir ./docs --chunk-size 500 --workers 3

# Estimate embedding API costs
tributary cost-estimate --docs-dir ./docs --model text-embedding-3-small

# Run with real-time web dashboard
tributary dashboard --config pipeline.yaml --port 8765
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
- **Connection pooling** — all destinations use persistent connections via `connect()`/`close()` lifecycle, initialized once and reused across all batches
- **Lazy dependency loading** — optional packages (OpenAI, Pinecone, PyMuPDF, etc.) are only loaded when used, with interactive install prompts for missing dependencies

---

## Reliability & Resilience

```python
from tributary.pipeline.state_store import StateStore
from tributary.pipeline.retry import RetryPolicy, DeadLetterQueue

pipeline = Pipeline(
    source=..., chunker=..., embedder=..., destination=...,
    state_store=StateStore(".tributary_state.json"),
    retry_policy=RetryPolicy(max_retries=3, base_delay=1.0),
    dead_letter_queue=DeadLetterQueue(".tributary_dlq.jsonl"),
    checkpoint_interval=10,
)
```

| Feature | How it works |
|---------|--------------|
| **Document deduplication** | SHA-256 hash of content — already-processed documents are skipped on restart |
| **Idempotent restart** | `StateStore` loads from disk on init, pipeline resumes where it left off |
| **Retry with exponential backoff** | Failed documents retry up to `max_retries` times with `base_delay * 2^attempt` delay |
| **Dead-letter queue** | After all retries exhausted, failed documents are persisted to a JSONL file for inspection |
| **Checkpointing** | State saved to disk every N documents (configurable via `checkpoint_interval`) |
| **Graceful shutdown** | SIGINT/SIGTERM stops fetching new documents, finishes current work, saves state |

All resilience features are opt-in. Pass nothing and the pipeline works exactly as before.

---

## Observability

**Correlation IDs** — each document gets a unique 12-character ID that flows through every log line, making it easy to trace a single document across extraction, chunking, embedding, and storage stages.

**Cost estimation** — estimate embedding API costs before running:

```bash
tributary cost-estimate --docs-dir ./docs --model text-embedding-3-small
```

```python
from tributary.pipeline.cost_estimator import estimate_cost

est = estimate_cost(chunks, model_name="text-embedding-3-small")
print(f"~{est.estimated_tokens:,} tokens, ~${est.estimated_cost_usd:.4f}")
```

**OpenTelemetry export** — bridge pipeline events to Prometheus/Grafana/Datadog:

```python
from tributary.pipeline.otel_exporter import TributaryMetricsExporter

exporter = TributaryMetricsExporter(service_name="my-pipeline")
pipeline = Pipeline(..., on_event=exporter.on_event)
```

Exposes counters (`documents.processed`, `documents.failed`, `pipeline.runs`) and histograms (`pipeline.duration_ms`, `document.chunks`). Requires `opentelemetry-sdk` — gracefully no-ops if not installed.

**Real-time dashboard** — browser-based live view of pipeline progress:

```bash
tributary dashboard --config pipeline.yaml --port 8765
```

Opens a web dashboard at `http://localhost:8765` showing live document count, success/failure rates, docs/sec throughput, event log, and failure details — all streamed via WebSocket.

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
pytest -v  # 202 tests passing
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
