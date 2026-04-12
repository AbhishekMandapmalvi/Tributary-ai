# Changelog

All notable changes to Tributary are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-04-12

The **distributed release**. Tributary now scales horizontally across machines
via a pluggable message-queue layer. Single-process mode is unchanged and
remains the default path for small jobs; distributed mode is strictly additive
and opt-in.

### Added

#### Distributed pipeline layer
- **`DistributedPipeline`** — orchestrates a producer + N extraction workers +
  M embedding workers connected by two message queues. Runs on one machine or
  sharded across many — launch the same script on every node to grow the
  worker pool. Handles `SIGINT`/`SIGTERM` with a clean drain (producer stops
  publishing, workers finish in-flight messages, then exit).
- **`ExtractionWorker`** — pulls `DocumentMessage`s from the document queue,
  extracts and chunks, publishes `ChunkMessage`s to the chunk queue. Accepts
  an optional `extractor` parameter; when omitted, auto-detects the right
  extractor per file via `get_extractor_for_extension()`, matching the
  single-process `Pipeline`'s behavior.
- **`EmbeddingWorker`** — pulls `ChunkMessage`s from the chunk queue, embeds,
  writes to the destination. Reuses the same `BaseEmbedder` and
  `BaseDestination` interfaces as single-process mode.
- **`BaseQueue`** — abstract interface with `push`, `poll`, `ack`, `nack`.
  All backends guarantee at-least-once delivery, automatic retries up to
  `max_retries`, and dead-letter routing for poison messages.

#### Six queue backends
- **`RedisQueue`** — self-hosted Redis with explicit DLQ. Uses pending list,
  in-flight sorted set, retry counter hash, and DLQ list. Includes a
  `requeue_expired()` reaper built on an atomic Lua script so multiple
  reapers can run without double-requeuing expired messages.
- **`SQSQueue`** — AWS SQS with native `ApproximateReceiveCount` for retry
  tracking. Configure a redrive policy on the queue for built-in DLQ routing.
- **`RabbitMQQueue`** — RabbitMQ via `aio-pika`, with durable queues,
  persistent messages, and `abandon`/`reject` semantics for retry/DLQ.
- **`PubSubQueue`** — GCP Pub/Sub via `gcloud-aio-pubsub`, with attribute-
  based retry tracking and a `{topic}-dlq` topic for dead letters.
- **`ServiceBusQueue`** — Azure Service Bus with native `delivery_count` and
  built-in dead-letter subqueue (`abandon_message` / `dead_letter_message`).
- **`KafkaQueue`** — Apache Kafka via `aiokafka`, with log-structured retries
  (nack republishes to the same topic with updated headers). Retry order
  not guaranteed — documented caveat for users who need strict ordering.

#### Configuration
- **`get_queue(backend, **params)`** — factory in `tributary.workers` that
  builds any of the six backends by name. Each builder is lazy-imported, so
  a Redis-only deployment doesn't pull in the SQS or Kafka client libraries.
- **`distributed` section in YAML config** — top-level block with
  `document_queue`, `chunk_queue`, optional `extractor`, and worker counts.
  Schema-validated — typos and unknown backends are caught before the
  pipeline starts.
- **Environment variable substitution in YAML** — `${VAR}` and
  `${VAR:-default}` expansion in string values, applied after `extends`
  deep-merge. Works across all config sections, not just `distributed`.
  Non-string scalars (ints, floats, bools) are untouched; unset vars with
  no default are left as literal `${VAR}` so schema validation surfaces them.

#### CLI
- **`tributary run --config <yaml>`** — detects a `distributed` block
  automatically. Single-process configs continue to work unchanged;
  distributed configs launch `DistributedPipeline` with a
  `KeyboardInterrupt`-aware drain handler.

#### Docker Compose
- **`distributed` profile** in `docker-compose.yml` — spins up Redis plus a
  scalable worker service. Run with
  `docker compose --profile distributed up --scale worker=5` to launch 5
  worker containers drawing from the same broker.
- **`examples/distributed/config.docker.yaml`** — Compose-specific config
  pointing at the `redis` service by name.

#### Examples
- **`examples/distributed/`** — one runnable example per backend
  (`redis_example.py`, `sqs_example.py`, `rabbitmq_example.py`,
  `pubsub_example.py`, `servicebus_example.py`, `kafka_example.py`). Each is
  self-contained with prerequisites documented in the module docstring.
- **`examples/distributed/config.yaml`** — YAML-driven distributed pipeline.
- **`examples/distributed/README.md`** — backend comparison table, env-var
  usage notes, and scaling instructions.

#### Installation
New optional dependency groups for each backend:
- `pip install tributary-ai[redis]`
- `pip install tributary-ai[sqs]`
- `pip install tributary-ai[rabbitmq]`
- `pip install tributary-ai[pubsub]`
- `pip install tributary-ai[servicebus]`
- `pip install tributary-ai[kafka]`
- `pip install tributary-ai[distributed]` — all six at once

#### Tests
- 133 new tests (317 → 450) covering messages, all six queue backends, both
  workers, the distributed pipeline wiring, the queue factory, env-var
  substitution, and the extractor auto-detect path.

### Changed
- **Test suite reorganized** into module-mirroring subdirectories
  (`tests/chunkers/`, `tests/sources/`, `tests/workers/`, etc.) so test
  layout matches the source tree. All existing tests still run as-is —
  no test behavior changed.
- **`workers/__init__.py` and `workers/backends/__init__.py`** — added
  (the package was previously using implicit namespace packages). `workers`
  now exports the public API (`BaseMessage`, `DocumentMessage`,
  `ChunkMessage`, `BaseQueue`, `ExtractionWorker`, `EmbeddingWorker`,
  `DistributedPipeline`, `get_queue`). `backends/__init__.py` is
  intentionally empty to avoid triggering lazy imports of optional deps on
  package load.
- **`.env.example`** — added distributed-mode env vars (`REDIS_URL`,
  `DOCUMENT_QUEUE_URL`, `CHUNK_QUEUE_URL`, `RABBITMQ_URL`, `GCP_PROJECT`,
  `SERVICE_BUS_CONNECTION_STRING`, `KAFKA_BOOTSTRAP_SERVERS`).

### Removed
- **Empty stub files** `workers/coordinator.py`, `workers/producer.py`,
  `workers/worker.py` (all were 0-byte placeholders from a scaffold).

### Notes
- **Single-process mode is fully backwards-compatible.** If you don't use
  the `distributed` section, nothing in your pipeline changes.
- **No core dependencies added.** Queue drivers are all optional extras —
  install only the backend you actually use.

## [0.1.2] — Earlier release

Previous changes not documented here. See the git log for details:
`git log --oneline v0.1.2`.

[0.2.0]: https://github.com/AbhishekMandapmalvi/Tributary/releases/tag/v0.2.0
[0.1.2]: https://github.com/AbhishekMandapmalvi/Tributary/releases/tag/v0.1.2
