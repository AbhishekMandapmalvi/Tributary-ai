# Distributed Pipeline Examples

One example per supported queue backend. All of them wire the same
`DistributedPipeline` — a producer, N extraction workers, and M embedding
workers — to a different broker.

| Backend | Example | Spin-up cost | Best for |
|---------|---------|--------------|----------|
| Redis | [`redis_example.py`](redis_example.py) | `docker run redis` | Self-hosted, low latency, simple ops |
| AWS SQS | [`sqs_example.py`](sqs_example.py) | AWS account + queues | AWS-native, managed, built-in DLQ |
| RabbitMQ | [`rabbitmq_example.py`](rabbitmq_example.py) | `docker run rabbitmq` | AMQP ecosystem, routing, durable queues |
| GCP Pub/Sub | [`pubsub_example.py`](pubsub_example.py) | GCP project + topics | GCP-native, globally scalable |
| Azure Service Bus | [`servicebus_example.py`](servicebus_example.py) | Azure namespace + queues | Azure-native, native delivery-count + DLQ |
| Apache Kafka | [`kafka_example.py`](kafka_example.py) | `docker run kafka` | Log-structured, replay, very high throughput |

Each example is self-contained and runnable once its prerequisites are met.
Prerequisites are documented in the module docstring at the top of every file.

## Environment variables

Copy [`.env.example`](../../.env.example) at the repo root to `.env` and fill in
the values you need. Each example reads what it requires via `os.getenv()`;
unused backends can be left blank.

## YAML-driven configuration

[`config.yaml`](config.yaml) shows how to define the whole distributed
pipeline — including queue backends — in a single config file, runnable with:

```bash
tributary run --config examples/distributed/config.yaml
```

Swap the `distributed.document_queue.backend` and `chunk_queue.backend`
fields to `sqs`, `rabbitmq`, `pubsub`, `servicebus`, or `kafka` — the schema
validates every supported backend name and rejects typos before the pipeline
starts.

## Scaling out across machines

Each example runs one producer and a pool of workers in a single process.
To scale horizontally, launch the same script on additional machines pointing
at the same broker — the extra processes automatically join the worker pool
drawing from the shared queues. No coordination code needed.

## Graceful shutdown

Every example handles `SIGINT`/`SIGTERM`:

1. Producer stops fetching new documents from the source.
2. Workers finish their in-flight messages.
3. Each worker's `run()` loop exits when the queue is empty.
4. Process exits cleanly.

Ctrl-C in a terminal does the right thing — no partially-processed work is
lost, and in-flight messages are returned to the queue for another worker to
pick up (see the Redis message-lifecycle diagram in the main README).
