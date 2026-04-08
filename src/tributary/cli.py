import asyncio
import os
import click
import yaml
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from tributary.sources import get_source, _REGISTRY as source_reg
from tributary.chunkers import get_chunker, _REGISTRY as chunker_reg
from tributary.embedders import get_embedder, _REGISTRY as embedder_reg
from tributary.destinations import get_destination, _REGISTRY as dest_reg
from tributary.pipeline.orchestrator import Pipeline
from tributary.pipeline.events import PipelineEvent

console = Console()


def _build_pipeline(config: dict, on_event=None) -> Pipeline:
    from tributary.pipeline.adaptive_batcher import AdaptiveBatcher
    from tributary.pipeline.state_store import StateStore
    from tributary.pipeline.retry import RetryPolicy, DeadLetterQueue

    source = get_source(config["source"]["type"], **config["source"].get("params", {}))
    chunker = get_chunker(config["chunker"]["strategy"], **config["chunker"].get("params", {}))
    embedder = get_embedder(config["embedder"]["provider"], **config["embedder"].get("params", {}))
    destination = get_destination(config["destination"]["type"], **config["destination"].get("params", {}))

    pipeline_params = config.get("pipeline", {})

    # Build objects from nested config sections
    adaptive_cfg = pipeline_params.pop("adaptive_batching", None)
    adaptive_batcher = AdaptiveBatcher(**adaptive_cfg) if adaptive_cfg else None

    state_cfg = pipeline_params.pop("state_store", None)
    state_store = StateStore(**state_cfg) if state_cfg else None

    retry_cfg = pipeline_params.pop("retry_policy", None)
    retry_policy = RetryPolicy(**retry_cfg) if retry_cfg else None

    dlq_cfg = pipeline_params.pop("dead_letter_queue", None)
    dead_letter_queue = DeadLetterQueue(**dlq_cfg) if dlq_cfg else None

    return Pipeline(
        source=source,
        chunker=chunker,
        embedder=embedder,
        destination=destination,
        on_event=on_event,
        adaptive_batcher=adaptive_batcher,
        state_store=state_store,
        retry_policy=retry_policy,
        dead_letter_queue=dead_letter_queue,
        **pipeline_params,
    )


REQUIRED_SECTIONS = {
    "source": ["type"],
    "chunker": ["strategy"],
    "embedder": ["provider"],
    "destination": ["type"],
}


def _validate_config(cfg: dict) -> list[str]:
    """Return a list of error strings. Empty list means valid."""
    errors = []

    if not isinstance(cfg, dict):
        return ["Config file is not a valid YAML mapping."]

    for section, required_keys in REQUIRED_SECTIONS.items():
        if section not in cfg:
            errors.append(f"Missing required section: '{section}'")
            continue
        if not isinstance(cfg[section], dict):
            errors.append(f"Section '{section}' must be a mapping, got {type(cfg[section]).__name__}")
            continue
        for key in required_keys:
            if key not in cfg[section]:
                errors.append(f"Section '{section}' missing required key: '{key}'")

    registries = [
        ("source", "type", source_reg),
        ("chunker", "strategy", chunker_reg),
        ("embedder", "provider", embedder_reg),
        ("destination", "type", dest_reg),
    ]

    for section, key, registry in registries:
        if section in cfg and isinstance(cfg[section], dict) and key in cfg[section]:
            value = cfg[section][key]
            if value not in registry:
                available = ", ".join(sorted(registry))
                errors.append(f"Unknown {section} {key}: '{value}'. Available: {available}")

    return errors


def _print_result_table(result) -> None:
    """Print pipeline results as a rich table."""
    table = Table(title="Pipeline Results", show_header=False, border_style="blue")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Documents", str(result.total_documents))
    table.add_row("Successful", f"[green]{result.successful}[/green]")
    table.add_row("Failed", f"[red]{result.failed}[/red]" if result.failed else "0")
    table.add_row("Time", f"{result.time_ms:.0f}ms")
    console.print(table)


def _print_metrics_table(metrics: dict) -> None:
    """Print per-stage metrics as a rich table."""
    stages = ["extraction", "chunking", "embedding", "storage"]
    stage_metrics = {k: v for k, v in metrics.items() if k in stages}
    if not stage_metrics:
        return

    table = Table(title="Stage Timing", border_style="cyan")
    table.add_column("Stage", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Avg", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Total", justify="right")

    for stage in stages:
        if stage in stage_metrics:
            s = stage_metrics[stage]
            table.add_row(
                stage,
                str(s["count"]),
                f"{s['avg_ms']:.1f}ms",
                f"{s['min_ms']:.1f}ms",
                f"{s['max_ms']:.1f}ms",
                f"{s['total_ms']:.0f}ms",
            )
    console.print(table)

    if "cache" in metrics:
        c = metrics["cache"]
        hit_rate = f"{c['hit_rate']:.0%}" if c['hits'] + c['misses'] > 0 else "N/A"
        console.print(f"  Cache: [green]{c['hits']}[/green] hits, [yellow]{c['misses']}[/yellow] misses, hit rate: {hit_rate}")

    if "chunks" in metrics:
        ch = metrics["chunks"]
        console.print(f"  Chunks: {ch['total']} total, {ch['avg_per_doc']:.1f} avg/doc")


def _print_failures(failures: list) -> None:
    if not failures:
        return
    table = Table(title="Failures", border_style="red")
    table.add_column("Document", style="bold")
    table.add_column("Stage")
    table.add_column("Error", style="red")
    for f in failures:
        table.add_row(f.source_name, f.stage, f.error)
    console.print(table)


# --- Commands ---

@click.group()
def cli():
    """Tributary -- RAG ingestion pipeline."""
    pass


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
def run(config):
    """Run the ingestion pipeline from a YAML config file."""
    with open(config) as f:
        cfg = yaml.safe_load(f)

    errors = _validate_config(cfg)
    if errors:
        console.print("[red]Config validation failed:[/red]")
        for error in errors:
            console.print(f"  [red]-[/red] {error}")
        raise SystemExit(1)

    docs_done = {"n": 0}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Processing documents...", total=None)

        def on_event(event: PipelineEvent) -> None:
            if event.event_type == "document_completed" or event.event_type == "document_failed":
                docs_done["n"] += 1
                progress.update(task, completed=docs_done["n"], description=f"Processed {docs_done['n']} docs...")

        pipeline = _build_pipeline(cfg, on_event=on_event)
        result = asyncio.run(pipeline.run())

    console.print()
    _print_result_table(result)

    if result.metrics:
        console.print()
        _print_metrics_table(result.metrics)

    if result.failures:
        console.print()
        _print_failures(result.failures)


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
def validate(config):
    """Validate a config file without running the pipeline."""
    with open(config) as f:
        cfg = yaml.safe_load(f)

    errors = _validate_config(cfg)

    if errors:
        console.print("[red]Config validation failed:[/red]")
        for error in errors:
            console.print(f"  [red]-[/red] {error}")
        raise SystemExit(1)

    console.print("[green]Config is valid.[/green]")


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
def inspect(config):
    """Show what a config would do without running the pipeline (dry run)."""
    with open(config) as f:
        cfg = yaml.safe_load(f)

    errors = _validate_config(cfg)
    if errors:
        console.print("[red]Config validation failed:[/red]")
        for error in errors:
            console.print(f"  [red]-[/red] {error}")
        raise SystemExit(1)

    table = Table(title="Pipeline Configuration", show_header=False, border_style="blue")
    table.add_column("Component", style="bold")
    table.add_column("Value")

    table.add_row("Source", f"{cfg['source']['type']}")
    src_params = cfg["source"].get("params", {})
    for k, v in src_params.items():
        table.add_row(f"  {k}", str(v))

    table.add_row("Chunker", f"{cfg['chunker']['strategy']}")
    chunk_params = cfg["chunker"].get("params", {})
    for k, v in chunk_params.items():
        table.add_row(f"  {k}", str(v))

    table.add_row("Embedder", f"{cfg['embedder']['provider']}")
    embed_params = cfg["embedder"].get("params", {})
    for k, v in embed_params.items():
        if k == "api_key":
            table.add_row(f"  {k}", "***")
        else:
            table.add_row(f"  {k}", str(v))

    table.add_row("Destination", f"{cfg['destination']['type']}")
    dest_params = cfg["destination"].get("params", {})
    for k, v in dest_params.items():
        table.add_row(f"  {k}", str(v))

    pipeline_cfg = cfg.get("pipeline", {})
    if pipeline_cfg:
        table.add_row("Pipeline", "")
        for k, v in pipeline_cfg.items():
            table.add_row(f"  {k}", str(v))

    console.print(table)

    # Count source files if local
    if cfg["source"]["type"] == "local":
        directory = src_params.get("directory")
        extensions = src_params.get("extensions")
        if directory and os.path.isdir(directory):
            import pathlib
            path = pathlib.Path(directory)
            if extensions:
                files = [f for f in path.rglob("*") if f.is_file() and f.suffix.lower() in extensions]
            else:
                files = [f for f in path.rglob("*") if f.is_file()]
            console.print(f"\n  Found [bold]{len(files)}[/bold] files to process in [cyan]{directory}[/cyan]")


@cli.command()
@click.option("--docs-dir", "-d", required=True, type=click.Path(exists=True), help="Directory with sample documents.")
@click.option("--chunk-size", default=500, help="Chunk size for benchmarking.")
@click.option("--workers", default=3, help="Number of concurrent workers.")
def benchmark(docs_dir, chunk_size, workers):
    """Run throughput benchmarks on sample data."""
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    import tempfile

    console.print(Panel("Tributary Benchmark", border_style="blue"))
    console.print(f"  Source: [cyan]{docs_dir}[/cyan]")
    console.print(f"  Chunk size: {chunk_size}")
    console.print(f"  Workers: {workers}")
    console.print()

    def noop_embed(texts):
        return [[0.0] * 384 for _ in texts]

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
        output_path = tmp.name

    try:
        pipeline = Pipeline(
            source=LocalSource(directory=docs_dir),
            chunker=FixedChunker(chunk_size=chunk_size, overlap=0),
            embedder=CustomEmbedder(embed_fn=noop_embed),
            destination=JSONDestination(output_path),
            max_workers=workers,
        )
        result = asyncio.run(pipeline.run())

        _print_result_table(result)

        if result.metrics:
            console.print()
            _print_metrics_table(result.metrics)

        # Throughput stats
        if result.time_ms > 0 and result.total_documents > 0:
            docs_per_sec = result.total_documents / (result.time_ms / 1000)
            chunks_total = result.metrics.get("chunks", {}).get("total", 0)
            chunks_per_sec = chunks_total / (result.time_ms / 1000) if chunks_total else 0
            console.print()
            console.print(Panel(
                f"[bold]{docs_per_sec:.1f}[/bold] docs/sec  |  [bold]{chunks_per_sec:.1f}[/bold] chunks/sec",
                title="Throughput",
                border_style="green",
            ))
    finally:
        os.unlink(output_path)


@cli.command(name="init")
@click.option("--output", "-o", default="pipeline.yaml", help="Output file path.")
def init_config(output):
    """Interactively scaffold a new pipeline config file."""
    console.print(Panel("Tributary Config Generator", border_style="blue"))

    # Source
    source_types = sorted(source_reg.keys())
    console.print(f"\nAvailable sources: {', '.join(source_types)}")
    source_type = click.prompt("Source type", type=click.Choice(source_types), default="local")

    source_params = {}
    if source_type == "local":
        source_params["directory"] = click.prompt("Directory to scan", default="./docs")
        exts = click.prompt("File extensions (comma-separated, or 'all')", default="all")
        if exts != "all":
            source_params["extensions"] = [e.strip() for e in exts.split(",")]
    elif source_type == "s3":
        source_params["bucket"] = click.prompt("S3 bucket name")
        source_params["prefix"] = click.prompt("Key prefix", default="")
    elif source_type == "gcs":
        source_params["bucket"] = click.prompt("GCS bucket name")
        source_params["prefix"] = click.prompt("Key prefix", default="")
    elif source_type == "web":
        urls = click.prompt("URLs (comma-separated)")
        source_params["urls"] = [u.strip() for u in urls.split(",")]

    # Chunker
    chunker_types = sorted(chunker_reg.keys())
    console.print(f"\nAvailable chunkers: {', '.join(chunker_types)}")
    chunker_type = click.prompt("Chunking strategy", type=click.Choice(chunker_types), default="recursive")

    chunker_params = {}
    if chunker_type in ("fixed", "recursive", "token"):
        chunker_params["chunk_size"] = click.prompt("Chunk size", type=int, default=500)
        chunker_params["overlap"] = click.prompt("Overlap", type=int, default=50)
    elif chunker_type == "sentence":
        chunker_params["sentences_per_chunk"] = click.prompt("Sentences per chunk", type=int, default=5)
        chunker_params["overlap_sentences"] = click.prompt("Overlap sentences", type=int, default=1)
    elif chunker_type == "sliding_window":
        chunker_params["window_size"] = click.prompt("Window size", type=int, default=500)
        chunker_params["step_size"] = click.prompt("Step size", type=int, default=100)

    # Embedder
    embedder_types = sorted(embedder_reg.keys())
    console.print(f"\nAvailable embedders: {', '.join(embedder_types)}")
    embedder_type = click.prompt("Embedder provider", type=click.Choice(embedder_types), default="openai")

    embedder_params = {}
    if embedder_type == "openai":
        embedder_params["api_key"] = click.prompt("OpenAI API key (or set OPENAI_API_KEY env var)", default="", show_default=False)
        if not embedder_params["api_key"]:
            del embedder_params["api_key"]
    elif embedder_type == "cohere":
        embedder_params["api_key"] = click.prompt("Cohere API key", default="", show_default=False)
        if not embedder_params["api_key"]:
            del embedder_params["api_key"]

    # Destination
    dest_types = sorted(dest_reg.keys())
    console.print(f"\nAvailable destinations: {', '.join(dest_types)}")
    dest_type = click.prompt("Destination type", type=click.Choice(dest_types), default="json")

    dest_params = {}
    if dest_type == "json":
        dest_params["file_path"] = click.prompt("Output file path", default="./output.jsonl")
    elif dest_type == "pinecone":
        dest_params["index_name"] = click.prompt("Pinecone index name")
    elif dest_type == "qdrant":
        dest_params["collection_name"] = click.prompt("Qdrant collection name")
        dest_params["url"] = click.prompt("Qdrant URL", default="http://localhost:6333")
    elif dest_type == "chroma":
        dest_params["collection_name"] = click.prompt("ChromaDB collection name")
        persist = click.prompt("Persist path (or empty for in-memory)", default="", show_default=False)
        if persist:
            dest_params["persist_path"] = persist

    # Pipeline settings
    max_workers = click.prompt("\nMax workers", type=int, default=3)
    batch_size = click.prompt("Batch size", type=int, default=256)

    config = {
        "source": {"type": source_type, "params": source_params},
        "chunker": {"strategy": chunker_type, "params": chunker_params},
        "embedder": {"provider": embedder_type, "params": embedder_params},
        "destination": {"type": dest_type, "params": dest_params},
        "pipeline": {"max_workers": max_workers, "batch_size": batch_size},
    }

    with open(output, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[green]Config written to {output}[/green]")
    console.print(f"Run with: [bold]tributary run --config {output}[/bold]")


@cli.command(name="cost-estimate")
@click.option("--docs-dir", "-d", required=True, type=click.Path(exists=True), help="Directory with documents.")
@click.option("--model", "-m", default="text-embedding-3-small", help="Embedding model name.")
@click.option("--chunk-size", default=500, help="Chunk size.")
@click.option("--overlap", default=50, help="Chunk overlap.")
def cost_estimate(docs_dir, model, chunk_size, overlap):
    """Estimate embedding API costs without running the pipeline."""
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.extractors import get_extractor_for_extension
    from tributary.pipeline.cost_estimator import estimate_cost, DEFAULT_PRICING
    import pathlib

    source = LocalSource(directory=docs_dir)
    chunker = FixedChunker(chunk_size=chunk_size, overlap=overlap)

    all_chunks: list[str] = []
    doc_count = 0

    async def collect():
        nonlocal doc_count
        async for sr in source.fetch():
            try:
                extractor = get_extractor_for_extension(sr.file_name)
                result = await extractor.extract(sr.raw_bytes, sr.file_name)
                chunks = chunker.chunk(result.text, sr.file_name)
                all_chunks.extend(c.text for c in chunks)
                doc_count += 1
            except Exception:
                pass

    asyncio.run(collect())

    est = estimate_cost(all_chunks, model_name=model)

    table = Table(title="Cost Estimate", show_header=False, border_style="green")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Documents", str(doc_count))
    table.add_row("Chunks", str(est.document_count))
    table.add_row("Total characters", f"{est.total_characters:,}")
    table.add_row("Estimated tokens", f"{est.estimated_tokens:,}")
    table.add_row("Model", est.model_name)
    table.add_row("Price / 1M tokens", f"${est.price_per_million_tokens:.2f}")
    table.add_row("Estimated cost", f"[bold green]${est.estimated_cost_usd:.4f}[/bold green]")
    console.print(table)

    if est.price_per_million_tokens == 0:
        console.print(f"\n[yellow]No pricing found for '{model}'. Known models: {', '.join(sorted(DEFAULT_PRICING))}[/yellow]")


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--port", "-p", default=8765, help="Dashboard port.")
@click.option("--host", default="127.0.0.1", help="Dashboard host.")
def dashboard(config, port, host):
    """Run the pipeline with a real-time web dashboard."""
    from tributary.dashboard.server import DashboardServer

    with open(config) as f:
        cfg = yaml.safe_load(f)

    errors = _validate_config(cfg)
    if errors:
        console.print("[red]Config validation failed:[/red]")
        for error in errors:
            console.print(f"  [red]-[/red] {error}")
        raise SystemExit(1)

    async def run_with_dashboard():
        server = DashboardServer(host=host, port=port)
        pipeline = _build_pipeline(cfg, on_event=server.on_event)
        await server.start()
        console.print(f"  Dashboard: [bold cyan]http://{host}:{port}[/bold cyan]")
        console.print(f"  Open in browser to see live progress.\n")

        result = asyncio.run(pipeline.run()) if False else await pipeline.run()

        # Keep dashboard alive briefly so user can see final state
        console.print("\n  Pipeline finished. Dashboard still running — press Ctrl+C to exit.")
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await server.stop()

        return result

    try:
        result = asyncio.run(run_with_dashboard())
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")
        return

    console.print()
    _print_result_table(result)


if __name__ == "__main__":
    cli()
