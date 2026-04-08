import asyncio
import click
import yaml
from tributary.sources import get_source
from tributary.chunkers import get_chunker
from tributary.embedders import get_embedder
from tributary.destinations import get_destination
from tributary.pipeline.orchestrator import Pipeline


def _build_pipeline(config: dict) -> Pipeline:
    source = get_source(config["source"]["type"], **config["source"].get("params", {}))
    chunker = get_chunker(config["chunker"]["strategy"], **config["chunker"].get("params", {}))
    embedder = get_embedder(config["embedder"]["provider"], **config["embedder"].get("params", {}))
    destination = get_destination(config["destination"]["type"], **config["destination"].get("params", {}))

    pipeline_params = config.get("pipeline", {})
    return Pipeline(
        source=source,
        chunker=chunker,
        embedder=embedder,
        destination=destination,
        **pipeline_params,
    )


@click.group()
def cli():
    """Tributary — RAG ingestion pipeline."""
    pass


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
def run(config):
    """Run the ingestion pipeline from a YAML config file."""
    with open(config) as f:
        cfg = yaml.safe_load(f)

    pipeline = _build_pipeline(cfg)
    result = asyncio.run(pipeline.run())

    click.echo(f"\nPipeline complete:")
    click.echo(f"  Documents: {result.total_documents}")
    click.echo(f"  Successful: {result.successful}")
    click.echo(f"  Failed: {result.failed}")
    click.echo(f"  Time: {result.time_ms:.0f}ms")

    if result.failures:
        click.echo(f"\nFailures:")
        for failure in result.failures:
            click.echo(f"  {failure.source_name} — {failure.stage}: {failure.error}")

    if result.metrics:
        click.echo(f"\nMetrics:")
        for stage, stats in result.metrics.items():
            if isinstance(stats, dict):
                click.echo(f"  {stage}: {stats}")


if __name__ == "__main__":
    cli()
