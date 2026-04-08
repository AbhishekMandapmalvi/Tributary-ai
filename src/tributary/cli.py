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

    # Validate registry values if sections exist
    from tributary.sources import _REGISTRY as source_reg
    from tributary.chunkers import _REGISTRY as chunker_reg
    from tributary.embedders import _REGISTRY as embedder_reg
    from tributary.destinations import _REGISTRY as dest_reg

    checks = [
        ("source", "type", source_reg),
        ("chunker", "strategy", chunker_reg),
        ("embedder", "provider", embedder_reg),
        ("destination", "type", dest_reg),
    ]

    for section, key, registry in checks:
        if section in cfg and isinstance(cfg[section], dict) and key in cfg[section]:
            value = cfg[section][key]
            if value not in registry:
                available = ", ".join(sorted(registry))
                errors.append(f"Unknown {section} {key}: '{value}'. Available: {available}")

    return errors


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to YAML config file.")
def validate(config):
    """Validate a config file without running the pipeline."""
    with open(config) as f:
        cfg = yaml.safe_load(f)

    errors = _validate_config(cfg)

    if errors:
        click.echo("Config validation failed:")
        for error in errors:
            click.echo(f"  - {error}")
        raise SystemExit(1)

    click.echo("Config is valid.")


if __name__ == "__main__":
    cli()
