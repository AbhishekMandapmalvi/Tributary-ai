import pytest
import asyncio
from tributary.dashboard.server import DashboardServer
from tributary.pipeline.events import PipelineEvent


@pytest.mark.asyncio
async def test_dashboard_server_starts_and_stops():
    server = DashboardServer(port=18765)
    await server.start()
    # Server should be running
    assert server._server_task is not None
    assert not server._server_task.done()
    await server.stop()


@pytest.mark.asyncio
async def test_dashboard_on_event_stores_history():
    server = DashboardServer(port=18766)
    await server.on_event(PipelineEvent(event_type="pipeline_started"))
    await server.on_event(PipelineEvent(event_type="document_started", source_name="a.txt"))
    await server.on_event(PipelineEvent(event_type="document_completed", source_name="a.txt", chunks_count=5))

    assert len(server._event_history) == 3
    assert server._event_history[0]["event_type"] == "pipeline_started"
    assert server._event_history[1]["source_name"] == "a.txt"
    assert server._event_history[2]["chunks_count"] == 5


@pytest.mark.asyncio
async def test_dashboard_on_event_filters_none():
    server = DashboardServer(port=18767)
    await server.on_event(PipelineEvent(event_type="document_started", source_name="a.txt"))

    event = server._event_history[0]
    # Fields that are None should not be in the serialized event
    assert "error" not in event
    assert "stage" not in event
    assert "event_type" in event
    assert "source_name" in event


@pytest.mark.asyncio
async def test_dashboard_with_pipeline(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline

    (tmp_path / "a.txt").write_text("Hello world document.")
    (tmp_path / "b.txt").write_text("Another test document.")
    output = tmp_path / "output.jsonl"

    server = DashboardServer(port=18768)
    # Don't start the HTTP server — just use on_event directly
    pipeline = Pipeline(
        source=LocalSource(directory=str(tmp_path), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=50, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        on_event=server.on_event,
    )
    result = await pipeline.run()

    assert result.successful == 2
    types = [e["event_type"] for e in server._event_history]
    assert "pipeline_started" in types
    assert "document_started" in types
    assert "document_completed" in types
    assert "pipeline_completed" in types


def test_dashboard_cli_help():
    from click.testing import CliRunner
    from tributary.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "--config" in result.output
    assert "--port" in result.output
