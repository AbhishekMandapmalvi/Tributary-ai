"""OpenTelemetry metrics exporter for Tributary pipeline.

Exposes pipeline metrics as OpenTelemetry instruments so they can be
scraped by Prometheus, sent to Datadog/Grafana, or any OTLP-compatible backend.

Usage:
    from tributary.pipeline.otel_exporter import TributaryMetricsExporter

    exporter = TributaryMetricsExporter(service_name="my-rag-pipeline")
    # Pass as event callback to pipeline
    pipeline = Pipeline(..., on_event=exporter.on_event)
"""
from __future__ import annotations
from typing import Any
from tributary.pipeline.events import PipelineEvent
import structlog

logger = structlog.get_logger(__name__)


class TributaryMetricsExporter:
    """Bridges pipeline events to OpenTelemetry metrics.

    Lazily imports opentelemetry — if not installed, logs a warning and no-ops.
    """

    def __init__(self, service_name: str = "tributary") -> None:
        self.service_name = service_name
        self._meter: Any = None
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._setup()

    def _setup(self) -> None:
        try:
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.resources import Resource

            resource = Resource.create({"service.name": self.service_name})
            provider = MeterProvider(resource=resource)
            metrics.set_meter_provider(provider)
            self._meter = metrics.get_meter("tributary")

            self._counters["documents_processed"] = self._meter.create_counter(
                "tributary.documents.processed",
                description="Total documents successfully processed",
            )
            self._counters["documents_failed"] = self._meter.create_counter(
                "tributary.documents.failed",
                description="Total documents that failed processing",
            )
            self._counters["pipeline_runs"] = self._meter.create_counter(
                "tributary.pipeline.runs",
                description="Total pipeline runs completed",
            )
            self._histograms["pipeline_duration"] = self._meter.create_histogram(
                "tributary.pipeline.duration_ms",
                description="Pipeline run duration in milliseconds",
                unit="ms",
            )
            self._histograms["document_chunks"] = self._meter.create_histogram(
                "tributary.document.chunks",
                description="Number of chunks per document",
            )

            logger.info("OpenTelemetry metrics initialized", service_name=self.service_name)

        except ImportError:
            logger.warning("opentelemetry not installed — metrics export disabled. Install with: pip install opentelemetry-sdk")
            self._meter = None

    def on_event(self, event: PipelineEvent) -> None:
        """Sync event callback — pass to Pipeline(on_event=exporter.on_event)."""
        if self._meter is None:
            return

        if event.event_type == "document_completed":
            self._counters["documents_processed"].add(1)
            if event.chunks_count is not None:
                self._histograms["document_chunks"].record(event.chunks_count)

        elif event.event_type == "document_failed":
            self._counters["documents_failed"].add(
                1, {"stage": event.stage or "unknown"}
            )

        elif event.event_type == "pipeline_completed":
            self._counters["pipeline_runs"].add(1)
            if event.time_ms is not None:
                self._histograms["pipeline_duration"].record(event.time_ms)
