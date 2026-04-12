"""Tests for WebhookNotifier."""

import asyncio
from dataclasses import dataclass, field
from time import perf_counter
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tributary.pipeline.webhook import WebhookNotifier
from tributary.pipeline.events import PipelineEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(event_type="document_completed", **kwargs):
    return PipelineEvent(event_type=event_type, **kwargs)


def _mock_aiohttp(*, status=200, raise_on_post=None):
    """Return a mock aiohttp module with a fake ClientSession."""
    mock_response = AsyncMock()
    mock_response.status = status
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    if raise_on_post:
        mock_session.post = MagicMock(side_effect=raise_on_post)
    else:
        mock_session.post = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_timeout = MagicMock()

    mock_module = MagicMock()
    mock_module.ClientSession = MagicMock(return_value=mock_session)
    mock_module.ClientTimeout = MagicMock(return_value=mock_timeout)

    return mock_module, mock_session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWebhookPayload:
    """on_event sends the correct JSON payload."""

    def test_sends_correct_payload(self):
        mock_aiohttp, mock_session = _mock_aiohttp()

        notifier = WebhookNotifier(url="https://example.com/hook")
        event = _make_event(
            source_name="doc.txt",
            stage="extraction",
            chunks_count=5,
        )

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(event)

        # Verify POST was called
        mock_session.post.assert_called_once()
        call_kwargs = mock_session.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")

        assert payload["event_type"] == "document_completed"
        assert payload["source_name"] == "doc.txt"
        assert payload["stage"] == "extraction"
        assert payload["chunks_count"] == 5
        assert "timestamp" in payload
        # None fields should be excluded
        assert "error" not in payload
        assert "total_documents" not in payload

    def test_sends_to_correct_url(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        notifier = WebhookNotifier(url="https://hooks.example.com/abc")
        event = _make_event()

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(event)

        call_args = mock_session.post.call_args
        url = call_args.args[0] if call_args.args else call_args[0][0]
        assert url == "https://hooks.example.com/abc"

    def test_custom_headers_sent(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        headers = {"Authorization": "Bearer secret"}
        notifier = WebhookNotifier(url="https://example.com/hook", headers=headers)
        event = _make_event()

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(event)

        call_kwargs = mock_session.post.call_args
        sent_headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert sent_headers["Authorization"] == "Bearer secret"


class TestEventFiltering:
    """Only matching event types trigger a POST."""

    def test_matching_event_sends(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        notifier = WebhookNotifier(
            url="https://example.com/hook",
            event_types=["pipeline_completed", "document_failed"],
        )

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(_make_event("pipeline_completed"))

        mock_session.post.assert_called_once()

    def test_non_matching_event_skipped(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        notifier = WebhookNotifier(
            url="https://example.com/hook",
            event_types=["pipeline_completed"],
        )

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(_make_event("document_completed"))

        mock_session.post.assert_not_called()

    def test_multiple_events_only_matching_sent(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        notifier = WebhookNotifier(
            url="https://example.com/hook",
            event_types=["document_failed"],
        )

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(_make_event("document_completed"))
            notifier.on_event(_make_event("document_failed"))
            notifier.on_event(_make_event("pipeline_completed"))

        assert mock_session.post.call_count == 1


class TestNoFilter:
    """With no event_types filter, all events are sent."""

    def test_all_events_sent_when_no_filter(self):
        mock_aiohttp, mock_session = _mock_aiohttp()
        notifier = WebhookNotifier(url="https://example.com/hook")

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(_make_event("document_completed"))
            notifier.on_event(_make_event("document_failed"))
            notifier.on_event(_make_event("pipeline_completed"))

        assert mock_session.post.call_count == 3


class TestFailureResilience:
    """Webhook failures must not crash the pipeline."""

    def test_http_error_status_does_not_raise(self):
        mock_aiohttp, mock_session = _mock_aiohttp(status=500)
        notifier = WebhookNotifier(url="https://example.com/hook")

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            # Should not raise
            notifier.on_event(_make_event())

    def test_connection_error_does_not_raise(self):
        mock_aiohttp, _ = _mock_aiohttp(raise_on_post=ConnectionError("refused"))
        notifier = WebhookNotifier(url="https://example.com/hook")

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            # Should not raise
            notifier.on_event(_make_event())

    def test_import_error_does_not_raise(self):
        notifier = WebhookNotifier(url="https://example.com/hook")

        with patch(
            "tributary.pipeline.webhook.lazy_import",
            side_effect=ImportError("aiohttp not installed"),
        ):
            # Should not raise
            notifier.on_event(_make_event())

    def test_timeout_parameter_passed(self):
        mock_aiohttp, _ = _mock_aiohttp()
        notifier = WebhookNotifier(url="https://example.com/hook", timeout=10.0)

        with patch("tributary.pipeline.webhook.lazy_import", return_value=mock_aiohttp):
            notifier.on_event(_make_event())

        mock_aiohttp.ClientTimeout.assert_called_once_with(total=10.0)


class TestBuildPayload:
    """_build_payload drops None fields and includes all non-None fields."""

    def test_full_event(self):
        event = PipelineEvent(
            event_type="pipeline_completed",
            timestamp=123.456,
            total_documents=10,
            successful=9,
            failed=1,
            time_ms=5432.1,
        )
        payload = WebhookNotifier._build_payload(event)

        assert payload == {
            "event_type": "pipeline_completed",
            "timestamp": 123.456,
            "total_documents": 10,
            "successful": 9,
            "failed": 1,
            "time_ms": 5432.1,
        }

    def test_minimal_event(self):
        event = PipelineEvent(event_type="test", timestamp=1.0)
        payload = WebhookNotifier._build_payload(event)

        assert payload == {"event_type": "test", "timestamp": 1.0}
        assert "source_name" not in payload
