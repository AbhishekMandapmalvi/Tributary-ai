"""Webhook notifier for pipeline events.

Sends JSON payloads to a configured HTTP endpoint when pipeline events occur.
Uses aiohttp for async HTTP requests. Failures are logged but never crash the pipeline.
"""

import asyncio
import dataclasses
import structlog

from tributary.utils.lazy_import import lazy_import

logger = structlog.get_logger(__name__)


class WebhookNotifier:
    """POSTs pipeline events as JSON to a webhook URL.

    Args:
        url: The webhook endpoint URL.
        headers: Optional dict of HTTP headers to include.
        event_types: List of event type strings to filter on.
            If empty or None, all events are sent.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        event_types: list[str] | None = None,
        timeout: float = 5.0,
    ) -> None:
        self.url = url
        self.headers = headers or {}
        self.event_types = set(event_types) if event_types else None
        self.timeout = timeout

    def on_event(self, event) -> None:
        """Pipeline callback — fires an async POST without blocking.

        If there is no running event loop, creates one for the request.
        Errors are caught and logged as warnings so the pipeline continues.
        """
        if self.event_types and event.event_type not in self.event_types:
            return

        payload = self._build_payload(event)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            loop.create_task(self._send(payload))
        else:
            asyncio.run(self._send(payload))

    async def _send(self, payload: dict) -> None:
        """POST the JSON payload to the webhook URL."""
        try:
            aiohttp = lazy_import("aiohttp", pip_name="aiohttp")
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.url,
                    json=payload,
                    headers=self.headers,
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(
                            "Webhook returned error status",
                            url=self.url,
                            status=resp.status,
                        )
        except Exception as exc:
            logger.warning(
                "Webhook request failed",
                url=self.url,
                error=str(exc),
            )

    @staticmethod
    def _build_payload(event) -> dict:
        """Convert a PipelineEvent to a JSON-serialisable dict, dropping None fields."""
        data = dataclasses.asdict(event)
        return {k: v for k, v in data.items() if v is not None}
