"""Retry with exponential backoff and dead-letter queue for failed documents."""
from __future__ import annotations
import asyncio
import json
from pathlib import Path
from dataclasses import asdict
from tributary.sources.models import SourceResult
import structlog

logger = structlog.get_logger(__name__)


class RetryPolicy:
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 30.0) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def delay_for_attempt(self, attempt: int) -> float:
        """Exponential backoff: base_delay * 2^attempt, capped at max_delay."""
        delay = self.base_delay * (2 ** attempt)
        return min(delay, self.max_delay)

    def should_retry(self, attempt: int) -> bool:
        return attempt < self.max_retries


class DeadLetterQueue:
    """Persists failed documents to a JSONL file for later inspection or retry."""

    def __init__(self, path: str = ".tributary_dlq.jsonl") -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()

    async def push(self, source_name: str, stage: str, error: str, attempt: int) -> None:
        async with self._lock:
            entry = {
                "source_name": source_name,
                "stage": stage,
                "error": error,
                "attempts": attempt + 1,
            }
            with open(self._path, "a") as f:
                f.write(json.dumps(entry) + "\n")
            logger.warning("Document sent to dead-letter queue", source_name=source_name, attempts=attempt + 1)

    def read_all(self) -> list[dict]:
        if not self._path.exists():
            return []
        entries = []
        for line in self._path.read_text().strip().split("\n"):
            if line:
                entries.append(json.loads(line))
        return entries

    async def clear(self) -> None:
        async with self._lock:
            if self._path.exists():
                self._path.unlink()

    @property
    def count(self) -> int:
        if not self._path.exists():
            return 0
        return sum(1 for line in self._path.read_text().strip().split("\n") if line)
