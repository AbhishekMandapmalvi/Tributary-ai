"""Persistent state tracking for idempotent restart, checkpointing, and deduplication.

Tracks which documents have been processed by content hash so the pipeline
can resume after a crash without reprocessing completed documents.
"""
from __future__ import annotations
import hashlib
import json
import asyncio
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)


class StateStore:
    def __init__(self, path: str = ".tributary_state.json") -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._processed: dict[str, str] = {}  # content_hash -> source_name
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                self._processed = data.get("processed", {})
                logger.info("State loaded", processed_count=len(self._processed), path=str(self._path))
            except (json.JSONDecodeError, KeyError):
                logger.warning("Corrupt state file, starting fresh", path=str(self._path))
                self._processed = {}

    async def checkpoint(self) -> None:
        """Save current state to disk."""
        async with self._lock:
            data = {"processed": self._processed}
            self._path.write_text(json.dumps(data))

    def is_processed(self, content_hash: str) -> bool:
        return content_hash in self._processed

    async def mark_processed(self, content_hash: str, source_name: str) -> None:
        async with self._lock:
            self._processed[content_hash] = source_name

    async def clear(self) -> None:
        async with self._lock:
            self._processed = {}
            if self._path.exists():
                self._path.unlink()

    @property
    def processed_count(self) -> int:
        return len(self._processed)

    @staticmethod
    def hash_content(raw_bytes: bytes) -> str:
        return hashlib.sha256(raw_bytes).hexdigest()
