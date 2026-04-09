from __future__ import annotations
from typing import Any
from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
from dataclasses import asdict
import asyncio
import json
import aiofiles


class JSONDestination(BaseDestination):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._lock = asyncio.Lock()
        self._file: Any = None

    async def connect(self) -> None:
        if self._file is None:
            self._file = await aiofiles.open(self.file_path, "a")

    async def store(self, results: list[EmbeddingResult]) -> None:
        await self.connect()
        async with self._lock:
            for result in results:
                line = json.dumps(asdict(result))
                await self._file.write(line + "\n")
            await self._file.flush()

    async def close(self) -> None:
        if self._file:
            await self._file.close()
            self._file = None
