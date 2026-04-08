from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
from dataclasses import asdict
import json
import aiofiles


class JSONDestination(BaseDestination):
    def __init__(self, file_path: str):
        self.file_path = file_path

    async def store(self, results: list[EmbeddingResult]) -> None:
        async with aiofiles.open(self.file_path, "a") as f:
            for result in results:
                line = json.dumps(asdict(result))
                await f.write(line + "\n")
