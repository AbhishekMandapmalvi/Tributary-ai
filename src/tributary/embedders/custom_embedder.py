from tributary.embedders.base import BaseEmbedder
from collections.abc import Callable
import asyncio


class CustomEmbedder(BaseEmbedder):
    def __init__(self, embed_fn: Callable[[list[str]], list[list[float]]], model_name: str = "custom"):
        super().__init__(model_name)
        self.embed_fn = embed_fn

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if asyncio.iscoroutinefunction(self.embed_fn):
            return await self.embed_fn(texts)
        return self.embed_fn(texts)
