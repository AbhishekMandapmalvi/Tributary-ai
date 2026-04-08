from tributary.embedders.base import BaseEmbedder
from tributary.utils.lazy_import import lazy_import


class OpenAIEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "text-embedding-3-small", api_key: str | None = None):
        super().__init__(model_name)
        openai = lazy_import("openai")
        self.client = openai.AsyncOpenAI(api_key=api_key)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embeddings.create(
            input=texts,
            model=self.model_name,
        )
        return [item.embedding for item in response.data]
