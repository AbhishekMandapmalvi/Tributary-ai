from tributary.embedders.base import BaseEmbedder


class VoyageEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "voyage-3", api_key: str | None = None, input_type: str = "document"):
        super().__init__(model_name)
        self.input_type = input_type
        from voyageai import AsyncClient
        self.client = AsyncClient(api_key=api_key)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embed(
            texts,
            model=self.model_name,
            input_type=self.input_type,
        )
        return response.embeddings
