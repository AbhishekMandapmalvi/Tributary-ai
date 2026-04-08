from tributary.embedders.base import BaseEmbedder


class CohereEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "embed-english-v3.0", api_key: str | None = None, input_type: str = "search_document"):
        super().__init__(model_name)
        from cohere import AsyncClientV2
        self.client = AsyncClientV2(api_key=api_key)
        self.input_type = input_type

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embed(
            texts=texts,
            model=self.model_name,
            input_type=self.input_type,
        )
        return [list(v) for v in response.embeddings]  # type: ignore[arg-type]
