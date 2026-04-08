from tributary.embedders.base import BaseEmbedder


class VertexAIEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "text-embedding-005", project: str | None = None, location: str = "us-central1"):
        super().__init__(model_name)
        from google.cloud import aiplatform
        aiplatform.init(project=project, location=location)
        self.project = project
        self.location = location

    async def embed(self, texts: list[str]) -> list[list[float]]:
        from vertexai.language_models import TextEmbeddingModel
        model = TextEmbeddingModel.from_pretrained(self.model_name)
        embeddings = model.get_embeddings(texts)
        return [e.values for e in embeddings]
