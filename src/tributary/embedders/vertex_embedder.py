from tributary.embedders.base import BaseEmbedder
from tributary.utils.lazy_import import lazy_import


class VertexAIEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "text-embedding-005", project: str | None = None, location: str = "us-central1"):
        super().__init__(model_name)
        aiplatform = lazy_import("google.cloud.aiplatform", pip_name="google-cloud-aiplatform")
        aiplatform.init(project=project, location=location)
        self.project = project
        self.location = location

    async def embed(self, texts: list[str]) -> list[list[float]]:
        vertexai = lazy_import("vertexai", pip_name="google-cloud-aiplatform")
        model = vertexai.language_models.TextEmbeddingModel.from_pretrained(self.model_name)
        embeddings = model.get_embeddings(texts)
        return [e.values for e in embeddings]
