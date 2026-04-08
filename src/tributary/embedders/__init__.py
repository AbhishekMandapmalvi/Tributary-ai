from tributary.embedders.base import BaseEmbedder
from tributary.embedders.models import EmbeddingResult
from tributary.embedders.openai_embedder import OpenAIEmbedder
from tributary.embedders.cohere_embedder import CohereEmbedder
from tributary.embedders.vertex_embedder import VertexAIEmbedder
from tributary.embedders.bedrock_embedder import BedrockEmbedder
from tributary.embedders.voyage_embedder import VoyageEmbedder
from tributary.embedders.custom_embedder import CustomEmbedder

_REGISTRY = {
    "openai": OpenAIEmbedder,
    "cohere": CohereEmbedder,
    "vertex": VertexAIEmbedder,
    "bedrock": BedrockEmbedder,
    "voyage": VoyageEmbedder,
    "custom": CustomEmbedder,
}


def get_embedder(provider: str, **kwargs) -> BaseEmbedder:
    embedder_cls = _REGISTRY.get(provider)
    if embedder_cls is None:
        raise ValueError(
            f"Unknown embedder provider: '{provider}'. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return embedder_cls(**kwargs)
