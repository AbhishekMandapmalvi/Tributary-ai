from tributary.destinations.base import BaseDestination
from tributary.destinations.json_destination import JSONDestination
from tributary.destinations.pinecone_destination import PineconeDestination
from tributary.destinations.qdrant_destination import QdrantDestination
from tributary.destinations.chroma_destination import ChromaDestination
from tributary.destinations.pgvector_destination import PgvectorDestination
from tributary.destinations.multi_destination import MultiDestination

_REGISTRY = {
    "json": JSONDestination,
    "pinecone": PineconeDestination,
    "qdrant": QdrantDestination,
    "chroma": ChromaDestination,
    "pgvector": PgvectorDestination,
}


def get_destination(destination_type: str, **kwargs) -> BaseDestination:
    dest_cls = _REGISTRY.get(destination_type)
    if dest_cls is None:
        raise ValueError(
            f"Unknown destination type: '{destination_type}'. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return dest_cls(**kwargs)
