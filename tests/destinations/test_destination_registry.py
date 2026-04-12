import pytest
from unittest.mock import patch, MagicMock
from tributary.destinations import get_destination
from tributary.destinations.json_destination import JSONDestination
from tributary.destinations.pinecone_destination import PineconeDestination
from tributary.destinations.qdrant_destination import QdrantDestination
from tributary.destinations.chroma_destination import ChromaDestination
from tributary.destinations.pgvector_destination import PgvectorDestination
import tributary.destinations.pinecone_destination as pinecone_module


def test_json(tmp_path):
    dest = get_destination("json", file_path=str(tmp_path / "out.jsonl"))
    assert isinstance(dest, JSONDestination)


def test_pinecone():
    with patch("pinecone.Pinecone"):
        dest = get_destination("pinecone", index_name="test", api_key="fake")
    assert isinstance(dest, PineconeDestination)


def test_qdrant():
    dest = get_destination("qdrant", collection_name="test")
    assert isinstance(dest, QdrantDestination)


def test_chroma():
    dest = get_destination("chroma", collection_name="test")
    assert isinstance(dest, ChromaDestination)


def test_pgvector():
    dest = get_destination("pgvector", dsn="postgresql://localhost/test")
    assert isinstance(dest, PgvectorDestination)


def test_unknown_raises():
    with pytest.raises(ValueError, match="Unknown destination type"):
        get_destination("redis")
