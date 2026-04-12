import json
import pytest
from tributary.workers.messages import BaseMessage, DocumentMessage, ChunkMessage


def test_base_message_serialize_roundtrip():
    msg = BaseMessage(message_id="test-id", created_at=1000.0, retry_count=2)
    raw = msg.serialize()
    restored = BaseMessage.deserialize(raw)
    assert restored.message_id == "test-id"
    assert restored.created_at == 1000.0
    assert restored.retry_count == 2


def test_document_message_serialize_roundtrip():
    msg = DocumentMessage(
        message_id="doc-1", created_at=2000.0,
        source_type="pdf", source_path="/tmp/a.pdf", source_name="a.pdf",
    )
    raw = msg.serialize()
    restored = BaseMessage.deserialize(raw)
    assert isinstance(restored, DocumentMessage)
    assert restored.source_type == "pdf"
    assert restored.source_path == "/tmp/a.pdf"
    assert restored.source_name == "a.pdf"


def test_chunk_message_serialize_roundtrip():
    msg = ChunkMessage(
        message_id="chunk-1", created_at=3000.0,
        document_id="doc-1", text="hello world",
        source_name="a.pdf", source_path="/tmp/a.pdf",
        chunk_index=0, start_char=0, end_char=11, char_count=11,
    )
    raw = msg.serialize()
    restored = BaseMessage.deserialize(raw)
    assert isinstance(restored, ChunkMessage)
    assert restored.text == "hello world"
    assert restored.chunk_index == 0
    assert restored.char_count == 11


def test_deserialize_from_bytes():
    msg = DocumentMessage(
        message_id="doc-2", created_at=4000.0,
        source_type="txt", source_path="/tmp/b.txt", source_name="b.txt",
    )
    raw_bytes = msg.serialize().encode("utf-8")
    restored = BaseMessage.deserialize(raw_bytes)
    assert isinstance(restored, DocumentMessage)
    assert restored.message_id == "doc-2"


def test_serialize_excludes_raw():
    msg = BaseMessage(message_id="test-raw", created_at=5000.0)
    msg._raw = b"some redis bytes"
    raw = msg.serialize()
    data = json.loads(raw)
    assert "_raw" not in data


def test_serialize_includes_type_tag():
    msg = DocumentMessage(
        message_id="doc-3", created_at=6000.0,
        source_type="md", source_path="/tmp/c.md", source_name="c.md",
    )
    data = json.loads(msg.serialize())
    assert data["__type__"] == "DocumentMessage"


def test_increment_retry():
    msg = BaseMessage()
    assert msg.retry_count == 0
    msg.increment_retry()
    assert msg.retry_count == 1
    msg.increment_retry()
    assert msg.retry_count == 2
