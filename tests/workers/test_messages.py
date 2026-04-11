import pytest
import time
from uuid import UUID
from tributary.workers.messages import BaseMessage, DocumentMessage, ChunkMessage

class TestBaseMessage:
    """Unit tests for BaseMessage dataclass."""
    
    def test_base_message_default_initialization(self):
        """Test BaseMessage initializes with auto-generated UUID and timestamp."""
        msg = BaseMessage()
        assert isinstance(msg.message_id, str)
        assert UUID(msg.message_id)  # Valid UUID format
        assert isinstance(msg.created_at, float)
        assert msg.created_at > 0
        assert msg.retry_count == 0
    
    def test_base_message_unique_ids(self):
        """Test each BaseMessage gets a unique message_id."""
        msg1 = BaseMessage()
        msg2 = BaseMessage()
        assert msg1.message_id != msg2.message_id
    
    def test_base_message_timestamp_progression(self):
        """Test timestamps are monotonically increasing."""
        msg1 = BaseMessage()
        time.sleep(0.01)
        msg2 = BaseMessage()
        assert msg2.created_at >= msg1.created_at
    
    def test_base_message_explicit_fields(self):
        """Test BaseMessage accepts explicit field values."""
        msg_id = "test-id-123"
        timestamp = 1234567890.0
        msg = BaseMessage(message_id=msg_id, created_at=timestamp, retry_count=2)
        assert msg.message_id == msg_id
        assert msg.created_at == timestamp
        assert msg.retry_count == 2
    
    def test_increment_retry_increments_count(self):
        """Test increment_retry() increments the counter."""
        msg = BaseMessage()
        assert msg.retry_count == 0
        msg.increment_retry()
        assert msg.retry_count == 1
        msg.increment_retry()
        assert msg.retry_count == 2
    
    def test_increment_retry_multiple_calls(self):
        """Test multiple consecutive retry increments."""
        msg = BaseMessage(retry_count=5)
        for _ in range(10):
            msg.increment_retry()
        assert msg.retry_count == 15


class TestDocumentMessage:
    """Unit tests for DocumentMessage dataclass."""
    
    def test_document_message_initialization(self):
        """Test DocumentMessage initializes with required and inherited fields."""
        msg = DocumentMessage(
            source_type="pdf",
            source_path="/path/to/doc.pdf",
            source_name="doc.pdf"
        )
        assert msg.source_type == "pdf"
        assert msg.source_path == "/path/to/doc.pdf"
        assert msg.source_name == "doc.pdf"
        assert isinstance(msg.message_id, str)
        assert msg.retry_count == 0
    
    def test_document_message_inherits_base_functionality(self):
        """Test DocumentMessage inherits BaseMessage methods."""
        msg = DocumentMessage(
            source_type="docx",
            source_path="/docs/report.docx",
            source_name="report.docx"
        )
        msg.increment_retry()
        assert msg.retry_count == 1
    
    def test_document_message_with_explicit_base_fields(self):
        """Test DocumentMessage with explicit BaseMessage fields."""
        msg = DocumentMessage(
            message_id="custom-id",
            created_at=1000000.0,
            retry_count=3,
            source_type="txt",
            source_path="/file.txt",
            source_name="file.txt"
        )
        assert msg.message_id == "custom-id"
        assert msg.created_at == 1000000.0
        assert msg.retry_count == 3


class TestChunkMessage:
    """Unit tests for ChunkMessage dataclass."""
    
    def test_chunk_message_initialization(self):
        """Test ChunkMessage initializes with all required fields."""
        msg = ChunkMessage(
            document_id="doc-123",
            text="Sample text content.",
            source_name="doc.pdf",
            source_path="/path/doc.pdf",
            chunk_index=0,
            start_char=0,
            end_char=20,
            char_count=20
        )
        assert msg.document_id == "doc-123"
        assert msg.text == "Sample text content."
        assert msg.source_name == "doc.pdf"
        assert msg.source_path == "/path/doc.pdf"
        assert msg.chunk_index == 0
        assert msg.start_char == 0
        assert msg.end_char == 20
        assert msg.char_count == 20
    
    def test_chunk_message_inherits_base_fields(self):
        """Test ChunkMessage inherits BaseMessage defaults."""
        msg = ChunkMessage(
            document_id="doc-456",
            text="Text",
            source_name="file.txt",
            source_path="/file.txt",
            chunk_index=1,
            start_char=100,
            end_char=104,
            char_count=4
        )
        assert isinstance(msg.message_id, str)
        assert isinstance(msg.created_at, float)
        assert msg.retry_count == 0
    
    def test_chunk_message_multiple_chunks(self):
        """Test ChunkMessage for sequential chunks."""
        chunks = [
            ChunkMessage(
                document_id="doc-789",
                text=f"Chunk {i}",
                source_name="doc.txt",
                source_path="/doc.txt",
                chunk_index=i,
                start_char=i * 10,
                end_char=(i + 1) * 10,
                char_count=10
            )
            for i in range(3)
        ]
        assert len(chunks) == 3
        assert chunks[0].chunk_index == 0
        assert chunks[1].chunk_index == 1
        assert chunks[2].chunk_index == 2
        assert all(c.document_id == "doc-789" for c in chunks)
    
    def test_chunk_message_retry_inheritance(self):
        """Test ChunkMessage can use inherited retry functionality."""
        msg = ChunkMessage(
            document_id="doc-xyz",
            text="Content",
            source_name="test.txt",
            source_path="/test.txt",
            chunk_index=0,
            start_char=0,
            end_char=7,
            char_count=7
        )
        for i in range(5):
            msg.increment_retry()
        assert msg.retry_count == 5