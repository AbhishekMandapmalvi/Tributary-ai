from dataclasses import KW_ONLY, dataclass, field
import time
from uuid import uuid4

@dataclass
class BaseMessage:
    message_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0

    def increment_retry(self):
        self.retry_count += 1

@dataclass
class DocumentMessage(BaseMessage):
    _: KW_ONLY
    source_type: str
    source_path: str
    source_name: str

@dataclass
class ChunkMessage(BaseMessage):
    _: KW_ONLY
    document_id: str
    text: str
    source_name: str
    source_path: str
    chunk_index: int
    start_char: int
    end_char: int
    char_count: int