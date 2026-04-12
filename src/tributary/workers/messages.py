from dataclasses import KW_ONLY, dataclass, field, asdict
from uuid import uuid4
import time
import json

def _all_subclasses(cls):
    for sub in cls.__subclasses__():
        yield sub
        yield from _all_subclasses(sub)

@dataclass
class BaseMessage:
    message_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0

    def increment_retry(self):
        self.retry_count += 1

    def serialize(self) -> str:
        """Convert the message to a string for storage or transmission."""
        data = asdict(self)
        data.pop("_raw", None)
        data["__type__"] = type(self).__name__
        return json.dumps(data)
    
    @classmethod
    def deserialize(cls, raw: str | bytes) -> 'BaseMessage':
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        data = json.loads(raw)
        data.pop("_raw", None)
        type_name = data.pop("__type__")
        registry = {c.__name__: c for c in _all_subclasses(cls)}
        registry[cls.__name__] = cls
        klass = registry[type_name]
        return klass(**data)

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