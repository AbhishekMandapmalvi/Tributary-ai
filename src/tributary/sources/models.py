from dataclasses import dataclass

@dataclass
class SourceResult:
    raw_bytes: bytes
    file_name: str
    source_path: str
    source_type: str
    size: int = 0

    def __post_init__(self):
        self.size = len(self.raw_bytes)