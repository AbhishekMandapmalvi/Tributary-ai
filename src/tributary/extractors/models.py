from dataclasses import dataclass

@dataclass
class ExtractionResult:
    text: str
    source_name: str
    content_type: str
    extraction_time_ms: float
    char_count: int = 0

    def __post_init__(self):
        self.char_count = len(self.text)