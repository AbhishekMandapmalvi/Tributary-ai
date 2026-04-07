from dataclasses import dataclass

@dataclass
class ChunkResult:
    text: str
    source_name: str
    chunk_index: int
    start_char: int
    end_char: int
    char_count: int = 0
    
    def __post_init__(self):
        self.char_count = len(self.text)