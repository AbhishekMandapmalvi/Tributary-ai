from dataclasses import dataclass

@dataclass
class EmbeddingResult:
    chunk_text: str
    vector: list[float]
    source_name: str
    chunk_index: int
    model_name: str