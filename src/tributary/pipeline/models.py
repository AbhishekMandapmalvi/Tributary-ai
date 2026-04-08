from dataclasses import dataclass, field


@dataclass
class FailedDocument:
    source_name: str
    stage: str
    error: str


@dataclass
class PipelineResult:
    total_documents: int = 0
    successful: int = 0
    failed: int = 0
    time_ms: float = 0.0
    failures: list[FailedDocument] = field(default_factory=list)
