from dataclasses import dataclass, field
from time import perf_counter


@dataclass
class PipelineEvent:
    event_type: str
    timestamp: float = field(default_factory=perf_counter)

    # Document events
    source_name: str | None = None
    stage: str | None = None
    chunks_count: int | None = None
    error: str | None = None

    # Pipeline events
    total_documents: int | None = None
    successful: int | None = None
    failed: int | None = None
    time_ms: float | None = None