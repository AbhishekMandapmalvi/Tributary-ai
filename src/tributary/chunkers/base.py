from abc import ABC, abstractmethod
from tributary.chunkers.models import ChunkResult
import structlog

logger = structlog.get_logger(__name__)

class BaseChunker(ABC):
    @abstractmethod
    def chunk(self, text: str, source_name: str = "unknown") -> list[ChunkResult]:
        """
        Chunk the input text and return it as a list of ChunkResults.

        Args:
            text (str): The input text to be chunked.
            source_name (str): The name of the source.

        Returns:
            list[ChunkResult]: A list of chunk results.
        """
        pass