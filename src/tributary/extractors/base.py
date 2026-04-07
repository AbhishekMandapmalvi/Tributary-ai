from abc import ABC, abstractmethod
from tributary.extractors.models import ExtractionResult
import structlog

logger = structlog.get_logger(__name__)


class BaseExtractor(ABC):
    @abstractmethod
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        """
        Extract text from bytes data and return it as an ExtractionResult.

        Args:
            bytes_data (bytes): The input data in bytes format.
            source_name (str): The name of the source.

        Returns:
            ExtractionResult: The result of the extraction.
        """
        pass

    def _decode_bytes(self, bytes_data: bytes, source_name: str = "unknown") -> str:
        """
        Helper method to decode bytes data to a string, trying utf-8 first and falling back to latin-1.

        Args:
            bytes_data (bytes): The input data in bytes format.
            source_name (str): The name of the source, used for logging.

        Returns:
            str: The decoded string.
        """
        if not bytes_data:
            return ""
        try:
            return bytes_data.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning("UTF-8 decode failed, falling back to Latin-1", source_name=source_name, byte_length=len(bytes_data))
            return bytes_data.decode('latin-1')