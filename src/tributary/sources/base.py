from abc import ABC, abstractmethod
import pathlib
from collections.abc import AsyncIterator
from tributary.sources.models import SourceResult
import structlog

logger = structlog.get_logger(__name__)

class BaseSource(ABC):
    def __init__(self, extensions: list[str] | None = None):
        self.extensions = extensions or []

    def _is_supported_extension(self, file_name: str) -> bool:
        """
        Check if the file extension of the given file name is supported by this source.

        Args:
            file_name (str): The name of the file to check.
        Returns:
            bool: True if the file extension is supported, False otherwise.
        """
        if not self.extensions:
            return True  # If no extensions are specified, support all files
        file_extension = pathlib.Path(file_name).suffix.lower()
        return file_extension in [ext.lower() for ext in self.extensions]    
    
    @abstractmethod
    async def fetch(self) -> AsyncIterator[SourceResult]:
        """
        Read data from the source and return it as a SourceResult.

        Returns:
            AsyncIterator of SourceResult objects.
        """
        yield  # type: ignore[misc]