from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from collections.abc import AsyncIterator 
import structlog
import pathlib

logger = structlog.get_logger(__name__)

class LocalSource(BaseSource):
    def __init__(self, file_paths: list[str] | None = None, extensions: list[str] | None = None, directory: str | None = None):
        if not file_paths and not directory:
            raise ValueError("Provide file_paths, directory, or both")
        super().__init__(extensions)
        self.file_paths = file_paths or []
        self.directory = pathlib.Path(directory) if directory else None

    async def fetch(self) -> AsyncIterator[SourceResult]:
        all_paths = list(self.file_paths)

        if self.directory and self.directory.is_dir():
            for path in self.directory.rglob('*'):
                if path.is_file():
                    all_paths.append(str(path))

        for file_path in all_paths:
            if not self._is_supported_extension(file_path):
                logger.info("Skipping unsupported file", file_path=file_path)
                continue
            
            try:
                with open(file_path, 'rb') as f:
                    raw_bytes = f.read()
                    yield SourceResult(
                        raw_bytes=raw_bytes,
                        file_name=pathlib.Path(file_path).name,
                        source_path=str(pathlib.Path(file_path).resolve()),
                        source_type="local_file"
                    )
            except Exception as e:
                logger.error("Error reading file", file_path=file_path, error=str(e))