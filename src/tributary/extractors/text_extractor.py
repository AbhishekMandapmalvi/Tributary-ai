from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from time import perf_counter

class TextExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = perf_counter()
        text = self._decode_bytes(bytes_data, source_name)
        extraction_time_ms = (perf_counter() - start_time) * 1000
        
        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='text/plain',
            extraction_time_ms=extraction_time_ms
        )