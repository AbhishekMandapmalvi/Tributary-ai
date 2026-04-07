from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from bs4 import BeautifulSoup
from time import perf_counter


class HTMLExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = perf_counter()
        html_text = self._decode_bytes(bytes_data, source_name)
        text = BeautifulSoup(html_text, 'lxml').get_text(separator='\n')
        extraction_time_ms = (perf_counter() - start_time) * 1000
        
        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='html',
            extraction_time_ms=extraction_time_ms
        )