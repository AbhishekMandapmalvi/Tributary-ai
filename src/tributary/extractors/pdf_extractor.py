from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
import fitz
from time import time

class PDFExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = time()
        with fitz.open(stream=bytes_data, filetype="pdf") as pdf_document:
            pages = [page.get_text() for page in pdf_document]
            text = "".join(pages)
        extraction_time_ms = (time() - start_time) * 1000

        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='pdf',
            extraction_time_ms=extraction_time_ms
        )