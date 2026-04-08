from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from tributary.utils.lazy_import import lazy_import
from time import perf_counter

class PDFExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = perf_counter()
        fitz = lazy_import("fitz", pip_name="pymupdf")
        with fitz.open(stream=bytes_data, filetype="pdf") as pdf_document:
            pages = [page.get_text() for page in pdf_document]
            text = "".join(pages)
        extraction_time_ms = (perf_counter() - start_time) * 1000

        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='pdf',
            extraction_time_ms=extraction_time_ms
        )