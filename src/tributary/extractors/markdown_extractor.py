from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from markdown_it import MarkdownIt
from mdit_plain.renderer import RendererPlain
from time import perf_counter


class MarkdownExtractor(BaseExtractor):
    def __init__(self):
        self.md = MarkdownIt(renderer_cls=RendererPlain)
    
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = perf_counter()
        markdown_text = self._decode_bytes(bytes_data, source_name)
        text = self.md.render(markdown_text)
        extraction_time_ms = (perf_counter() - start_time) * 1000
        
        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='markdown',
            extraction_time_ms=extraction_time_ms
        )