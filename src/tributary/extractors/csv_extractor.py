from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from time import time
from csv import DictReader

class CSVExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = time()
        csv_text = self._decode_bytes(bytes_data)

        delimiter = '\t' if source_name.endswith('.tsv') else ','
        rows = DictReader(csv_text.splitlines(), delimiter=delimiter)
        lines = [", ".join(f"{k}: {v}" for k, v in row.items()) for row in rows]
        text = "\n".join(lines)

        extraction_time_ms = (time() - start_time) * 1000

        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='csv' if delimiter == ',' else 'tsv',
            extraction_time_ms=extraction_time_ms
        )