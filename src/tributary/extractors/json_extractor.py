from tributary.extractors.base import BaseExtractor
from tributary.extractors.models import ExtractionResult
from time import time
import json


class JSONExtractor(BaseExtractor):
    async def extract(self, bytes_data: bytes, source_name: str) -> ExtractionResult:
        start_time = time()
        raw_text = self._decode_bytes(bytes_data)
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in '{source_name}': {e}") from e
        lines = []
        self._flatten(data, "", lines)
        text = "\n".join(lines)
        extraction_time_ms = (time() - start_time) * 1000

        return ExtractionResult(
            text=text,
            source_name=source_name,
            content_type='json',
            extraction_time_ms=extraction_time_ms
        )

    def _flatten(self, obj, prefix: str, lines: list[str]):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_prefix = f"{prefix}.{key}" if prefix else key
                self._flatten(value, new_prefix, lines)
        elif isinstance(obj, list):
            if obj and all(isinstance(item, (str, int, float, bool)) for item in obj):
                lines.append(f"{prefix}: {', '.join(str(item) for item in obj)}")
            else:
                for i, item in enumerate(obj):
                    self._flatten(item, f"{prefix}.{i}", lines)
        else:
            lines.append(f"{prefix}: {obj}")
