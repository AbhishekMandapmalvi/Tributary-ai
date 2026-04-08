from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from collections.abc import AsyncIterator
import pathlib
from gcloud.aio.storage import Storage
import structlog

logger = structlog.get_logger(__name__)


class GCSSource(BaseSource):
    def __init__(self, bucket: str, prefix: str = "", extensions: list[str] | None = None):
        super().__init__(extensions)
        self.bucket_name = bucket
        self.prefix = prefix

    async def fetch(self) -> AsyncIterator[SourceResult]:
        async with Storage() as storage:
            page_token = None

            while True:
                params = {"prefix": self.prefix}
                if page_token:
                    params["pageToken"] = page_token

                response = await storage.list_objects(self.bucket_name, params=params)
                items = response.get("items", [])

                for item in items:
                    key = item["name"]
                    if not self._is_supported_extension(key):
                        logger.info("Skipping unsupported file", file_name=key)
                        continue

                    try:
                        raw_bytes = await storage.download(self.bucket_name, key)
                        yield SourceResult(
                            raw_bytes=raw_bytes,
                            file_name=pathlib.Path(key).name,
                            source_path=f"gs://{self.bucket_name}/{key}",
                            source_type="gcs_object"
                        )
                    except Exception as e:
                        logger.error("Error reading GCS object", file_name=key, error=str(e))

                page_token = response.get("nextPageToken")
                if not page_token:
                    break
