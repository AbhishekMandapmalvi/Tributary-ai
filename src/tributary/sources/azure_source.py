from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from collections.abc import AsyncIterator
import pathlib
from azure.storage.blob.aio import BlobServiceClient
import structlog

logger = structlog.get_logger(__name__)


class AzureBlobSource(BaseSource):
    def __init__(self, container: str, connection_string: str | None = None, account_url: str | None = None, prefix: str = "", extensions: list[str] | None = None):
        if not connection_string and not account_url:
            raise ValueError("Provide connection_string or account_url")
        super().__init__(extensions)
        self.container_name = container
        self.connection_string = connection_string
        self.account_url = account_url
        self.prefix = prefix

    async def fetch(self) -> AsyncIterator[SourceResult]:
        if self.connection_string:
            service_client = BlobServiceClient.from_connection_string(self.connection_string)
        else:
            service_client = BlobServiceClient(self.account_url)  # type: ignore[arg-type]

        async with service_client:
            container_client = service_client.get_container_client(self.container_name)

            async for blob in container_client.list_blobs(name_starts_with=self.prefix):
                key = blob.name
                if not self._is_supported_extension(key):
                    logger.info("Skipping unsupported file", file_name=key)
                    continue

                try:
                    blob_data = await container_client.download_blob(key)
                    raw_bytes = await blob_data.readall()
                    yield SourceResult(
                        raw_bytes=raw_bytes,
                        file_name=pathlib.Path(key).name,
                        source_path=f"azure://{self.container_name}/{key}",
                        source_type="azure_blob"
                    )
                except Exception as e:
                    logger.error("Error reading Azure blob", file_name=key, error=str(e))
