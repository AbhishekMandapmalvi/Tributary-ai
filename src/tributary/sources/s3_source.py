from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from collections.abc import AsyncIterator
import pathlib
import aiobotocore
import structlog

logger = structlog.get_logger(__name__)

class S3Source(BaseSource):
    def __init__(self, bucket: str, prefix: str = "", extensions: list[str] | None = None):
        super().__init__(extensions)
        self.bucket_name = bucket
        self.prefix = prefix

    async def fetch(self) -> AsyncIterator[SourceResult]:
        session = aiobotocore.session.get_session()

        async with session.create_client('s3') as s3_client:
            paginator = s3_client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if not self._is_supported_extension(key):
                        logger.info("Skipping unsupported file", file_name=key)
                        continue
                    
                    try:
                        response = await s3_client.get_object(Bucket=self.bucket_name, Key=key)
                        raw_bytes = await response['Body'].read()
                        yield SourceResult(
                            raw_bytes=raw_bytes,
                            file_name=pathlib.Path(key).name,
                            source_path=f"s3://{self.bucket_name}/{key}",
                            source_type="s3_object"
                        )
                    except Exception as e:
                        logger.error("Error reading S3 object", file_name=key, error=str(e))