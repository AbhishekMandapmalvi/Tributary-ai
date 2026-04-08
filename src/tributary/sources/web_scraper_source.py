from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from tributary.utils.lazy_import import lazy_import
from collections.abc import AsyncIterator
from urllib.parse import urlparse
import asyncio
import structlog

logger = structlog.get_logger()

class WebScraperSource(BaseSource):
    def __init__(self, urls: list[str], max_concurrent: int = 5, timeout: int = 10, extensions: list[str] | None = None):
        super().__init__(extensions)
        self.urls = urls
        self.max_concurrent = max_concurrent
        self.timeout = timeout

    async def fetch(self) -> AsyncIterator[SourceResult]:
        aiohttp = lazy_import("aiohttp")
        semaphore = asyncio.Semaphore(self.max_concurrent)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
            for url in self.urls:
                async with semaphore:
                    try:
                        async with session.get(url) as response:
                            raw_bytes = await response.read()
                        parsed = urlparse(url)
                        name = parsed.path.rstrip("/").split("/")[-1] if parsed.path.strip("/") else (parsed.hostname or url)
                        yield SourceResult(
                            raw_bytes=raw_bytes,
                            file_name=name,
                            source_path=url,
                            source_type="web_page"
                        )
                    except Exception as e:
                        logger.error("Error fetching URL", url=url, error=str(e))