from tributary.sources.base import BaseSource
from tributary.sources.models import SourceResult
from tributary.sources.local_source import LocalSource
from tributary.sources.s3_source import S3Source
from tributary.sources.gcs_source import GCSSource
from tributary.sources.azure_source import AzureBlobSource
from tributary.sources.web_scraper_source import WebScraperSource

_REGISTRY = {
    "local": LocalSource,
    "s3": S3Source,
    "gcs": GCSSource,
    "azure": AzureBlobSource,
    "web": WebScraperSource,
}


def get_source(source_type: str, **kwargs) -> BaseSource:
    source_cls = _REGISTRY.get(source_type)
    if source_cls is None:
        raise ValueError(
            f"Unknown source type: '{source_type}'. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return source_cls(**kwargs)
