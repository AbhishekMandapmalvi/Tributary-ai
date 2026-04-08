import pytest
from tributary.sources import get_source
from tributary.sources.local_source import LocalSource
from tributary.sources.s3_source import S3Source
from tributary.sources.gcs_source import GCSSource
from tributary.sources.azure_source import AzureBlobSource
from tributary.sources.web_scraper_source import WebScraperSource


def test_local():
    assert isinstance(get_source("local", directory="."), LocalSource)


def test_s3():
    assert isinstance(get_source("s3", bucket="my-bucket"), S3Source)


def test_gcs():
    assert isinstance(get_source("gcs", bucket="my-bucket"), GCSSource)


def test_azure():
    assert isinstance(get_source("azure", container="c", connection_string="conn"), AzureBlobSource)


def test_web():
    assert isinstance(get_source("web", urls=["https://example.com"]), WebScraperSource)


def test_kwargs_passed_through():
    source = get_source("local", directory=".", extensions=[".txt"])
    assert source.extensions == [".txt"]


def test_unknown_source_raises():
    with pytest.raises(ValueError, match="Unknown source type"):
        get_source("ftp")
