import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tributary.sources.s3_source import S3Source


def _make_mock_s3(objects: dict[str, bytes]):
    """Build a mock aiobotocore module with the given S3 objects."""
    client = MagicMock()

    page_contents = [{"Key": key} for key in objects]
    pages = [{"Contents": page_contents}] if page_contents else [{}]

    async def _paginate(**kwargs):
        for page in pages:
            yield page

    paginator = MagicMock()
    paginator.paginate = _paginate
    client.get_paginator.return_value = paginator

    async def _get_object(Bucket, Key):
        body = AsyncMock()
        body.read.return_value = objects[Key]
        return {"Body": body}

    client.get_object = _get_object

    ctx = AsyncMock()
    ctx.__aenter__.return_value = client

    session = MagicMock()
    session.create_client.return_value = ctx

    mock_aio = MagicMock()
    mock_aio.session.get_session.return_value = session
    return mock_aio, client


def _patch_s3(mock_aio):
    """Patch lazy_import in s3_source to return mock aiobotocore."""
    return patch("tributary.sources.s3_source.lazy_import", return_value=mock_aio)


@pytest.mark.asyncio
async def test_fetch_objects():
    mock_aio, _ = _make_mock_s3({"docs/readme.txt": b"hello", "docs/guide.md": b"world"})
    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="my-bucket", prefix="docs/").fetch()]
    assert len(results) == 2
    assert {r.file_name for r in results} == {"readme.txt", "guide.md"}
    assert all(r.source_type == "s3_object" for r in results)


@pytest.mark.asyncio
async def test_extension_filtering():
    mock_aio, _ = _make_mock_s3({"data.txt": b"text", "data.csv": b"csv", "image.png": b"png"})
    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="b", extensions=[".txt", ".csv"]).fetch()]
    assert {r.file_name for r in results} == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_skips_unsupported_extensions():
    mock_aio, _ = _make_mock_s3({"keep.txt": b"keep", "skip.xyz": b"skip"})
    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="b", extensions=[".txt"]).fetch()]
    assert len(results) == 1
    assert results[0].file_name == "keep.txt"


@pytest.mark.asyncio
async def test_empty_bucket():
    mock_aio, _ = _make_mock_s3({})
    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="empty").fetch()]
    assert results == []


@pytest.mark.asyncio
async def test_source_result_metadata():
    mock_aio, _ = _make_mock_s3({"reports/q1.pdf": b"pdf-content"})
    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="data-bucket", prefix="reports/").fetch()]
    assert len(results) == 1
    r = results[0]
    assert r.file_name == "q1.pdf"
    assert r.source_path == "s3://data-bucket/reports/q1.pdf"
    assert r.raw_bytes == b"pdf-content"
    assert r.size == len(b"pdf-content")


@pytest.mark.asyncio
async def test_error_handled_gracefully():
    client = MagicMock()

    async def _paginate(**kwargs):
        yield {"Contents": [{"Key": "good.txt"}, {"Key": "bad.txt"}]}

    paginator = MagicMock()
    paginator.paginate = _paginate
    client.get_paginator.return_value = paginator

    async def _get_object(Bucket, Key):
        if Key == "bad.txt":
            raise Exception("Access denied")
        body = AsyncMock()
        body.read.return_value = b"good"
        return {"Body": body}

    client.get_object = _get_object
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client
    session = MagicMock()
    session.create_client.return_value = ctx
    mock_aio = MagicMock()
    mock_aio.session.get_session.return_value = session

    with _patch_s3(mock_aio):
        results = [r async for r in S3Source(bucket="my-bucket").fetch()]
    assert len(results) == 1
    assert results[0].file_name == "good.txt"
