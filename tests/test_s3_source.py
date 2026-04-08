import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import tributary.sources.s3_source as s3_module
from tributary.sources.s3_source import S3Source


def _make_mock_s3(objects: dict[str, bytes]):
    """Build a mock aiobotocore module with the given S3 objects."""
    # Client must be MagicMock — get_paginator is sync, get_object is async
    client = MagicMock()

    # Mock paginator (sync call returning object with async iterator)
    page_contents = [{"Key": key} for key in objects]
    pages = [{"Contents": page_contents}] if page_contents else [{}]

    async def _paginate(**kwargs):
        for page in pages:
            yield page

    paginator = MagicMock()
    paginator.paginate = _paginate
    client.get_paginator.return_value = paginator

    # Mock get_object (async)
    async def _get_object(Bucket, Key):
        body = AsyncMock()
        body.read.return_value = objects[Key]
        return {"Body": body}

    client.get_object = _get_object

    # create_client returns async context manager yielding client
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client

    session = MagicMock()
    session.create_client.return_value = ctx

    mock_aio = MagicMock()
    mock_aio.session.get_session.return_value = session
    return mock_aio, client


@pytest.mark.asyncio
async def test_fetch_objects():
    objects = {
        "docs/readme.txt": b"hello",
        "docs/guide.md": b"world",
    }
    mock_aio, _ = _make_mock_s3(objects)

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="my-bucket", prefix="docs/")
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"readme.txt", "guide.md"}
    assert all(r.source_type == "s3_object" for r in results)
    assert all("s3://my-bucket/" in r.source_path for r in results)


@pytest.mark.asyncio
async def test_extension_filtering():
    objects = {
        "data.txt": b"text",
        "data.csv": b"csv",
        "image.png": b"png",
    }
    mock_aio, _ = _make_mock_s3(objects)

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="my-bucket", extensions=[".txt", ".csv"])
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_skips_unsupported_extensions():
    objects = {
        "keep.txt": b"keep",
        "skip.xyz": b"skip",
    }
    mock_aio, _ = _make_mock_s3(objects)

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="my-bucket", extensions=[".txt"])
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    assert results[0].file_name == "keep.txt"


@pytest.mark.asyncio
async def test_empty_bucket():
    mock_aio, _ = _make_mock_s3({})

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="empty-bucket")
        results = [r async for r in source.fetch()]

    assert results == []


@pytest.mark.asyncio
async def test_source_result_metadata():
    objects = {"reports/q1.pdf": b"pdf-content"}
    mock_aio, _ = _make_mock_s3(objects)

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="data-bucket", prefix="reports/")
        results = [r async for r in source.fetch()]

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

    with patch.object(s3_module, "aiobotocore", mock_aio):
        source = S3Source(bucket="my-bucket")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    assert results[0].file_name == "good.txt"
