import pytest
from unittest.mock import AsyncMock, patch
import tributary.sources.gcs_source as gcs_module
from tributary.sources.gcs_source import GCSSource


def _make_mock_storage(objects: dict[str, bytes], pages: int = 1):
    """Build a mock Storage that returns the given objects, optionally paginated."""
    storage = AsyncMock()

    items = [{"name": key} for key in objects]
    items_per_page = len(items) // pages if pages > 0 else len(items)
    paginated = []
    for i in range(pages):
        start = i * items_per_page
        end = start + items_per_page if i < pages - 1 else len(items)
        page = {"items": items[start:end]}
        if i < pages - 1:
            page["nextPageToken"] = f"token_{i + 1}"
        paginated.append(page)
    if not paginated:
        paginated = [{}]

    call_count = {"n": 0}

    async def _list_objects(bucket, params=None):
        idx = call_count["n"]
        call_count["n"] += 1
        return paginated[idx] if idx < len(paginated) else {}

    storage.list_objects = AsyncMock(side_effect=_list_objects)

    async def _download(bucket, key):
        return objects[key]

    storage.download = AsyncMock(side_effect=_download)

    storage.__aenter__.return_value = storage
    storage.__aexit__.return_value = None
    return storage


@pytest.mark.asyncio
async def test_fetch_objects():
    objects = {
        "docs/readme.txt": b"hello",
        "docs/guide.md": b"world",
    }
    storage = _make_mock_storage(objects)

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="my-bucket", prefix="docs/")
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"readme.txt", "guide.md"}
    assert all(r.source_type == "gcs_object" for r in results)
    assert all("gs://my-bucket/" in r.source_path for r in results)


@pytest.mark.asyncio
async def test_extension_filtering():
    objects = {
        "data.txt": b"text",
        "data.csv": b"csv",
        "image.png": b"png",
    }
    storage = _make_mock_storage(objects)

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="my-bucket", extensions=[".txt", ".csv"])
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_empty_bucket():
    storage = _make_mock_storage({})

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="empty-bucket")
        results = [r async for r in source.fetch()]

    assert results == []


@pytest.mark.asyncio
async def test_pagination():
    objects = {
        "a.txt": b"a",
        "b.txt": b"b",
        "c.txt": b"c",
        "d.txt": b"d",
    }
    storage = _make_mock_storage(objects, pages=2)

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="my-bucket")
        results = [r async for r in source.fetch()]

    assert len(results) == 4
    assert storage.list_objects.call_count == 2


@pytest.mark.asyncio
async def test_source_result_metadata():
    objects = {"reports/q1.pdf": b"pdf-content"}
    storage = _make_mock_storage(objects)

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="data-bucket", prefix="reports/")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    r = results[0]
    assert r.file_name == "q1.pdf"
    assert r.source_path == "gs://data-bucket/reports/q1.pdf"
    assert r.raw_bytes == b"pdf-content"
    assert r.size == len(b"pdf-content")


@pytest.mark.asyncio
async def test_error_handled_gracefully():
    storage = AsyncMock()
    storage.list_objects = AsyncMock(return_value={"items": [{"name": "good.txt"}, {"name": "bad.txt"}]})

    async def _download(bucket, key):
        if key == "bad.txt":
            raise Exception("Access denied")
        return b"good"

    storage.download = AsyncMock(side_effect=_download)
    storage.__aenter__.return_value = storage
    storage.__aexit__.return_value = None

    with patch.object(gcs_module, "Storage", return_value=storage):
        source = GCSSource(bucket="my-bucket")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    assert results[0].file_name == "good.txt"
