import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tributary.sources.gcs_source import GCSSource


def _make_mock_gcs(objects: dict[str, bytes], pages: int = 1):
    """Build a mock gcloud.aio.storage module."""
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

    mock_module = MagicMock()
    mock_module.Storage.return_value = storage
    return mock_module, storage


def _patch_gcs(mock_module):
    return patch("tributary.sources.gcs_source.lazy_import", return_value=mock_module)


@pytest.mark.asyncio
async def test_fetch_objects():
    mock_mod, _ = _make_mock_gcs({"docs/readme.txt": b"hello", "docs/guide.md": b"world"})
    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="my-bucket", prefix="docs/").fetch()]
    assert len(results) == 2
    assert {r.file_name for r in results} == {"readme.txt", "guide.md"}
    assert all(r.source_type == "gcs_object" for r in results)


@pytest.mark.asyncio
async def test_extension_filtering():
    mock_mod, _ = _make_mock_gcs({"data.txt": b"text", "data.csv": b"csv", "image.png": b"png"})
    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="b", extensions=[".txt", ".csv"]).fetch()]
    assert {r.file_name for r in results} == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_empty_bucket():
    mock_mod, _ = _make_mock_gcs({})
    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="empty").fetch()]
    assert results == []


@pytest.mark.asyncio
async def test_pagination():
    mock_mod, storage = _make_mock_gcs({"a.txt": b"a", "b.txt": b"b", "c.txt": b"c", "d.txt": b"d"}, pages=2)
    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="b").fetch()]
    assert len(results) == 4
    assert storage.list_objects.call_count == 2


@pytest.mark.asyncio
async def test_source_result_metadata():
    mock_mod, _ = _make_mock_gcs({"reports/q1.pdf": b"pdf-content"})
    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="data-bucket", prefix="reports/").fetch()]
    r = results[0]
    assert r.file_name == "q1.pdf"
    assert r.source_path == "gs://data-bucket/reports/q1.pdf"
    assert r.raw_bytes == b"pdf-content"


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

    mock_mod = MagicMock()
    mock_mod.Storage.return_value = storage

    with _patch_gcs(mock_mod):
        results = [r async for r in GCSSource(bucket="b").fetch()]
    assert len(results) == 1
    assert results[0].file_name == "good.txt"
