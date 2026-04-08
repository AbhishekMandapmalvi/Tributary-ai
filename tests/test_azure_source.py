import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import tributary.sources.azure_source as azure_module
from tributary.sources.azure_source import AzureBlobSource


def _make_blob(name: str):
    blob = MagicMock()
    blob.name = name
    return blob


def _make_mock_service_client(objects: dict[str, bytes]):
    """Build a mock BlobServiceClient with the given blob objects."""
    container_client = MagicMock()

    async def _list_blobs(name_starts_with=""):
        for key in objects:
            if key.startswith(name_starts_with):
                yield _make_blob(key)

    container_client.list_blobs = _list_blobs

    async def _download_blob(key):
        download = AsyncMock()
        download.readall.return_value = objects[key]
        return download

    container_client.download_blob = _download_blob

    # service_client must be MagicMock (get_container_client is sync)
    service_client = MagicMock()
    service_client.get_container_client.return_value = container_client
    service_client.__aenter__ = AsyncMock(return_value=service_client)
    service_client.__aexit__ = AsyncMock(return_value=None)
    return service_client


@pytest.mark.asyncio
async def test_fetch_objects():
    objects = {
        "docs/readme.txt": b"hello",
        "docs/guide.md": b"world",
    }
    mock_client = _make_mock_service_client(objects)

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.from_connection_string.return_value = mock_client
        source = AzureBlobSource(container="my-container", connection_string="fake-conn-str", prefix="docs/")
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"readme.txt", "guide.md"}
    assert all(r.source_type == "azure_blob" for r in results)
    assert all("azure://my-container/" in r.source_path for r in results)


@pytest.mark.asyncio
async def test_account_url_auth():
    objects = {"file.txt": b"content"}
    mock_client = _make_mock_service_client(objects)

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.return_value = mock_client
        source = AzureBlobSource(container="my-container", account_url="https://myaccount.blob.core.windows.net")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    MockBSC.assert_called_once_with("https://myaccount.blob.core.windows.net")


@pytest.mark.asyncio
async def test_extension_filtering():
    objects = {
        "data.txt": b"text",
        "data.csv": b"csv",
        "image.png": b"png",
    }
    mock_client = _make_mock_service_client(objects)

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.from_connection_string.return_value = mock_client
        source = AzureBlobSource(container="c", connection_string="conn", extensions=[".txt", ".csv"])
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_empty_container():
    mock_client = _make_mock_service_client({})

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.from_connection_string.return_value = mock_client
        source = AzureBlobSource(container="empty", connection_string="conn")
        results = [r async for r in source.fetch()]

    assert results == []


@pytest.mark.asyncio
async def test_source_result_metadata():
    objects = {"reports/q1.pdf": b"pdf-content"}
    mock_client = _make_mock_service_client(objects)

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.from_connection_string.return_value = mock_client
        source = AzureBlobSource(container="data", connection_string="conn", prefix="reports/")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    r = results[0]
    assert r.file_name == "q1.pdf"
    assert r.source_path == "azure://data/reports/q1.pdf"
    assert r.raw_bytes == b"pdf-content"
    assert r.size == len(b"pdf-content")


@pytest.mark.asyncio
async def test_error_handled_gracefully():
    container_client = MagicMock()

    async def _list_blobs(name_starts_with=""):
        yield _make_blob("good.txt")
        yield _make_blob("bad.txt")

    container_client.list_blobs = _list_blobs

    async def _download_blob(key):
        if key == "bad.txt":
            raise Exception("Access denied")
        download = AsyncMock()
        download.readall.return_value = b"good"
        return download

    container_client.download_blob = _download_blob

    service_client = MagicMock()
    service_client.get_container_client.return_value = container_client
    service_client.__aenter__ = AsyncMock(return_value=service_client)
    service_client.__aexit__ = AsyncMock(return_value=None)

    with patch.object(azure_module, "BlobServiceClient") as MockBSC:
        MockBSC.from_connection_string.return_value = service_client
        source = AzureBlobSource(container="c", connection_string="conn")
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    assert results[0].file_name == "good.txt"


def test_no_auth_raises():
    with pytest.raises(ValueError, match="Provide connection_string or account_url"):
        AzureBlobSource(container="my-container")
