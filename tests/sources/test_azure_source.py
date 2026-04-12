import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tributary.sources.azure_source import AzureBlobSource


def _make_blob(name: str):
    blob = MagicMock()
    blob.name = name
    return blob


def _make_mock_azure(objects: dict[str, bytes]):
    """Build a mock azure.storage.blob.aio module."""
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

    service_client = MagicMock()
    service_client.get_container_client.return_value = container_client
    service_client.__aenter__ = AsyncMock(return_value=service_client)
    service_client.__aexit__ = AsyncMock(return_value=None)

    mock_module = MagicMock()
    mock_module.BlobServiceClient.from_connection_string.return_value = service_client
    mock_module.BlobServiceClient.return_value = service_client
    return mock_module, service_client


def _patch_azure(mock_module):
    return patch("tributary.sources.azure_source.lazy_import", return_value=mock_module)


@pytest.mark.asyncio
async def test_fetch_objects():
    mock_mod, _ = _make_mock_azure({"docs/readme.txt": b"hello", "docs/guide.md": b"world"})
    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="c", connection_string="conn", prefix="docs/").fetch()]
    assert len(results) == 2
    assert {r.file_name for r in results} == {"readme.txt", "guide.md"}
    assert all(r.source_type == "azure_blob" for r in results)


@pytest.mark.asyncio
async def test_account_url_auth():
    mock_mod, _ = _make_mock_azure({"file.txt": b"content"})
    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="c", account_url="https://acct.blob.core.windows.net").fetch()]
    assert len(results) == 1
    mock_mod.BlobServiceClient.assert_called_once_with("https://acct.blob.core.windows.net")


@pytest.mark.asyncio
async def test_extension_filtering():
    mock_mod, _ = _make_mock_azure({"data.txt": b"text", "data.csv": b"csv", "image.png": b"png"})
    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="c", connection_string="conn", extensions=[".txt", ".csv"]).fetch()]
    assert {r.file_name for r in results} == {"data.txt", "data.csv"}


@pytest.mark.asyncio
async def test_empty_container():
    mock_mod, _ = _make_mock_azure({})
    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="empty", connection_string="conn").fetch()]
    assert results == []


@pytest.mark.asyncio
async def test_source_result_metadata():
    mock_mod, _ = _make_mock_azure({"reports/q1.pdf": b"pdf-content"})
    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="data", connection_string="conn", prefix="reports/").fetch()]
    r = results[0]
    assert r.file_name == "q1.pdf"
    assert r.source_path == "azure://data/reports/q1.pdf"
    assert r.raw_bytes == b"pdf-content"


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

    mock_mod = MagicMock()
    mock_mod.BlobServiceClient.from_connection_string.return_value = service_client

    with _patch_azure(mock_mod):
        results = [r async for r in AzureBlobSource(container="c", connection_string="conn").fetch()]
    assert len(results) == 1
    assert results[0].file_name == "good.txt"


def test_no_auth_raises():
    with pytest.raises(ValueError, match="Provide connection_string or account_url"):
        AzureBlobSource(container="my-container")
