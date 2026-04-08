import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from tributary.sources.web_scraper_source import WebScraperSource


def _mock_response(body: bytes, status: int = 200):
    resp = AsyncMock()
    resp.read.return_value = body
    resp.status = status
    resp.__aenter__.return_value = resp
    resp.__aexit__.return_value = None
    return resp


def _mock_aiohttp(url_responses: dict[str, bytes]):
    session = AsyncMock()

    def _get(url):
        if url in url_responses:
            return _mock_response(url_responses[url])
        raise Exception("Connection refused")

    session.get = MagicMock(side_effect=_get)
    session.__aenter__.return_value = session
    session.__aexit__.return_value = None

    mock_module = MagicMock()
    mock_module.ClientSession.return_value = session
    mock_module.ClientTimeout.return_value = MagicMock()
    return mock_module


def _patch_web(mock_module):
    return patch("tributary.sources.web_scraper_source.lazy_import", return_value=mock_module)


@pytest.mark.asyncio
async def test_fetch_urls():
    responses = {
        "https://example.com/page1.html": b"<h1>Page 1</h1>",
        "https://example.com/page2.html": b"<h1>Page 2</h1>",
    }
    with _patch_web(_mock_aiohttp(responses)):
        results = [r async for r in WebScraperSource(urls=list(responses.keys())).fetch()]
    assert len(results) == 2
    assert {r.file_name for r in results} == {"page1.html", "page2.html"}
    assert all(r.source_type == "web_page" for r in results)


@pytest.mark.asyncio
async def test_source_result_metadata():
    with _patch_web(_mock_aiohttp({"https://example.com/docs/guide.html": b"content"})):
        results = [r async for r in WebScraperSource(urls=["https://example.com/docs/guide.html"]).fetch()]
    r = results[0]
    assert r.file_name == "guide.html"
    assert r.source_path == "https://example.com/docs/guide.html"
    assert r.raw_bytes == b"content"
    assert r.size == len(b"content")


@pytest.mark.asyncio
async def test_file_name_trailing_slash():
    with _patch_web(_mock_aiohttp({"https://example.com/docs/": b"content"})):
        results = [r async for r in WebScraperSource(urls=["https://example.com/docs/"]).fetch()]
    assert results[0].file_name == "docs"


@pytest.mark.asyncio
async def test_file_name_root_url():
    with _patch_web(_mock_aiohttp({"https://example.com": b"content"})):
        results = [r async for r in WebScraperSource(urls=["https://example.com"]).fetch()]
    assert results[0].file_name == "example.com"


@pytest.mark.asyncio
async def test_error_handled_gracefully():
    with _patch_web(_mock_aiohttp({"https://good.com/page.html": b"good"})):
        results = [r async for r in WebScraperSource(urls=["https://good.com/page.html", "https://bad.com/fail"]).fetch()]
    assert len(results) == 1
    assert results[0].file_name == "page.html"


@pytest.mark.asyncio
async def test_empty_urls():
    source = WebScraperSource(urls=[])
    results = [r async for r in source.fetch()]
    assert results == []
