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


def _mock_session(url_responses: dict[str, bytes]):
    session = AsyncMock()

    def _get(url):
        return _mock_response(url_responses[url])

    session.get = MagicMock(side_effect=_get)
    session.__aenter__.return_value = session
    session.__aexit__.return_value = None
    return session


@pytest.mark.asyncio
async def test_fetch_urls():
    responses = {
        "https://example.com/page1.html": b"<h1>Page 1</h1>",
        "https://example.com/page2.html": b"<h1>Page 2</h1>",
    }
    session = _mock_session(responses)

    with patch("tributary.sources.web_scraper_source.aiohttp.ClientSession", return_value=session):
        source = WebScraperSource(urls=list(responses.keys()))
        results = [r async for r in source.fetch()]

    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"page1.html", "page2.html"}
    assert all(r.source_type == "web_page" for r in results)


@pytest.mark.asyncio
async def test_source_result_metadata():
    responses = {"https://example.com/docs/guide.html": b"content"}
    session = _mock_session(responses)

    with patch("tributary.sources.web_scraper_source.aiohttp.ClientSession", return_value=session):
        source = WebScraperSource(urls=["https://example.com/docs/guide.html"])
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    r = results[0]
    assert r.file_name == "guide.html"
    assert r.source_path == "https://example.com/docs/guide.html"
    assert r.raw_bytes == b"content"
    assert r.size == len(b"content")


@pytest.mark.asyncio
async def test_file_name_trailing_slash():
    responses = {"https://example.com/docs/": b"content"}
    session = _mock_session(responses)

    with patch("tributary.sources.web_scraper_source.aiohttp.ClientSession", return_value=session):
        source = WebScraperSource(urls=["https://example.com/docs/"])
        results = [r async for r in source.fetch()]

    assert results[0].file_name == "docs"


@pytest.mark.asyncio
async def test_file_name_root_url():
    responses = {"https://example.com": b"content"}
    session = _mock_session(responses)

    with patch("tributary.sources.web_scraper_source.aiohttp.ClientSession", return_value=session):
        source = WebScraperSource(urls=["https://example.com"])
        results = [r async for r in source.fetch()]

    assert results[0].file_name == "example.com"


@pytest.mark.asyncio
async def test_error_handled_gracefully():
    session = AsyncMock()

    call_count = 0

    def _get(url):
        nonlocal call_count
        call_count += 1
        if "bad" in url:
            raise Exception("Connection refused")
        return _mock_response(b"good")

    session.get = MagicMock(side_effect=_get)
    session.__aenter__.return_value = session
    session.__aexit__.return_value = None

    with patch("tributary.sources.web_scraper_source.aiohttp.ClientSession", return_value=session):
        source = WebScraperSource(urls=["https://good.com/page.html", "https://bad.com/fail"])
        results = [r async for r in source.fetch()]

    assert len(results) == 1
    assert results[0].file_name == "page.html"


@pytest.mark.asyncio
async def test_empty_urls():
    source = WebScraperSource(urls=[])
    results = [r async for r in source.fetch()]
    assert results == []
