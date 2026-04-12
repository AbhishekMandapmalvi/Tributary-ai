import pytest
from tributary.extractors.html_extractor import HTMLExtractor


@pytest.mark.asyncio
async def test_basic_html_tags_stripped():
    extractor = HTMLExtractor()
    raw = b"<p>Hello</p>"
    result = await extractor.extract(raw, "test.html")
    assert result.text == "Hello"
    assert result.source_name == "test.html"
    assert result.content_type == "html"
    assert result.char_count == len("Hello")
    assert result.extraction_time_ms >= 0


@pytest.mark.asyncio
async def test_nested_tags():
    extractor = HTMLExtractor()
    raw = b"<div><p>Hello <b>world</b></p></div>"
    result = await extractor.extract(raw, "test.html")
    assert "Hello" in result.text
    assert "world" in result.text
    assert "<b>" not in result.text
    assert "<div>" not in result.text


@pytest.mark.asyncio
async def test_separator_between_paragraphs():
    extractor = HTMLExtractor()
    raw = b"<p>First</p><p>Second</p>"
    result = await extractor.extract(raw, "test.html")
    assert "First" in result.text
    assert "Second" in result.text
    assert "\n" in result.text


@pytest.mark.asyncio
async def test_encoding_fallback():
    extractor = HTMLExtractor()
    raw = "<p>café</p>".encode("latin-1")
    result = await extractor.extract(raw, "test.html")
    assert "café" in result.text
    assert result.char_count > 0
