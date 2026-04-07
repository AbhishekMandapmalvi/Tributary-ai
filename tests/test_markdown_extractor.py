import pytest
from tributary.extractors.markdown_extractor import MarkdownExtractor

@pytest.mark.asyncio
async def test_utf8_extraction():
    extractor = MarkdownExtractor()
    raw = "# **hello world**".encode("utf-8")
    result = await extractor.extract(raw, "markdown")
    # assert the text is correct
    assert result.text == "hello world"
    # assert source_name is correct
    assert result.source_name == "markdown"
    # assert content_type is correct
    assert result.content_type == "markdown"
    # assert char_count is correct
    assert result.char_count == len("hello world")
    # assert extraction_time_ms is a positive number
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_latin1_fallback():
    # create bytes that are valid latin-1 but invalid utf-8
    # hint: the byte 0xe9 is 'é' in latin-1 but invalid standalone utf-8
    raw = "café".encode("latin-1")
    extractor = MarkdownExtractor()
    result = await extractor.extract(raw, "markdown")
    assert result.text == "café"
    assert result.source_name == "markdown"
    assert result.content_type == "markdown"
    assert result.char_count == len("café")
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_empty_bytes():
    # decide: should this raise an error or return empty text?
    # write the test accordingly
    raw = b""
    extractor = MarkdownExtractor()
    result = await extractor.extract(raw, "markdown")
    assert result.text == ""
    assert result.source_name == "markdown"
    assert result.content_type == "markdown"
    assert result.char_count == 0
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_header_stripping():
    extractor = MarkdownExtractor()
    raw = "# Hello World".encode("utf-8")
    result = await extractor.extract(raw, "test.md")
    # what should result.text be?
    assert result.text == "Hello World"
    assert result.source_name == "test.md"
    assert result.content_type == "markdown"
    assert result.char_count == len("Hello World")
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_bold_italic_stripping():
    extractor = MarkdownExtractor()
    raw = "**bold** and *italic*".encode("utf-8")
    result = await extractor.extract(raw, "test.md")
    # what should result.text be?
    assert result.text == "bold and italic"
    assert result.source_name == "test.md"
    assert result.content_type == "markdown"
    assert result.char_count == len("bold and italic")
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_link_stripping():
    extractor = MarkdownExtractor()
    raw = "[click here](https://example.com)".encode("utf-8")
    result = await extractor.extract(raw, "test.md")
    # what should result.text be?
    assert result.text == "click here"
    assert result.source_name == "test.md"
    assert result.content_type == "markdown"
    assert result.char_count == len("click here")
    assert result.extraction_time_ms >= 0