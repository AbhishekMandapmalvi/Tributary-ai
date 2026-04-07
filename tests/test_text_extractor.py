import pytest
from tributary.extractors.text_extractor import TextExtractor

@pytest.mark.asyncio
async def test_utf8_extraction():
    extractor = TextExtractor()
    raw = "hello world".encode("utf-8")
    result = await extractor.extract(raw, "test.txt")
    # assert the text is correct
    assert result.text == "hello world"
    # assert source_name is correct
    assert result.source_name == "test.txt"
    # assert content_type is correct
    assert result.content_type == "text/plain"
    # assert char_count is correct
    assert result.char_count == len("hello world")
    # assert extraction_time_ms is a positive number
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_latin1_fallback():
    # create bytes that are valid latin-1 but invalid utf-8
    # hint: the byte 0xe9 is 'é' in latin-1 but invalid standalone utf-8
    raw = "café".encode("latin-1")
    extractor = TextExtractor()
    result = await extractor.extract(raw, "test.txt")
    assert result.text == "café"
    assert result.source_name == "test.txt"
    assert result.content_type == "text/plain"
    assert result.char_count == len("café")
    assert result.extraction_time_ms >= 0

@pytest.mark.asyncio
async def test_empty_bytes():
    # decide: should this raise an error or return empty text?
    # write the test accordingly
    raw = b""
    extractor = TextExtractor()
    result = await extractor.extract(raw, "test.txt")
    assert result.text == ""
    assert result.source_name == "test.txt"
    assert result.content_type == "text/plain"
    assert result.char_count == 0
    assert result.extraction_time_ms >= 0