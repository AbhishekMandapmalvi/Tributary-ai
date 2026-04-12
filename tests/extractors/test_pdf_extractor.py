import pytest
import fitz
from tributary.extractors.pdf_extractor import PDFExtractor


def make_pdf(pages_text: list[str]) -> bytes:
    doc = fitz.open()
    for text in pages_text:
        page = doc.new_page()
        page.insert_text((72, 72), text)
    pdf_bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


@pytest.mark.asyncio
async def test_single_page():
    extractor = PDFExtractor()
    pdf_bytes = make_pdf(["Hello World"])
    result = await extractor.extract(pdf_bytes, "test.pdf")
    assert "Hello World" in result.text
    assert result.source_name == "test.pdf"
    assert result.content_type == "pdf"
    assert result.char_count > 0
    assert result.extraction_time_ms >= 0


@pytest.mark.asyncio
async def test_multi_page():
    extractor = PDFExtractor()
    pdf_bytes = make_pdf(["Page one", "Page two", "Page three"])
    result = await extractor.extract(pdf_bytes, "multi.pdf")
    assert "Page one" in result.text
    assert "Page two" in result.text
    assert "Page three" in result.text


@pytest.mark.asyncio
async def test_empty_page():
    extractor = PDFExtractor()
    pdf_bytes = make_pdf([""])
    result = await extractor.extract(pdf_bytes, "empty.pdf")
    assert result.text.strip() == ""
    assert result.content_type == "pdf"


@pytest.mark.asyncio
async def test_invalid_pdf_raises():
    extractor = PDFExtractor()
    with pytest.raises(Exception):
        await extractor.extract(b"not a pdf", "bad.pdf")
