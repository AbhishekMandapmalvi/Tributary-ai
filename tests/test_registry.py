import pytest
from tributary.extractors import get_extractor_for_extension
from tributary.extractors.text_extractor import TextExtractor
from tributary.extractors.markdown_extractor import MarkdownExtractor
from tributary.extractors.html_extractor import HTMLExtractor
from tributary.extractors.csv_extractor import CSVExtractor
from tributary.extractors.json_extractor import JSONExtractor
from tributary.extractors.pdf_extractor import PDFExtractor


def test_txt_returns_text_extractor():
    result = get_extractor_for_extension("guide.txt")
    assert isinstance(result, TextExtractor)


def test_md_returns_markdown_extractor():
    result = get_extractor_for_extension("readme.md")
    assert isinstance(result, MarkdownExtractor)


def test_markdown_returns_markdown_extractor():
    result = get_extractor_for_extension("readme.markdown")
    assert isinstance(result, MarkdownExtractor)


def test_html_returns_html_extractor():
    result = get_extractor_for_extension("page.html")
    assert isinstance(result, HTMLExtractor)


def test_htm_returns_html_extractor():
    result = get_extractor_for_extension("page.htm")
    assert isinstance(result, HTMLExtractor)


def test_csv_returns_csv_extractor():
    result = get_extractor_for_extension("data.csv")
    assert isinstance(result, CSVExtractor)


def test_tsv_returns_csv_extractor():
    result = get_extractor_for_extension("data.tsv")
    assert isinstance(result, CSVExtractor)


def test_json_returns_json_extractor():
    result = get_extractor_for_extension("config.json")
    assert isinstance(result, JSONExtractor)


def test_pdf_returns_pdf_extractor():
    result = get_extractor_for_extension("report.pdf")
    assert isinstance(result, PDFExtractor)


def test_unknown_extension_raises_value_error():
    with pytest.raises(ValueError):
        get_extractor_for_extension("data.xyz")
