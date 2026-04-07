import pytest
from tributary.extractors import get_extractor_for_extension
from tributary.extractors.text_extractor import TextExtractor
from tributary.extractors.markdown_extractor import MarkdownExtractor


def test_txt_returns_text_extractor():
    result = get_extractor_for_extension("guide.txt")
    assert isinstance(result, TextExtractor)


def test_md_returns_markdown_extractor():
    result = get_extractor_for_extension("readme.md")
    assert isinstance(result, MarkdownExtractor)


def test_markdown_returns_markdown_extractor():
    result = get_extractor_for_extension("readme.markdown")
    assert isinstance(result, MarkdownExtractor)


def test_unknown_extension_raises_value_error():
    with pytest.raises(ValueError):
        get_extractor_for_extension("data.xyz")
