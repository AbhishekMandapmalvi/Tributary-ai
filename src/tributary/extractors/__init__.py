from tributary.extractors.base import BaseExtractor
from tributary.extractors.csv_extractor import CSVExtractor
from tributary.extractors.html_extractor import HTMLExtractor
from tributary.extractors.pdf_extractor import PDFExtractor
from tributary.extractors.text_extractor import TextExtractor
from tributary.extractors.markdown_extractor import MarkdownExtractor 
from pathlib import Path

_REGISTRY = {
    ".txt": TextExtractor,
    ".md": MarkdownExtractor,
    ".markdown": MarkdownExtractor,
    ".html": HTMLExtractor,
    ".htm": HTMLExtractor,
    ".csv": CSVExtractor,
    ".tsv": CSVExtractor,
    ".pdf": PDFExtractor
}

def get_extractor_for_extension(filename: str) -> BaseExtractor:
    extension = Path(filename).suffix
    extractor_cls = _REGISTRY.get(extension.lower())
    if extractor_cls is None:
        raise ValueError(f"No extractor found for extension: {extension}")
    return extractor_cls()