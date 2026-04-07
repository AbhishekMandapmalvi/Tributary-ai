import pytest
from tributary.extractors.csv_extractor import CSVExtractor


@pytest.mark.asyncio
async def test_basic_csv_key_value_format():
    extractor = CSVExtractor()
    raw = b"name,role,department\nAlice,Engineer,Platform\nBob,Designer,Product"
    result = await extractor.extract(raw, "team.csv")
    assert result.text == "name: Alice, role: Engineer, department: Platform\nname: Bob, role: Designer, department: Product"
    assert result.source_name == "team.csv"
    assert result.content_type == "csv"
    assert result.char_count == len(result.text)
    assert result.extraction_time_ms >= 0


@pytest.mark.asyncio
async def test_tsv_tab_delimiter():
    extractor = CSVExtractor()
    raw = b"name\trole\nAlice\tEngineer\nBob\tDesigner"
    result = await extractor.extract(raw, "team.tsv")
    assert result.text == "name: Alice, role: Engineer\nname: Bob, role: Designer"
    assert result.content_type == "tsv"


@pytest.mark.asyncio
async def test_csv_with_empty_values():
    extractor = CSVExtractor()
    raw = b"name,role,department\nAlice,,Platform\nBob,Designer,"
    result = await extractor.extract(raw, "team.csv")
    lines = result.text.split("\n")
    assert lines[0] == "name: Alice, role: , department: Platform"
    assert lines[1] == "name: Bob, role: Designer, department: "


@pytest.mark.asyncio
async def test_single_row_csv():
    extractor = CSVExtractor()
    raw = b"name,role\nAlice,Engineer"
    result = await extractor.extract(raw, "team.csv")
    assert result.text == "name: Alice, role: Engineer"
    assert "\n" not in result.text
    assert result.char_count == len("name: Alice, role: Engineer")
