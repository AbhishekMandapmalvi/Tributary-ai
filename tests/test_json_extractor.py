import pytest
from tributary.extractors.json_extractor import JSONExtractor


@pytest.mark.asyncio
async def test_flat_object():
    extractor = JSONExtractor()
    raw = b'{"name": "Alice", "role": "Engineer"}'
    result = await extractor.extract(raw, "person.json")
    assert result.text == "name: Alice\nrole: Engineer"
    assert result.content_type == "json"
    assert result.source_name == "person.json"
    assert result.extraction_time_ms >= 0


@pytest.mark.asyncio
async def test_nested_object():
    extractor = JSONExtractor()
    raw = b'{"name": "Alice", "team": {"name": "Platform", "lead": "Charlie"}}'
    result = await extractor.extract(raw, "person.json")
    lines = result.text.split("\n")
    assert "name: Alice" in lines
    assert "team.name: Platform" in lines
    assert "team.lead: Charlie" in lines


@pytest.mark.asyncio
async def test_array_of_primitives():
    extractor = JSONExtractor()
    raw = b'{"name": "Alice", "skills": ["Python", "Kafka", "Docker"]}'
    result = await extractor.extract(raw, "person.json")
    lines = result.text.split("\n")
    assert "name: Alice" in lines
    assert "skills: Python, Kafka, Docker" in lines


@pytest.mark.asyncio
async def test_array_of_objects():
    extractor = JSONExtractor()
    raw = b'{"employees": [{"name": "Alice"}, {"name": "Bob"}]}'
    result = await extractor.extract(raw, "team.json")
    lines = result.text.split("\n")
    assert "employees.0.name: Alice" in lines
    assert "employees.1.name: Bob" in lines


@pytest.mark.asyncio
async def test_deeply_nested():
    extractor = JSONExtractor()
    raw = b'{"a": {"b": {"c": "deep"}}}'
    result = await extractor.extract(raw, "deep.json")
    assert result.text == "a.b.c: deep"


@pytest.mark.asyncio
async def test_encoding_fallback():
    extractor = JSONExtractor()
    raw = '{"city": "café"}'.encode("latin-1")
    result = await extractor.extract(raw, "place.json")
    assert "city: café" in result.text


@pytest.mark.asyncio
async def test_invalid_json_raises_with_source_name():
    extractor = JSONExtractor()
    raw = b"not valid json"
    with pytest.raises(ValueError, match="Invalid JSON in 'bad_data.json'"):
        await extractor.extract(raw, "bad_data.json")
