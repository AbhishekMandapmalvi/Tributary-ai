import pytest
from tributary.sources.local_source import LocalSource


async def collect(source):
    results = []
    async for item in source.fetch():
        results.append(item)
    return results


@pytest.mark.asyncio
async def test_fetch_from_file_paths(tmp_path):
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"
    f1.write_bytes(b"hello")
    f2.write_bytes(b"world")

    source = LocalSource(file_paths=[str(f1), str(f2)])
    results = await collect(source)
    assert len(results) == 2
    texts = {r.raw_bytes for r in results}
    assert texts == {b"hello", b"world"}
    assert all(r.source_type == "local_file" for r in results)


@pytest.mark.asyncio
async def test_fetch_from_directory(tmp_path):
    (tmp_path / "one.txt").write_bytes(b"one")
    (tmp_path / "two.txt").write_bytes(b"two")

    source = LocalSource(directory=str(tmp_path))
    results = await collect(source)
    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"one.txt", "two.txt"}


@pytest.mark.asyncio
async def test_extension_filtering(tmp_path):
    (tmp_path / "doc.txt").write_bytes(b"text")
    (tmp_path / "data.csv").write_bytes(b"csv")
    (tmp_path / "image.png").write_bytes(b"png")

    source = LocalSource(directory=str(tmp_path), extensions=[".txt", ".csv"])
    results = await collect(source)
    assert len(results) == 2
    names = {r.file_name for r in results}
    assert names == {"doc.txt", "data.csv"}


@pytest.mark.asyncio
async def test_skips_unsupported_extensions(tmp_path):
    (tmp_path / "keep.txt").write_bytes(b"keep")
    (tmp_path / "skip.xyz").write_bytes(b"skip")

    source = LocalSource(directory=str(tmp_path), extensions=[".txt"])
    results = await collect(source)
    assert len(results) == 1
    assert results[0].file_name == "keep.txt"


def test_no_file_paths_or_directory_raises():
    with pytest.raises(ValueError, match="Provide file_paths, directory, or both"):
        LocalSource()


@pytest.mark.asyncio
async def test_nonexistent_file_handled_gracefully(tmp_path):
    real = tmp_path / "real.txt"
    real.write_bytes(b"exists")
    fake = tmp_path / "nonexistent.txt"

    source = LocalSource(file_paths=[str(real), str(fake)])
    results = await collect(source)
    # Should yield the real file and skip the nonexistent one without crashing
    assert len(results) == 1
    assert results[0].file_name == "real.txt"
