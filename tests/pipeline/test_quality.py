import pytest
from tributary.pipeline.quality import ChunkQualityScorer, QualityScore
from tributary.chunkers.models import ChunkResult


@pytest.fixture
def scorer():
    return ChunkQualityScorer()


# --- Scoring individual signals ---

def test_good_text_scores_high(scorer):
    text = "The quick brown fox jumps over the lazy dog. This is a well-formed English sentence with proper structure."
    overall, details = scorer.score(text)
    assert overall > 0.7
    assert details.alpha_score > 0.7
    assert details.structure_score > 0.5


def test_empty_text_scores_zero(scorer):
    overall, details = scorer.score("")
    assert overall == 0.0


def test_very_short_text_scores_low(scorer):
    overall, _ = scorer.score("hi")
    assert overall < 0.5


def test_mostly_whitespace_scores_low(scorer):
    text = "a" + " " * 100 + "b"
    _, details = scorer.score(text)
    assert details.whitespace_score < 0.3


def test_garbled_text_scores_low(scorer):
    text = "1234567890!@#$%^&*(){}[]<>?/\\|~`+=_-" * 5
    _, details = scorer.score(text)
    assert details.alpha_score < 0.3


def test_repeated_chars_scores_low(scorer):
    text = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    _, details = scorer.score(text)
    assert details.repetition_score < 0.3


def test_repeated_words_scores_low(scorer):
    text = "the the the the the the the the the the the the the the the"
    _, details = scorer.score(text)
    assert details.repetition_score < 0.5


def test_no_sentence_structure_scores_low(scorer):
    text = "no capitals no periods no structure whatsoever just lowercase rambling"
    _, details = scorer.score(text)
    assert details.structure_score < 0.5


def test_good_structure_scores_high(scorer):
    text = "First sentence here. Second sentence follows. Third one too."
    _, details = scorer.score(text)
    assert details.structure_score > 0.7


# --- is_quality threshold ---

def test_is_quality_accepts_good_text(scorer):
    text = "The retrieval augmented generation pipeline processes documents in multiple stages. Each stage has specific responsibilities."
    assert scorer.is_quality(text)


def test_is_quality_rejects_garbage(scorer):
    assert not scorer.is_quality("")
    assert not scorer.is_quality("   ")
    assert not scorer.is_quality("aaa")


def test_is_quality_rejects_symbols_with_strict_threshold():
    strict = ChunkQualityScorer(min_score=0.6)
    assert not strict.is_quality("!@#$%^&*" * 5)


def test_custom_min_score():
    strict = ChunkQualityScorer(min_score=0.8)
    lenient = ChunkQualityScorer(min_score=0.1)
    text = "Short text."
    assert not strict.is_quality(text)
    assert lenient.is_quality(text)


# --- as_chunk_filter hook ---

def test_chunk_filter_removes_garbage(scorer):
    chunks = [
        ChunkResult(text="Good quality text with proper sentences. Multiple ones even.", source_name="a.txt", chunk_index=0, start_char=0, end_char=60),
        ChunkResult(text="   ", source_name="a.txt", chunk_index=1, start_char=61, end_char=64),
        ChunkResult(text="!@#$%", source_name="a.txt", chunk_index=2, start_char=65, end_char=70),
        ChunkResult(text="Another good sentence here. This one is fine too.", source_name="a.txt", chunk_index=3, start_char=71, end_char=120),
    ]
    filter_fn = scorer.as_chunk_filter()
    filtered = filter_fn(chunks, "a.txt")
    texts = [c.text for c in filtered]
    assert "   " not in texts
    assert "!@#$%" not in texts
    assert len(filtered) == 2


# --- QualityScore dataclass ---

def test_quality_score_dataclass():
    qs = QualityScore(
        overall=0.85, length_score=0.9, whitespace_score=1.0,
        alpha_score=0.8, repetition_score=0.9, structure_score=0.7,
    )
    assert qs.overall == 0.85
    assert qs.structure_score == 0.7


# --- Pipeline integration ---

@pytest.mark.asyncio
async def test_pipeline_with_quality_filter(tmp_path):
    from tributary.sources.local_source import LocalSource
    from tributary.chunkers.fixed_chunker import FixedChunker
    from tributary.embedders.custom_embedder import CustomEmbedder
    from tributary.destinations.json_destination import JSONDestination
    from tributary.pipeline.orchestrator import Pipeline
    from tributary.pipeline.hooks import PipelineHooks
    import json

    docs = tmp_path / "docs"
    docs.mkdir()
    # Mix of good content and garbage
    (docs / "mixed.txt").write_text(
        "The quick brown fox jumps over the lazy dog. This is a proper sentence.\n"
        "     \n"
        "!@#$%^&*()!@#$%^&*()\n"
        "Another well-formed paragraph with real content. It discusses important topics."
    )
    output = tmp_path / "output.jsonl"

    hooks = PipelineHooks()
    scorer = ChunkQualityScorer(min_score=0.3)
    hooks.after_chunk(scorer.as_chunk_filter())

    pipeline = Pipeline(
        source=LocalSource(directory=str(docs), extensions=[".txt"]),
        chunker=FixedChunker(chunk_size=40, overlap=0),
        embedder=CustomEmbedder(embed_fn=lambda t: [[0.0] * 3 for _ in t]),
        destination=JSONDestination(str(output)),
        hooks=hooks,
    )
    result = await pipeline.run()
    assert result.successful == 1

    # All stored chunks should be quality
    lines = output.read_text().strip().split("\n")
    for line in lines:
        chunk_text = json.loads(line)["chunk_text"]
        assert scorer.is_quality(chunk_text), f"Low-quality chunk stored: {chunk_text!r}"
