"""Chunk quality scoring — detect and flag low-quality chunks before embedding.

Scores each chunk 0.0 (garbage) to 1.0 (high quality) based on:
- Length: too short chunks carry no useful context
- Whitespace ratio: mostly whitespace = formatting artifact
- Alphabetic ratio: garbled text / binary content has few letters
- Repetition: repeated characters/words = extraction artifacts
- Sentence structure: presence of real sentences (capital + period)

Can be used as a pipeline hook to filter garbage before wasting embedding API calls.

Usage:
    from tributary.pipeline.quality import ChunkQualityScorer

    scorer = ChunkQualityScorer(min_score=0.3)
    hooks = PipelineHooks()
    hooks.after_chunk(scorer.as_chunk_filter())

    # Or score manually
    score, details = scorer.score("Some text")
"""
from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Callable
import re


@dataclass
class QualityScore:
    overall: float
    length_score: float
    whitespace_score: float
    alpha_score: float
    repetition_score: float
    structure_score: float


class ChunkQualityScorer:
    def __init__(
        self,
        min_length: int = 20,
        ideal_length: int = 200,
        max_whitespace_ratio: float = 0.5,
        min_alpha_ratio: float = 0.5,
        max_repetition_ratio: float = 0.3,
        min_score: float = 0.3,
        weights: dict[str, float] | None = None,
    ) -> None:
        self.min_length = min_length
        self.ideal_length = ideal_length
        self.max_whitespace_ratio = max_whitespace_ratio
        self.min_alpha_ratio = min_alpha_ratio
        self.max_repetition_ratio = max_repetition_ratio
        self.min_score = min_score
        self.weights = weights or {
            "length": 0.20,
            "whitespace": 0.20,
            "alpha": 0.25,
            "repetition": 0.15,
            "structure": 0.20,
        }

    def score(self, text: str) -> tuple[float, QualityScore]:
        """Score a text chunk. Returns (overall_score, detailed_scores)."""
        if not text or not text.strip():
            return 0.0, QualityScore(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        length = self._score_length(text)
        whitespace = self._score_whitespace(text)
        alpha = self._score_alpha(text)
        repetition = self._score_repetition(text)
        structure = self._score_structure(text)

        w = self.weights
        overall = (
            w["length"] * length
            + w["whitespace"] * whitespace
            + w["alpha"] * alpha
            + w["repetition"] * repetition
            + w["structure"] * structure
        )
        # Hard penalty: text below min_length can't score above 0.3
        if len(text) < self.min_length:
            overall = min(overall, 0.3 * (len(text) / self.min_length))
        overall = max(0.0, min(1.0, overall))

        details = QualityScore(
            overall=round(overall, 3),
            length_score=round(length, 3),
            whitespace_score=round(whitespace, 3),
            alpha_score=round(alpha, 3),
            repetition_score=round(repetition, 3),
            structure_score=round(structure, 3),
        )
        return overall, details

    def _score_length(self, text: str) -> float:
        n = len(text)
        if n == 0:
            return 0.0
        if n < self.min_length:
            return n / self.min_length * 0.5
        if n >= self.ideal_length:
            return 1.0
        return 0.5 + 0.5 * ((n - self.min_length) / (self.ideal_length - self.min_length))

    def _score_whitespace(self, text: str) -> float:
        if not text:
            return 0.0
        ws_count = sum(1 for c in text if c.isspace())
        ratio = ws_count / len(text)
        if ratio <= 0.15:
            return 1.0
        if ratio >= self.max_whitespace_ratio:
            return 0.0
        return 1.0 - (ratio - 0.15) / (self.max_whitespace_ratio - 0.15)

    def _score_alpha(self, text: str) -> float:
        if not text:
            return 0.0
        alpha_count = sum(1 for c in text if c.isalpha())
        ratio = alpha_count / len(text)
        if ratio >= self.min_alpha_ratio:
            return 1.0
        return ratio / self.min_alpha_ratio

    def _score_repetition(self, text: str) -> float:
        if len(text) < 10:
            return 0.3
        # Check character-level repetition (e.g., "aaaaaaa")
        max_run = 1
        current_run = 1
        for i in range(1, len(text)):
            if text[i] == text[i - 1]:
                current_run += 1
                max_run = max(max_run, current_run)
            else:
                current_run = 1
        char_rep = max_run / len(text)

        # Check word-level repetition
        words = text.lower().split()
        if len(words) > 2:
            unique = len(set(words))
            word_rep = 1.0 - (unique / len(words))
        else:
            word_rep = 0.0

        combined = max(char_rep, word_rep)
        if combined <= 0.1:
            return 1.0
        if combined >= self.max_repetition_ratio:
            return 0.0
        return 1.0 - (combined - 0.1) / (self.max_repetition_ratio - 0.1)

    def _score_structure(self, text: str) -> float:
        if not text:
            return 0.0
        # Check for sentence-like patterns: capital letter ... period
        sentences = re.findall(r'[A-Z][^.!?]*[.!?]', text)
        if len(sentences) >= 2:
            return 1.0
        if len(sentences) == 1:
            return 0.7
        # Check for at least some capitalization and punctuation
        has_caps = any(c.isupper() for c in text)
        has_punct = any(c in '.!?,:;' for c in text)
        if has_caps and has_punct:
            return 0.4
        if has_caps or has_punct:
            return 0.2
        return 0.0

    def is_quality(self, text: str) -> bool:
        """Returns True if chunk meets minimum quality threshold."""
        overall, _ = self.score(text)
        return overall >= self.min_score

    def as_chunk_filter(self) -> Callable:
        """Return a hook-compatible function for use with PipelineHooks.after_chunk."""
        def _filter(chunks, source_name):
            return [c for c in chunks if self.is_quality(c.text)]
        return _filter
