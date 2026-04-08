"""Estimate embedding API costs before running the pipeline.

Counts tokens/characters across all source documents, applies provider pricing,
and returns a cost estimate without making any API calls.
"""
from dataclasses import dataclass


# Pricing per 1M tokens (as of 2025). Users can override.
DEFAULT_PRICING = {
    "text-embedding-3-small": 0.02,
    "text-embedding-3-large": 0.13,
    "text-embedding-ada-002": 0.10,
    "embed-english-v3.0": 0.10,
    "embed-multilingual-v3.0": 0.10,
    "voyage-3": 0.06,
    "voyage-3-lite": 0.02,
}


@dataclass
class CostEstimate:
    total_characters: int
    estimated_tokens: int
    model_name: str
    price_per_million_tokens: float
    estimated_cost_usd: float
    document_count: int


def estimate_cost(
    texts: list[str],
    model_name: str = "text-embedding-3-small",
    price_per_million_tokens: float | None = None,
    chars_per_token: float = 4.0,
) -> CostEstimate:
    """
    Estimate embedding cost for a list of texts.

    Args:
        texts: The text chunks that would be embedded.
        model_name: The embedding model name (for pricing lookup).
        price_per_million_tokens: Override pricing. If None, uses DEFAULT_PRICING.
        chars_per_token: Average characters per token (default 4.0 for English).

    Returns:
        CostEstimate with token count and estimated USD cost.
    """
    total_chars = sum(len(t) for t in texts)
    estimated_tokens = int(total_chars / chars_per_token)

    if price_per_million_tokens is None:
        price_per_million_tokens = DEFAULT_PRICING.get(model_name, 0.0)

    cost = (estimated_tokens / 1_000_000) * price_per_million_tokens

    return CostEstimate(
        total_characters=total_chars,
        estimated_tokens=estimated_tokens,
        model_name=model_name,
        price_per_million_tokens=price_per_million_tokens,
        estimated_cost_usd=round(cost, 6),
        document_count=len(texts),
    )
