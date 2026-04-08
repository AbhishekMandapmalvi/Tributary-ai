import asyncio


class MetricsCollector:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._stages: dict[str, list[float]] = {}
        self._chunks: list[int] = []
        self._cache_hits: int = 0
        self._cache_misses: int = 0

    async def record_stage(self, file_name: str, stage: str, duration_ms: float):
        async with self._lock:
            if stage not in self._stages:
                self._stages[stage] = []
            self._stages[stage].append(duration_ms)

    async def record_chunks(self, file_name: str, count: int):
        async with self._lock:
            self._chunks.append(count)

    async def record_cache(self, hits: int, misses: int):
        async with self._lock:
            self._cache_hits += hits
            self._cache_misses += misses

    def summary(self) -> dict:
        result = {}

        for stage, durations in self._stages.items():
            result[stage] = {
                "count": len(durations),
                "total_ms": round(sum(durations), 2),
                "avg_ms": round(sum(durations) / len(durations), 2),
                "min_ms": round(min(durations), 2),
                "max_ms": round(max(durations), 2),
            }

        if self._chunks:
            result["chunks"] = {
                "total": sum(self._chunks),
                "avg_per_doc": round(sum(self._chunks) / len(self._chunks), 2),
                "min_per_doc": min(self._chunks),
                "max_per_doc": max(self._chunks),
            }

        total_cache = self._cache_hits + self._cache_misses
        result["cache"] = {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": round(self._cache_hits / total_cache, 2) if total_cache > 0 else 0.0,
        }

        return result
