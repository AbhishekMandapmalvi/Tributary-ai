"""Adaptive batch sizing based on embedding API response times.

Starts at an initial batch size and adjusts up/down based on observed latency:
- If latency is below the target → grow batch size (fewer API calls)
- If latency is above the target → shrink batch size (avoid timeouts/rate limits)
- On errors (rate limits, timeouts) → halve immediately

Uses exponential moving average to smooth out latency spikes.
"""
import asyncio
import structlog

logger = structlog.get_logger(__name__)


class AdaptiveBatcher:
    def __init__(
        self,
        initial_batch_size: int = 64,
        min_batch_size: int = 8,
        max_batch_size: int = 512,
        target_latency_ms: float = 2000.0,
        growth_factor: float = 1.25,
        shrink_factor: float = 0.75,
        ema_alpha: float = 0.3,
    ) -> None:
        self.batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_latency_ms = target_latency_ms
        self.growth_factor = growth_factor
        self.shrink_factor = shrink_factor
        self.ema_alpha = ema_alpha
        self._avg_latency_ms: float | None = None
        self._lock = asyncio.Lock()
        self._total_adjustments = 0

    async def record_latency(self, latency_ms: float) -> None:
        """Record a batch's latency and adjust batch size accordingly."""
        async with self._lock:
            # Update exponential moving average
            if self._avg_latency_ms is None:
                self._avg_latency_ms = latency_ms
            else:
                self._avg_latency_ms = (
                    self.ema_alpha * latency_ms
                    + (1 - self.ema_alpha) * self._avg_latency_ms
                )

            old_size = self.batch_size

            if self._avg_latency_ms < self.target_latency_ms * 0.7:
                # Well under target — grow
                self.batch_size = min(
                    int(self.batch_size * self.growth_factor),
                    self.max_batch_size,
                )
            elif self._avg_latency_ms > self.target_latency_ms:
                # Over target — shrink
                self.batch_size = max(
                    int(self.batch_size * self.shrink_factor),
                    self.min_batch_size,
                )

            if self.batch_size != old_size:
                self._total_adjustments += 1
                logger.debug(
                    "Batch size adjusted",
                    old=old_size,
                    new=self.batch_size,
                    avg_latency_ms=round(self._avg_latency_ms, 1),
                    target_ms=self.target_latency_ms,
                )

    async def record_error(self) -> None:
        """On error (rate limit, timeout), halve batch size immediately."""
        async with self._lock:
            old_size = self.batch_size
            self.batch_size = max(self.batch_size // 2, self.min_batch_size)
            self._total_adjustments += 1
            logger.warning("Batch size halved after error", old=old_size, new=self.batch_size)

    def get_batch_size(self) -> int:
        return self.batch_size

    def summary(self) -> dict:
        return {
            "current_batch_size": self.batch_size,
            "avg_latency_ms": round(self._avg_latency_ms, 1) if self._avg_latency_ms else None,
            "total_adjustments": self._total_adjustments,
        }
