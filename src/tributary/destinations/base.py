from abc import ABC, abstractmethod
from tributary.embedders.models import EmbeddingResult


class BaseDestination(ABC):
    @abstractmethod
    async def store(self, results: list[EmbeddingResult]) -> None:
        """Store a batch of embedding results."""
        pass

    async def close(self) -> None:
        """Clean up resources. Override in subclasses that need it."""
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
