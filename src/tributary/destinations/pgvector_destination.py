from __future__ import annotations
from typing import TYPE_CHECKING
from tributary.destinations.base import BaseDestination
from tributary.embedders.models import EmbeddingResult
import json

if TYPE_CHECKING:
    import asyncpg


class PgvectorDestination(BaseDestination):
    def __init__(self, dsn: str, table_name: str = "embeddings", vector_dimensions: int = 1536):
        self.dsn = dsn
        self.table_name = table_name
        self.vector_dimensions = vector_dimensions
        self.pool: asyncpg.Pool | None = None

    async def _ensure_pool(self) -> None:
        if self.pool is None:
            import asyncpg
            self.pool = await asyncpg.create_pool(self.dsn)
            async with self.pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id TEXT PRIMARY KEY,
                        embedding vector({self.vector_dimensions}),
                        chunk_text TEXT,
                        source_name TEXT,
                        chunk_index INTEGER,
                        model_name TEXT,
                        metadata JSONB
                    )
                """)

    async def store(self, results: list[EmbeddingResult]) -> None:
        if not results:
            return
        await self._ensure_pool()
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {self.table_name} (id, embedding, chunk_text, source_name, chunk_index, model_name, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    chunk_text = EXCLUDED.chunk_text,
                    source_name = EXCLUDED.source_name,
                    chunk_index = EXCLUDED.chunk_index,
                    model_name = EXCLUDED.model_name,
                    metadata = EXCLUDED.metadata
                """,
                [
                    (
                        f"{r.source_name}#{r.chunk_index}",
                        str(r.vector),
                        r.chunk_text,
                        r.source_name,
                        r.chunk_index,
                        r.model_name,
                        json.dumps({"source_name": r.source_name, "chunk_index": r.chunk_index}),
                    )
                    for r in results
                ],
            )

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
