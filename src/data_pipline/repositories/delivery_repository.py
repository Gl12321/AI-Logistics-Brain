from typing import List, Dict, Any, Tuple
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert

from src.infrastructure.db.pg.postgres_client import postgres_client
from src.infrastructure.db.pg.models import Companies, Chunks, ChunkEmbeddings, Managers, Holdings
from src.core.logger import setup_logger

logger = setup_logger("DELIVERY_REPOSITORY")


class DeliveryRepository:
    def __init__(self, limit: int = 100):
        self.db = postgres_client

    async def get_chunks_without_embeddings(self, limit: int = 100) -> List[Tuple[str, str]]:
        stmt = (
            sqlalchemy.select(Chunks.chunkId, Chunks.text)
            .outerjoin(ChunkEmbeddings, Chunks.chunkId == ChunkEmbeddings.chunkId)
            .where(
                (ChunkEmbeddings.chunkId.is_(None)) |
                (ChunkEmbeddings.embeddings.is_(None))
            )
            .limit(limit)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        logger.info(f"Fetched {len(rows)} chunks for embedder")
        return rows

    async def get_companies_without_summaries(self):
        pass

    async def get_companies_without_embeddings(self):
        pass