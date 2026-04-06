from typing import List, Tuple
from pgvector.sqlalchemy import Vector

from src.core.logger import setup_logger
from src.infrastructure.llm.embedder import Embedder

logger = setup_logger("EMBEDDER_PROCESSOR")


class EmbedderProcessor:
    def __init__(self, repository, embedder: Embedder):
        self.repo = repository
        self.embedder = embedder

    async def run_chunks_embedding(self, records: List[Tuple[str, str]], batch_size=10) -> List[Tuple[str, Vector]]:
        logger.info("Starting chunks embedding process")

        ids = [r[0] for r in records]
        texts = [r[1] for r in records]

        vectors = await self.embedder.get_embeddings(texts)
        upload_data = list(zip(ids, vectors))

        logger.info(f"Indexed {len(ids)} chunks")
        return upload_data

    async def run_company_embedding(self, records: List[Tuple[str, str]], batch_size=5):
        logger.info("Starting form10 embedding process")

        ids = [r['cik'] for r in records]
        texts = [r['summary'] for r in records]

        vectors = await self.embedder.get_embeddings(texts)
        upload_data = list(zip(ids, vectors))
        self.repo.save_form10_embeddings(upload_data)

        logger.info(f"Indexed {len(ids)} forms")