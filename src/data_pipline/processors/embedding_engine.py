from typing import List, Tuple, Optional
import numpy as np
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

    async def run_form10_embedding(self, records: List[Tuple[str, str]]) -> List[Tuple[str, Vector]]:
        ids = [r[0] for r in records]
        texts = [r[1] for r in records]

        vectors = await self.embedder.get_embeddings(texts)
        upload_data = list(zip(ids, vectors))

        logger.info(f"Indexed {len(ids)} form10")
        return upload_data

    def aggregate_embeddings(self, embeddings_list: List[List[float]]) -> Optional[List[float]]:
        if not embeddings_list:
            return None
        embeddings_array = np.array(embeddings_list)
        mean_embedding = np.mean(embeddings_array, axis=0)
        return mean_embedding.tolist()