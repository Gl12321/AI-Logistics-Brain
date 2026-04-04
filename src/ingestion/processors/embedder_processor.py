from src.core.logger import setup_logger
from src.infrastructure.llm.embedder import Embedder

logger = setup_logger("EmbedderProcessor")


class EmbedderProcessor:
    def __init__(self, repository, embedder: Embedder):
        self.repo = repository
        self.embedder = embedder

    async def run_chunks_embedding(self, batch_size=10):
        logger.info("Starting chunks embedding process")

        while True:
            records = self.repo.get_unindexed_chunks(limit=batch_size)
            if not records:
                logger.info("All chunks are already embedded")
                break

            ids = [r['chunk_id'] for r in records]
            texts = [r["chunk_text"] for r in records]

            vectors = await self.embedder.get_embeddings(texts)
            upload_data = list(zip(ids, vectors))
            self.repo.save_chunck_embeddings(upload_data)

            logger.info(f"Indexed {len(ids)} chunks")

    async def run_form10_embedding(self, batch_size=5):
        logger.info("Starting form10 embedding process")

        while True:
            records = self.repo.get_unindexed_form10(limit=batch_size)
            if not records:
                logger.info("All forms are already embedded")
                break

            ids = [r['form_id'] for r in records]
            texts = [r['full_text'] for r in records]

            vectors = await self.embedder.get_embeddings(texts)
            upload_data = list(zip(ids, vectors))
            self.repo.save_form10_embeddings(upload_data)

            logger.info(f"Indexed {len(ids)} forms")