import os
import asyncio
from sentence_transformers import SentenceTransformer

from src.core.config import get_settings
from src.core.logger import setup_logger

logger = setup_logger("EMBEDDER")
settings = get_settings()


class Embedder:
    def __init__(self):
        embedder_config = settings.MODELS["embedder"]
        cache_path = embedder_config["cache_path"]

        if not cache_path.exists():
            os.mkdir(cache_path)

        self.model = SentenceTransformer(
            embedder_config["repo_id"],
            device = embedder_config["device"],
            cache_folder = cache_path,
            dimension = embedder_config["dimension"]
        )
        logger.info("Model loaded")

    async def get_embeddings(self, texts):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.model.encode(texts, show_progress_bar=False)
        )
        return result.tolist()