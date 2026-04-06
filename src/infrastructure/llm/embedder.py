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
            os.makedirs(cache_path, exist_ok=True)

        logger.info(f"Loading embedder model: {embedder_config['repo_id']}")
        
        # Download model with progress bar if not cached
        from huggingface_hub import snapshot_download
        from tqdm import tqdm
        
        local_path = snapshot_download(
            repo_id=embedder_config["repo_id"],
            cache_dir=cache_path,
            local_files_only=False,
            tqdm_class=tqdm
        )
        logger.info(f"Model files ready at: {local_path}")
        
        self.model = SentenceTransformer(
            local_path,
            device=embedder_config["device"]
        )
        logger.info("Model loaded")

    async def get_embeddings(self, texts):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.model.encode(texts, show_progress_bar=False)
        )
        return result.tolist()