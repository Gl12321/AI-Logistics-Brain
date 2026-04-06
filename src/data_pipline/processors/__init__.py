from typing import Dict
from pathlib import Path
from src.core.logger import setup_logger

logger = setup_logger("IngestionService")


class IngestionService:
    def __init__(self, repository, extractors: Dict, summary_engine):
        self.repository = repository
        self.extractors = extractors
        self.summary_engine = summary_engine

    async def process_folders(self, folder_path: Path):
        # 1. Сначала создаем структуру (метаданные + саммари)
        await self.process_form10(folder_path)
        # 2. Отдельно нарезаем и сохраняем чанки для поиска/RAG
        await self.process_form10_chunks(folder_path)
        # 3. Обрабатываем отчетность 13F
        await self.process_form13(folder_path)

    async def process_form10(self, folder_path: Path):
        """Создание основного узла Form10 и его саммаризация"""
        target = folder_path / "form10"
        extractor = self.extractors.get("10k")  # Твой основной экстрактор

        for file_path in target.glob('*.json'):
            try:
                # Извлекаем всё: и метаданные, и полный текст
                data = extractor.extract(file_path)

                # ШАГ 1: Сохраняем только метаданные (независимо от LLM)
                await self.repository.save_form_metadata(data['metadata'])

                # ШАГ 2: Саммаризация (отдельный процесс обогащения)
                if data.get('full_text'):
                    try:
                        summary = self.summary_engine.summarize(data['full_text'])
                        await self.repository.update_form_summary(data['metadata']['formId'], summary)
                    except Exception as llm_e:
                        logger.warning(f"LLM Summary failed for {file_path.name}: {llm_e}")

            except Exception as e:
                logger.error(f"Error in process_form10 for {file_path.name}: {e}")

    async def process_form10_chunks(self, folder_path: Path):
        """Отдельный процесс: нарезка текста на куски и их запись"""
        target = folder_path / "form10"
        extractor = self.extractors.get("10k_chunks")  # Экстрактор-сплиттер

        for file_path in target.glob('*.json'):
            try:
                # Здесь extractor.parse просто возвращает список чанков с метаданными
                chunks_data = extractor.parse(file_path)
                await self.repository.save_10k_chunks(chunks_data)
                logger.info(f"Chunks for {file_path.name} saved.")
            except Exception as e:
                logger.error(f"Error in process_form10_chunks: {e}")

    async def process_form13(self, folder_path: Path):
        target = folder_path / "form13"
        for file_path in target.glob("*.csv"):
            await self._run_extraction(file_path, '13f')

    async def _run_extraction(self, file_path: Path, doc_type: str):
        extractor = self.extractors.get(doc_type)
        if not extractor: return

        try:
            data = extractor.parse(file_path)
            if doc_type == "13f":
                await self.repository.save_13f_holdings(data)
            logger.info(f"Processed {file_path.name}")
        except Exception as e:
            logger.error(f"Failed {file_path.name}: {e}")