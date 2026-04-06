import asyncio
from sqlalchemy import inspect

from src.data_pipline.services.ingestion_service import IngestionService
from src.data_pipline.services.enrichment_service import EnrichmentService

from src.infrastructure.llm.embedder import Embedder
from src.data_pipline.processors.embedding_engine import EmbedderProcessor
from src.data_pipline.processors.summary_engine import SummaryEngine

from src.data_pipline.repositories.ingestion_repository import IngestionRepository
from src.data_pipline.repositories.delivery_repository import DeliveryRepository

from src.data_pipline.extractors.form10company_extractor import Form10Extractor
from src.data_pipline.extractors.form10chunks_extractor import Form10ChunksExtractor
from src.data_pipline.extractors.form13_extractor import Form13Extractor
from src.core.config import get_settings

from src.infrastructure.db.pg.models import Base
from src.infrastructure.db.pg.postgres_client import postgres_client

settings = get_settings()

async def create_table():
    engine = postgres_client.engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    print("Database setup")

    def get_tables(sync_conn):
        inspector = inspect(sync_conn)
        schema = Base.metadata.schema or "public"
        return inspector.get_table_names(schema=schema), schema

    async with engine.connect() as conn:
        tables, schema_name = await conn.run_sync(get_tables)

    print(f"schema: {schema_name}, tables: {tables}")

async def ingestion_metadata():
    writer_repository = IngestionRepository()
    extractors = {
        "10k_chunks": Form10ChunksExtractor(),
        "10k_companies": Form10Extractor(),
        "13f_managers_holdings": Form13Extractor()
    }

    service = IngestionService(writer_repository, extractors)
    await service.process_folders(settings.DATA_RAW_DIR)

async def calculate_embeddings_for_chunks():
    writer_repository = IngestionRepository()
    reader_repository = DeliveryRepository()
    embedder = Embedder()
    embedder_processor = EmbedderProcessor(None, embedder)
    service = EnrichmentService(writer_repository, reader_repository, None, embedder_processor)

    await service.run_chunks_embedding()

async def create_summary_for_companies():
    pass

async def calculate_embeddings_for_companies():
    pass

async def main():
    # await create_table()
    # await ingestion_metadata()
    await calculate_embeddings_for_chunks()
    await create_summary_for_companies()
    await calculate_embeddings_for_companies()

if __name__ == "__main__":
    asyncio.run(main())
