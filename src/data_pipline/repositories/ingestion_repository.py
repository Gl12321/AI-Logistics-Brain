from typing import List, Dict, Any, Tuple
from sqlalchemy.dialects.postgresql import insert
from pgvector.sqlalchemy import Vector

from src.infrastructure.db.pg.postgres_client import postgres_client
from src.infrastructure.db.pg.models import Companies, Chunks, Managers, Holdings, ChunkEmbeddings
from src.core.logger import setup_logger

logger = setup_logger("INGESTION_REPOSITORY")


class IngestionRepository:
    def __init__(self):
        self.db = postgres_client

    async def save_form10company(self, data: Dict[str, Any]):
        if not data or not data.get('cik'):
            logger.error("Invalid data format")
            return

        session = self.db.get_session()

        stmt = insert(Companies).values({
            "cik": data["cik"],
            "formId": data["formId"],
            "cusip6": data["cusip6"],
            "cusip": data.get("cusip", []),
            "names": data["names"],
            "source": data["source"],
            "summary": data.get("summary")
        })

        stmt = stmt.on_conflict_do_update(
            index_elements=['cik'],
            set_={
                "formId": stmt.excluded.formId,
                "cusip6": stmt.excluded.cusip6,
                "cusip": stmt.excluded.cusip,
                "names": stmt.excluded.names,
                "source": stmt.excluded.source,
                "summary": stmt.excluded.summary
            }
        )

        await session.execute(stmt)
        await session.commit()
        logger.info(f"Successfully saved formId: {data['formId']} companies")

        await session.close()

    async def save_companies_embeddings(self, company_embeddings: List[Dict[str, Vector]], batch=5):
        pass

    async def save_10k_chunks(self, chunks: List[Dict[str, Any]], batch_size: int = 500):
        if not chunks:
            logger.error("No chunks to save")
            return

        session = self.db.get_session()

        total = len(chunks)
        for i in range(0, total, batch_size):
            batch = chunks[i:i + batch_size]
            stmt = insert(Chunks).values([
                {**item, "names": item["names"]} for item in batch
            ])

            stmt = stmt.on_conflict_do_nothing(index_elements=['chunkId'])
            await session.execute(stmt)

        await session.commit()

        logger.info(f"Successfully saved {len(chunks)} chunks total")
        await session.close()

    async def save_embeddings_for_chunks(self, chunk_embeddings: List[Tuple[str, Vector]], batch_size: int = 500):
        # Convert tuples to dicts for SQLAlchemy insert
        records = [{"chunkId": chunk_id, "embeddings": embedding} for chunk_id, embedding in chunk_embeddings]
        
        async with self.db.get_session() as session:
            total = len(records)
            for i in range(0, total, batch_size):
                batch = records[i:i + batch_size]
                stmt = insert(ChunkEmbeddings).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['chunkId'],
                    set_={
                        "embeddings": stmt.excluded.embeddings
                    }
                )
                await session.execute(stmt)
            
            await session.commit()
            logger.info(f"Successfully saved {len(records)} embeddings")

    async def save_13f_holding_managers(self, data: tuple[List[Dict[str, Any]], List[Dict[str, Any]]], batch_size: int = 1000):
        session = self.db.get_session()

        managers_data, holdings = data
        if managers_data:
            total_m = len(managers_data)
            for i in range(0, total_m, batch_size):
                batch = managers_data[i:i + batch_size]
                stmt = insert(Managers).values(batch)
                stmt = stmt.on_conflict_do_nothing(index_elements=['manager_cik'])
                await session.execute(stmt)
                logger.info(f"Saved managers batch {i//batch_size + 1}/{(total_m//batch_size)+1} ({len(batch)} records)")

            logger.info(f"Successfully saved {len(managers_data)} managers total")

        if holdings:
            total = len(holdings)
            for i in range(0, total, batch_size):
                batch = holdings[i:i + batch_size]
                hold_stmt = insert(Holdings).values(batch)
                hold_stmt = hold_stmt.on_conflict_do_nothing()
                await session.execute(hold_stmt)
                logger.info(f"Saved holdings batch {i//batch_size + 1}/{(total//batch_size)+1} ({len(batch)} records)")

            logger.info(f"Successfully saved {len(holdings)} holdings total")

        await session.commit()
        await session.close()