from typing import List, Dict, Any, Tuple
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from pgvector.sqlalchemy import Vector

from src.infrastructure.db.pg.postgres_client import postgres_client
from src.infrastructure.db.pg.models import Companies, Form10, Chunks, Managers, Holdings, ChunkEmbeddings, Form10Embeddings, ItemsEmbeddings
from src.core.logger import setup_logger

logger = setup_logger("INGESTION_REPOSITORY")


class IngestionRepository:
    def __init__(self):
        self.db = postgres_client

    async def save_company(self, data: Dict[str, Any]):
        if not data or not data.get('cusip6'):
            logger.error("Invalid data format")
            return

        async with self.db.get_session() as session:
            stmt = insert(Companies).values({
                "cusip6": data["cusip6"],
                "cik": data["cik"],
                "cusip": data.get("cusip", []),
                "names": data["names"],
                "name": data["names"][0] if data.get("names") else None
            })
            stmt = stmt.on_conflict_do_nothing(index_elements=['cusip6'])
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Successfully saved company cusip6={data['cusip6']}")

    async def save_form10(self, data: Dict[str, Any]):
        if not data or not data.get('formId'):
            logger.error("Invalid data format")
            return

        async with self.db.get_session() as session:
            stmt = insert(Form10).values({
                "form_id": data["formId"],
                "cik": data["cik"],
                "cusip6": data["cusip6"],
                "cusip": data.get("cusip", []),
                "names": data["names"],
                "source": data["source"],
                "summary": data.get("summary")
            })
            stmt = stmt.on_conflict_do_update(
                index_elements=['form_id'],
                set_={
                    "cusip6": stmt.excluded.cusip6,
                    "cusip": stmt.excluded.cusip,
                    "names": stmt.excluded.names,
                    "source": stmt.excluded.source,
                    "summary": stmt.excluded.summary
                }
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Successfully saved form_id={data['formId']}")

    async def save_summaries(self, summaries: List[Dict[str, str]], batch_size: int = 100):
        async with self.db.get_session() as session:
            total = len(summaries)
            for i in range(0, total, batch_size):
                batch = summaries[i:i + batch_size]

                for item in batch:
                    stmt = (
                        sqlalchemy.update(Form10)
                        .where(Form10.form_id == item["formId"])
                        .values(summary=item["summary"])
                    )
                    await session.execute(stmt)
                await session.commit()

            logger.info(f"Successfully updated {total} summaries")

    async def save_form10_embeddings(self, form_embeddings: List[Tuple[str, Vector]], batch_size: int = 100):
        records = [{"form_id": form_id, "embeddings": embedding} for form_id, embedding in form_embeddings]

        async with self.db.get_session() as session:
            total = len(records)
            for i in range(0, total, batch_size):
                batch = records[i:i + batch_size]
                stmt = insert(Form10Embeddings).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['form_id'],
                    set_={
                        "embeddings": stmt.excluded.embeddings
                    }
                )
                await session.execute(stmt)

            await session.commit()
            logger.info(f"Successfully saved {len(records)} form10 embeddings")

    async def save_10k_chunks(self, chunks: List[Dict[str, Any]], batch_size: int = 500):
        if not chunks:
            logger.error("No chunks to save")
            return

        async with self.db.get_session() as session:
            total = len(chunks)
            for i in range(0, total, batch_size):
                batch = chunks[i:i + batch_size]
                stmt = insert(Chunks).values([
                    {
                        "chunk_id": item["chunkId"],
                        "form_id": item["formId"],
                        "cusip6": item["cusip6"],
                        "cik": item["cik"],
                        "item": item["item"],
                        "text": item["text"],
                        "names": item["names"],
                        "source": item.get("source")
                    } for item in batch
                ])
                stmt = stmt.on_conflict_do_nothing(index_elements=['chunk_id'])
                await session.execute(stmt)

            await session.commit()
            logger.info(f"Successfully saved {len(chunks)} chunks total")

    async def save_embeddings_for_chunks(self, chunk_embeddings: List[Tuple[str, Vector]], batch_size: int = 500):
        records = [{"chunk_id": chunk_id, "embeddings": embedding} for chunk_id, embedding in chunk_embeddings]

        async with self.db.get_session() as session:
            total = len(records)
            for i in range(0, total, batch_size):
                batch = records[i:i + batch_size]
                stmt = insert(ChunkEmbeddings).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['chunk_id'],
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

    async def fill_chunk_item_ids(self) -> int:
        async with self.db.get_session() as session:
            stmt = sqlalchemy.text("""
                UPDATE edgar.chunk_embeddings
                SET item_id = regexp_replace(chunk_id, '-chunk\\d{4}$', '')
                WHERE item_id IS NULL
            """)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_chunk_embeddings_grouped(self, limit: int = 1000, offset: int = 0) -> List[Tuple[str, List[List[float]]]]:
        async with self.db.get_session() as session:
            stmt = sqlalchemy.text("""
                SELECT item_id, array_agg(embeddings::text) as embeddings_texts
                FROM edgar.chunk_embeddings
                WHERE item_id IS NOT NULL AND embeddings IS NOT NULL
                GROUP BY item_id
                ORDER BY item_id
                LIMIT :limit OFFSET :offset
            """)
            result = await session.execute(stmt, {"limit": limit, "offset": offset})
            rows = result.all()
            results = []
            for row in rows:
                embeddings_list = []
                for text in row.embeddings_texts:
                    values = text.strip('[]').split(',')
                    embeddings_list.append([float(v) for v in values])
                results.append((row.item_id, embeddings_list))
            return results

    async def save_item_embeddings_batch(self, items_data: List[Dict[str, Any]], batch_size: int = 500):
        records = [{"item_id": item["item_id"], "embeddings": item["embeddings"]} for item in items_data]

        async with self.db.get_session() as session:
            total = len(records)
            for i in range(0, total, batch_size):
                batch = records[i:i + batch_size]
                stmt = insert(ItemsEmbeddings).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['item_id'],
                    set_={"embeddings": stmt.excluded.embeddings}
                )
                await session.execute(stmt)
            await session.commit()
            logger.info(f"Saved {len(records)} item embeddings")