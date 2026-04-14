from typing import List, Dict, Any, Tuple
import sqlalchemy
import re

from src.infrastructure.db.pg.postgres_client import postgres_client
from src.infrastructure.db.pg.models import Companies, Form10, Chunks, ChunkEmbeddings, Form10Embeddings, Managers, Holdings
from src.infrastructure.db.neo4j.models import SECTION_NAMES
from src.core.logger import setup_logger

logger = setup_logger("DELIVERY_REPOSITORY")


class DeliveryRepository:
    def __init__(self, limit: int = 100):
        self.db = postgres_client

    async def get_chunks_without_embeddings(self, limit: int = 100) -> List[Tuple[str, str]]:
        stmt = (
            sqlalchemy.select(Chunks.chunk_id, Chunks.text)
            .outerjoin(ChunkEmbeddings, Chunks.chunk_id == ChunkEmbeddings.chunk_id)
            .where(
                (ChunkEmbeddings.chunk_id.is_(None)) |
                (ChunkEmbeddings.embeddings.is_(None))
            )
            .limit(limit)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        logger.info(f"Fetched {len(rows)} chunks for embedder")
        return rows

    async def get_form10_without_embeddings(self, limit: int = 100) -> List[Tuple[str, str]]:
        stmt = (
            sqlalchemy.select(Form10.form_id, Form10.summary)
            .outerjoin(Form10Embeddings, Form10.form_id == Form10Embeddings.form_id)
            .where(
                (Form10Embeddings.form_id.is_(None)) |
                (Form10Embeddings.embeddings.is_(None))
            )
            .where(Form10.summary.isnot(None))
            .limit(limit)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        logger.info(f"Fetched {len(rows)} companies for embedder")
        return rows

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
                    # Parse "[0.1,0.2,...]" string to list of floats
                    values = text.strip('[]').split(',')
                    embeddings_list.append([float(v) for v in values])
                results.append((row.item_id, embeddings_list))
            return results

    async def get_chunks_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Chunks.chunk_id,
                Chunks.form_id,
                Chunks.item,
                Chunks.cik,
                Chunks.cusip6,
                Chunks.text,
                ChunkEmbeddings.embeddings
            )
            .join(ChunkEmbeddings, Chunks.chunk_id == ChunkEmbeddings.chunk_id)
            .where(ChunkEmbeddings.embeddings.isnot(None))
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        chunks = []
        for row in rows:
            embedding = None
            if row.embeddings is not None:
                text = str(row.embeddings).strip('[]')
                values = [v for v in re.split(r'[,\s]+', text) if v]
                embedding = [float(v) for v in values]
            match = re.search(r'-chunk(\d{4})$', row.chunk_id)
            sequence = int(match.group(1)) if match else 0
            chunks.append({
                "chunk_id": row.chunk_id,
                "form_id": row.form_id,
                "item": row.item,
                "sequence": sequence,
                "cik": row.cik,
                "cusip6": row.cusip6,
                "text": row.text,
                "text_embedding": embedding
            })
        return chunks

    async def get_forms_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Form10.form_id,
                Form10.cik,
                Form10.cusip6,
                Form10.source,
                Form10.summary,
                Companies.names
            )
            .join(Companies, Form10.cik == Companies.cik)
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        forms = []
        for row in rows:
            names = row.names if isinstance(row.names, list) else []
            forms.append({
                "form_id": row.form_id,
                "cik": row.cik,
                "cusip6": row.cusip6,
                "source": row.source,
                "summary": row.summary,
                "names": names
            })
        return forms

    async def get_sections_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = sqlalchemy.text("""
            SELECT DISTINCT item, form_id
            FROM edgar.chunks
            WHERE item IS NOT NULL AND form_id IS NOT NULL
            ORDER BY form_id, item
            LIMIT :limit OFFSET :offset
        """)

        async with self.db.get_session() as session:
            result = await session.execute(stmt, {"limit": limit, "offset": offset})
            rows = result.all()

        sections = []
        for row in rows:
            section_id = f"{row.form_id}-{row.item}"
            name = SECTION_NAMES.get(row.item, row.item)
            sections.append({
                "section_id": section_id,
                "item": row.item,
                "name": name,
                "form_id": row.form_id
            })
        return sections

    async def get_companies_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Companies.cik,
                Companies.names,
                Companies.cusip6,
                Companies.address
            )
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        companies = []
        for row in rows:
            name = row.names[0] if isinstance(row.names, list) and row.names else ""
            companies.append({
                "cik": row.cik,
                "name": name,
                "cusip6": row.cusip6,
                "address": row.address
            })
        return companies

    async def get_managers_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Managers.manager_cik,
                Managers.name,
                Managers.address
            )
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        managers = []
        for row in rows:
            managers.append({
                "manager_cik": row.manager_cik,
                "name": row.name,
                "address": row.address
            })
        return managers