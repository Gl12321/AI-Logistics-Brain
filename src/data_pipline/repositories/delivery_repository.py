from typing import List, Dict, Any, Tuple
from collections import defaultdict
import sqlalchemy
import re

from src.infrastructure.db.pg.postgres_client import postgres_client
from src.infrastructure.db.pg.models import Companies, Form10, Chunks, ChunkEmbeddings, Form10Embeddings, ItemsEmbeddings, Managers, Holdings
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
            stmt = (
                sqlalchemy.select(
                    ChunkEmbeddings.item_id,
                    ChunkEmbeddings.embeddings
                )
                .where(
                    ChunkEmbeddings.item_id.isnot(None),
                    ChunkEmbeddings.embeddings.isnot(None)
                )
                .limit(limit)
                .offset(offset)
            )
            result = await session.execute(stmt)
            rows = result.all()

            grouped = defaultdict(list)
            for row in rows:
                grouped[row.item_id].append(row.embeddings.tolist())

            return list(grouped.items())

    async def get_chunks_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Chunks.chunk_id,
                Chunks.form_id,
                Chunks.item,
                Chunks.cik,
                Chunks.cusip6,
                Chunks.text,
                Chunks.names,
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
            embedding = row.embeddings.tolist() if row.embeddings is not None else None
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
                "names": row.names,
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
                Form10.names
            )
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        forms = []
        for row in rows:
            forms.append({
                "form_id": row.form_id,
                "cik": row.cik,
                "cusip6": row.cusip6,
                "source": row.source,
                "summary": row.summary,
                "names": row.names
            })
        return forms

    async def get_sections_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        item_id_expr = Chunks.form_id + '-' + Chunks.item

        stmt = (
            sqlalchemy.select(
                Chunks.item,
                Chunks.form_id,
                ItemsEmbeddings.embeddings
            )
            .distinct()
            .outerjoin(
                ItemsEmbeddings,
                ItemsEmbeddings.item_id == item_id_expr
            )
            .where(
                Chunks.item.isnot(None),
                Chunks.form_id.isnot(None)
            )
            .order_by(Chunks.form_id, Chunks.item)
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        sections = []
        for row in rows:
            section_id = f"{row.form_id}-{row.item}"
            name = SECTION_NAMES.get(row.item, row.item)
            embedding = row.embeddings.tolist() if row.embeddings is not None else None
            sections.append({
                "section_id": section_id,
                "item": row.item,
                "name": name,
                "form_id": row.form_id,
                "text_embedding": embedding
            })
        return sections

    async def get_companies_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = (
            sqlalchemy.select(
                Companies.cik,
                Companies.names,
                Companies.cusip6,
                Companies.name
            )
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        companies = []
        for row in rows:
            companies.append({
                "cik": row.cik,
                "name": row.name,
                "cusip6": row.cusip6,
                "names": row.names
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

    async def get_form_embeddings_for_graph(self, limit: int = 1000, offset: int = 0) -> Dict[str, List[float]]:
        from src.infrastructure.db.pg.models import Form10Embeddings

        stmt = (
            sqlalchemy.select(
                Form10Embeddings.form_id,
                Form10Embeddings.embeddings
            )
            .where(Form10Embeddings.embeddings.isnot(None))
            .limit(limit)
            .offset(offset)
        )

        async with self.db.get_session() as session:
            result = await session.execute(stmt)
            rows = result.all()

        embeddings = {}
        for row in rows:
            embeddings[row.form_id] = row.embeddings.tolist()
        return embeddings

    async def get_holdings_for_graph(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = sqlalchemy.text("""
            SELECT
                manager_cik,
                cusip6,
                array_agg(value) as values,
                array_agg(shares) as shares,
                array_agg(report_date) as dates,
                array_agg(cusip) as cusips
            FROM (
                SELECT DISTINCT manager_cik, cusip6, value, shares, report_date, cusip
                FROM edgar.holdings
                WHERE manager_cik IS NOT NULL AND cusip6 IS NOT NULL
            ) sub
            GROUP BY manager_cik, cusip6
            LIMIT :limit OFFSET :offset
        """)

        async with self.db.get_session() as session:
            result = await session.execute(stmt, {"limit": limit, "offset": offset})
            rows = result.all()

        holdings = []
        for row in rows:
            holdings.append({
                "manager_cik": row.manager_cik,
                "cusip6": row.cusip6,
                "values": row.values,
                "shares": row.shares,
                "dates": row.dates,
                "cusips": row.cusips
            })
        return holdings