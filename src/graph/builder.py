from neomodel import adb
from src.core.logger import setup_logger
from src.infrastructure.db.neo4j.models import Chunk, Form, Section, Company, Manager
from src.core.config import get_settings

logger = setup_logger("GRAPH_BUILDER")

BATCH_SIZE = 1000


class GraphBuilder:
    def __init__(self, reader_repository, neo4j_client):
        self.repo = reader_repository
        self.neo4j = neo4j_client

    async def setup(self):
        settings = get_settings()
        uri = f"bolt://{settings.NEO4J_USER}:{settings.NEO4J_PASSWORD}@{settings.NEO4J_HOST}:{settings.NEO4J_PORT}"
        await adb.set_connection(uri)

    async def create_chunk_nodes(self):
        offset = 0
        total = 0

        while True:
            chunks = await self.repo.get_chunks_for_graph(limit=BATCH_SIZE, offset=offset)
            if not chunks:
                break

            for chunk_data in chunks:
                try:
                    await Chunk.nodes.get(chunk_id=chunk_data["chunk_id"])
                except Chunk.DoesNotExist:
                    await Chunk(
                        chunk_id=chunk_data["chunk_id"],
                        form_id=chunk_data["form_id"],
                        item=chunk_data["item"],
                        sequence=chunk_data["sequence"],
                        cik=chunk_data["cik"],
                        cusip6=chunk_data["cusip6"],
                        text=chunk_data["text"],
                        text_embedding=chunk_data["text_embedding"]
                    ).save()

            total += len(chunks)
            offset += BATCH_SIZE

        logger.info(f"Created/Updated {total} Chunk nodes")

    async def create_form_nodes(self):
        offset = 0
        total = 0

        while True:
            forms = await self.repo.get_forms_for_graph(limit=BATCH_SIZE, offset=offset)
            if not forms:
                break

            for form_data in forms:
                try:
                    await Form.nodes.get(form_id=form_data["form_id"])
                except Form.DoesNotExist:
                    await Form(
                        form_id=form_data["form_id"],
                        cik=form_data["cik"],
                        cusip6=form_data["cusip6"],
                        source=form_data["source"],
                        summary=form_data["summary"],
                        names=form_data["names"]
                    ).save()

            total += len(forms)
            offset += BATCH_SIZE

        logger.info(f"Created/Updated {total} Form nodes")

    async def create_section_nodes(self):
        offset = 0
        total = 0

        while True:
            sections = await self.repo.get_sections_for_graph(limit=BATCH_SIZE, offset=offset)
            if not sections:
                break

            for section_data in sections:
                try:
                    await Section.nodes.get(section_id=section_data["section_id"])
                except Section.DoesNotExist:
                    await Section(
                        section_id=section_data["section_id"],
                        item=section_data["item"],
                        name=section_data["name"],
                        form_id=section_data["form_id"],
                        text_embedding=section_data.get("text_embedding")
                    ).save()

            total += len(sections)
            offset += BATCH_SIZE

        logger.info(f"Created/Updated {total} Section nodes")

    async def create_companies_nodes(self):
        offset = 0
        total = 0

        while True:
            companies = await self.repo.get_companies_for_graph(limit=BATCH_SIZE, offset=offset)
            if not companies:
                break

            for company_data in companies:
                try:
                    await Company.nodes.get(cik=company_data["cik"])
                except Company.DoesNotExist:
                    await Company(
                        cik=company_data["cik"],
                        name=company_data["name"],
                        cusip6=company_data["cusip6"],
                        address=company_data["address"]
                    ).save()

            total += len(companies)
            offset += BATCH_SIZE

        logger.info(f"Created/Updated {total} Company nodes")

    async def create_managers_nodes(self):
        offset = 0
        total = 0

        while True:
            managers = await self.repo.get_managers_for_graph(limit=BATCH_SIZE, offset=offset)
            if not managers:
                break

            for manager_data in managers:
                try:
                    await Manager.nodes.get(manager_cik=manager_data["manager_cik"])
                except Manager.DoesNotExist:
                    await Manager(
                        manager_cik=manager_data["manager_cik"],
                        name=manager_data["name"],
                        address=manager_data["address"]
                    ).save()

            total += len(managers)
            offset += BATCH_SIZE

        logger.info(f"Created/Updated {total} Manager nodes")

    async def create_all_nodes(self):
        await self.create_form_nodes()
        await self.create_section_nodes()
        await self.create_chunk_nodes()
        await self.create_companies_nodes()
        await self.create_managers_nodes()


    async def build_chunks_topology(self):
        pass

    async def build_companies_topology(self):
        pass

    async def build_holdings_topology(self):
        pass

    async def build_all_relationships(self):
        await self.build_chunks_topology()
        await self.build_companies_topology()
        await self.build_holdings_topology()


    async def index_chunks_nodes(self):
        pass

    async def index_form_nodes(self):
        pass

    async def index_all_nodes(self):
        await self.index_chunks_nodes()
        await self.index_form_nodes()


    async def final_build(self):
        await self.create_all_nodes()
        await self.build_all_relationships()
        await self.index_all_nodes()