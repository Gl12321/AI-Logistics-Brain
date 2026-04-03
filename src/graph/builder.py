from src.graph import queries
from src.core.logger import setup_logger

logger = setup_logger("GRAPH_BUILDER")


class GraphBuilder:
    def __init__(self, repository, neo4j_client):
        self.repo = repository
        self.neo4j = neo4j_client

    def create_chunk_nodes(self):
        self.neo4j_client.execute(queries.CREATE_CHUNK_CONSTRAINT_QUERY)

        chunks = await self.repo.get_all_chunks_for_graph()

        logger.info("Start creating chunk nodes")
        batch_size = 1000
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            self.neo4j_client.execute(queries.CREATE_CHUNKS_BATCH_QUERY, batch)
        logger.info("Chunk nodes created")


    def final_build(self):
        self.create_chunk_nodes()
