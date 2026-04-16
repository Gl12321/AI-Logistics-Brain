from typing import List, Dict

from src.core.config import get_settings
from src.core.logger import setup_logger
from src.infrastructure.db.neo4j.neo4j_client import neo4j_client

settings = get_settings()
logger = setup_logger("RETRIEVER")


class Retriever:
    def __init__(self, embedder):
        self.neo4j = neo4j_client
        self.embedder = embedder

    async def vector_search(self, query: str, top_k: int = 3) -> List[Dict]:
        query_embedding = await self.embedder.get_embedding(query)

        async with await self.neo4j.get_session() as session:
            result = await session.run("""
                CALL db.index.vector.queryNodes('form_embedding_index', $k, $embedding)
                YIELD node, score
                RETURN node, score, 'Form' as node_type
                UNION ALL
                CALL db.index.vector.queryNodes('section_embedding_index', $k, $embedding)
                YIELD node, score
                RETURN node, score, 'Section' as node_type
                UNION ALL
                CALL db.index.vector.queryNodes('chunk_embedding_index', $k, $embedding)
                YIELD node, score
                RETURN node, score, 'Chunk' as node_type
            """, {"k": top_k, "embedding": query_embedding})

            records = []
            async for res in result:
                records.append({"node": res["node"], "score": res["score"]})

            records.sort(key=lambda x: x["score"], reverse=True)
            return [r["node"] for r in records]

    async def get_form_context_by_vector(self, query: str, steps: int = 1) -> List[Dict]:
        query_embedding = await self.embedder.get_embedding(query)

        cypher_query = f"""
            CALL db.index.vector.queryNodes('form_embedding_index', 1, $embedding)
            YIELD node, score
            WITH node as f
            MATCH (f)-[r*1..{steps}]-(n)
            RETURN DISTINCT n, type(r[-1]) as rel_type, labels(n) as labels
        """

        async with await self.neo4j.get_session() as session:
            result = await session.run(cypher_query, {"embedding": query_embedding})

            nodes = []
            async for res in result:
                nodes.append({"node": res["n"], "rel_type": res["rel_type"], "labels": res["labels"]})
            return nodes
