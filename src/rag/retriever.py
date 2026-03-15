import numpy as np
from src.infrastructure.db.falkordb_client import falkor_client


class Retriever:
    def __init__(self, graph_name="movies_knowledge_graph"):
        self.graph = falkor_client.get_graph(graph_name)

    def search_by_vector(self, query_vector, limit=5):
        vector = np.array(query_vector, dtype=np.float32).tolist()
        cypher = """
        CALL db.idx.vector.queryNodes(
            'Movie',
            'embedding',
            $k,
            vecf32($vector)
        )
        YIELD node, score
        RETURN
            node.tmdbId AS tmdbId,
            node.title AS title,
            node.original_title AS original_title,
            node.overview AS overview,
            node.runtime AS runtime,
            score
        """
        result = self.graph.query(cypher, params={"k": int(limit), "vector": vector})

        movies = []
        for row in result.result_set:
            movies.append({
                "tmdbId": row[0],
                "title": row[1] or row[2],
                "original_title": row[2],
                "overview": row[3],
                "runtime": row[4],
                "score": float(row[5])
            })
        return movies

    def get_all_connected_data(self, tmdb_id):
        cypher = """
        MATCH (m:Movie {tmdbId: $id})-[r]-(target)
        RETURN 
            type(r) AS rel_type,
            labels(target)[0] AS node_type,
            properties(target) AS node_data
        """
        result = self.graph.query(cypher, params={"id": int(tmdb_id)})

        connections = []
        for row in result.result_set:
            connections.append({
                "relationship": row[0],
                "type": row[1],
                "data": row[2]
            })
        return connections

    def find_recommendations_by_graph(self, tmdb_id, query_vector, depth=2, limit=5):
        vector = [float(x) for x in query_vector]

        cypher = f"""
        MATCH (m:Movie {{tmdbId: $id}})-[*1..{depth}]-(rec:Movie)
        WHERE m <> rec
        WITH DISTINCT rec
        RETURN 
            rec.tmdbId AS tmdbId,
            rec.original_title AS title,
            rec.overview AS overview,
            (2 - vec.cosineDistance(rec.embedding, vecf32($vector))) / 2 AS score
        ORDER BY score DESC
        LIMIT $limit
        """

        params = {
            "id": int(tmdb_id),
            "vector": vector,
            "limit": int(limit)
        }

        result = self.graph.query(cypher, params=params)

        recs = []
        for row in result.result_set:
            recs.append({
                "tmdbId": row[0],
                "title": row[1],
                "overview": row[2],
                "score": float(row[3])
            })
        return recs

    def hybrid_search(self, query_vector, vector_limit=1, graph_limit=5):
        seed_movies = self.search_by_vector(query_vector, limit=vector_limit)
        if not seed_movies:
            return None

        seed = seed_movies[0]
        seed_id = seed['tmdbId']

        context = self.get_all_connected_data(seed_id)

        recommendations = self.find_recommendations_by_graph(seed_id, query_vector, limit=graph_limit)

        return {
            "seed_movie": seed,
            "direct_connections": context,
            "graph_recommendations": recommendations
        }