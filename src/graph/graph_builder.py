import pandas as pd
import json
import numpy as np

from src.core.config import get_settings
from src.core.logger import setup_logger
from src.infrastructure.db.falkordb_client import falkor_client

logger = setup_logger("FALKOR_INGESTOR")
settings = get_settings()


class FalkorGraphBuilder:
    def __init__(self):
        self.client = falkor_client
        self.graph_name = "movies_knowledge_graph"

    def graph(self):
        return self.client.get_graph(self.graph_name)

    def db_cleanup(self):
        try:
            self.client.get_graph(self.graph_name).delete()
            logger.info("Database cleaned")
        except Exception as e:
            logger.info(f"Graph already deleted or not found {e}")

    def load_movies(self, metadata_file, embeddings_file):
        logger.info("Loading movies with all available attributes")

        movies_df = pd.read_csv(settings.DATA_RAW_DIR / metadata_file, dtype={"tmdbId": str})
        embeddings_df = pd.read_csv(settings.DATA_RAW_DIR / embeddings_file, dtype={"tmdbId": str})

        df = pd.merge(movies_df, embeddings_df, on="tmdbId", how="inner", suffixes=('', '_extra'))

        cols_to_keep = [c for c in df.columns if not c.endswith('_extra')]
        df = df[cols_to_keep]

        df["embedding"] = df["embedding"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        df = df.replace({np.nan: None})

        records = df.to_dict(orient="records")
        graph = self.graph()
        cypher = """
        MERGE (m:Movie {tmdbId: $id})
        SET m += $props,
            m.embedding = vecf32($emb)
        """

        for r in records:
            tmdb_id = int(r.pop("tmdbId"))
            embedding = [float(x) for x in r.pop("embedding")]

            params = {
                "id": tmdb_id,
                "emb": embedding,
                "props": r
            }
            graph.query(cypher, params=params)

        logger.info(f"Loaded {len(records)} movies")
        return len(embedding)

    def load_genres(self, filename):
        df = pd.read_csv(settings.DATA_RAW_DIR / filename).replace({np.nan: None})
        graph = self.graph()
        query = """
        MATCH (m:Movie {tmdbId: $mid})
        MERGE (g:Genre {genre_id: $gid})
        SET g.genre_name = $name
        MERGE (m)-[:HAS_GENRE]->(g)
        """
        for i, r in enumerate(df.to_dict(orient='records'), 1):
            try:
                graph.query(query,
                            params={"mid": int(r["tmdbId"]), "gid": int(r["genre_id"]), "name": str(r["genre_name"])})
                if i % 1000 == 0:
                    logger.info(f"Processed {i} genres")
            except Exception as e:
                logger.error(f"Genre error at ID {r.get('tmdbId')}: {e}")
        logger.info("Loaded genres.")

    def load_cast(self, filename):
        df = pd.read_csv(settings.DATA_RAW_DIR / filename).replace({np.nan: None})
        graph = self.graph()
        query = """
        MATCH (m:Movie {tmdbId: $mid})
        MERGE (p:Person {actor_id: $aid})
        SET p.name = $name, p:Actor
        MERGE (p)-[a:ACTED_IN]->(m)
        SET a.character = $char
        """
        for i, r in enumerate(df.to_dict(orient='records'), 1):
            try:
                graph.query(query, params={"mid": int(r["tmdbId"]), "aid": int(r["actor_id"]), "name": str(r["name"]),
                                           "char": str(r["character"])})
                if i % 1000 == 0:
                    logger.info(f"Processed {i} cast members")
            except Exception as e:
                logger.error(f"Cast error at ID {r.get('tmdbId')}: {e}")
        logger.info("Loaded cast")

    def load_crew(self, filename):
        df = pd.read_csv(settings.DATA_RAW_DIR / filename).replace({np.nan: None})
        graph = self.graph()
        for job, rel in [("Director", "DIRECTED"), ("Producer", "PRODUCED")]:
            job_records = [r for r in df.to_dict(orient='records') if r['job'] == job]
            query = f"""
            MATCH (m:Movie {{tmdbId: $mid}})
            MERGE (p:Person {{crew_id: $cid}})
            SET p.name = $name, p:{job}
            MERGE (p)-[:{rel}]->(m)
            """
            for i, r in enumerate(job_records, 1):
                try:
                    graph.query(query,
                                params={"mid": int(r["tmdbId"]), "cid": int(r["crew_id"]), "name": str(r["name"])})
                    if i % 1000 == 0:
                        logger.info(f"Processed {i} {job} records")
                except Exception as e:
                    logger.error(f"Crew error ({job}) at ID {r.get('tmdbId')}: {e}")
        logger.info("Loaded crew.")

    def fix_embeddings_format(self):
        graph = self.graph()
        logger.info("Converting embeddings using vecf32")
        graph.query("""
        MATCH (m:Movie)
        SET m.embedding = vecf32(m.embedding)
        """)
        logger.info("Embeddings converted")

    def create_vector_index(self, dimension):
        graph = self.graph()
        try:
            graph.query("DROP INDEX ON :Movie(embedding)")
        except:
            pass

        cypher = f"""
        CREATE VECTOR INDEX FOR (m:Movie)
        ON (m.embedding)
        OPTIONS {{
            dimension:{dimension},
            similarityFunction:'cosine'
        }}
        """
        graph.query(cypher)
        logger.info("Vector index created")


def main():
    builder = FalkorGraphBuilder()

    # builder.db_cleanup()
    dim = builder.load_movies("normalized_movies.csv", "movie_embeddings.csv")

    builder.load_genres("normalized_genres.csv")
    builder.load_cast("normalized_cast.csv")
    builder.load_crew("normalized_crew.csv")

    builder.fix_embeddings_format()
    builder.create_vector_index(768)

if __name__ == "__main__":
    main()
