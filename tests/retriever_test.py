import asyncio
from src.infrastructure.llm.embedder import Embedder
from src.rag.retriever import Retriever
from src.infrastructure.db.neo4j.neo4j_client import neo4j_client


async def test_retriever():
    embedder = Embedder()
    await neo4j_client.connect()
    
    try:
        retriever = Retriever(embedder)
        results = await retriever.get_form_context_by_vector("Big company for the production of sportswear Nike")
        
        print("=" * 60)
        
        for i, item in enumerate(results):
            node = dict(item["node"])
            rel_type = item["rel_type"]
            labels = item["labels"]
            if labels[0] == "Chunk":
                continue

            node_type = labels[0] if labels else "Unknown"
            
            if "text_embedding" in node:
                del node["text_embedding"]
            
            print(f"\n{node_type} | Connected via: {rel_type}")
            print("-" * 40)
            
            for key in ["form_id", "chunk_id", "section_id", "cik", "cusip6", "item", "names"]:
                if key in node and node[key]:
                    print(f"  {key:12}: {node[key]}")
            
            # Print text preview
            if "text" in node and node["text"]:
                text_preview = node["text"][:200].replace("\n", " ")
                print(f"  {'text':12}: {text_preview}...")
        
        print("\n" + "=" * 60)

    finally:
        await neo4j_client.close()


if __name__ == "__main__":
    asyncio.run(test_retriever())