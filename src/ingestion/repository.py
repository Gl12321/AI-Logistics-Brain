import json
from typing import List, Dict, Any

class IngestionRepository:
    def __init__(self, db_client):
        self.db = db_client
        self.schema = "staging"

    async def init_schema(self):
        queries = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema};",
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.form10k_chunks (
                chunk_id TEXT PRIMARY KEY,
                form_id TEXT,
                cik TEXT,
                item_type TEXT,
                chunk_seq_id INT,
                chunk_text TEXT,
                names TEXT[],
                cusip6 CHAR(6),
                source TEXT
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.form13 (
                id SERIAL PRIMARY KEY,
                source TEXT,
                manager_cik TEXT,
                manager_name TEXT,
                manager_address TEXT,
                report_calendar_or_quarter TEXT,
                cusip6 CHAR(6),
                cusip TEXT,
                company_name TEXT,
                value_usd FLOAT8,
                shares_amount BIGINT
            );
            """,
            f"CREATE INDEX IF NOT EXISTS idx_f10k_cik ON {self.schema}.form10k_chunks(cik);",
            f"CREATE INDEX IF NOT EXISTS idx_f13_manager_cik ON {self.schema}.form13(manager_cik);",
            f"CREATE INDEX IF NOT EXISTS idx_f13_cusip6 ON {self.schema}.form13(cusip6);"
        ]
        for query in queries:
            await self.db.execute(query)

    async def save_10k_chunks(self, chunks: List[Dict[str, Any]]):
        sql = f"""
            INSERT INTO {self.schema}.form10k_chunks
            (chunk_id, form_id, cik, item_type, chunk_seq_id, chunk_text, names, cusip6, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chunk_id) DO NOTHING;
        """
        values = [
            (
                c['chunkId'], c['formId'], c['cik'],
                c['item'], c['chunkSeqId'], c['text'],
                c['names'], c['cusip6'], c['source']
            )
            for c in chunks
        ]
        await self.db.executemany(sql, values)

    async def save_13f_holdings(self, holdings: List[Dict[str, Any]]):
        pass