from __future__ import annotations
import duckdb
from datetime import datetime, timezone
from typing import Optional, Any
import json

class DuckDBStore:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._conn = duckdb.connect(database=path, read_only=False)
        self._init_meta()

    def _init_meta(self) -> None:
        self._conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id TEXT PRIMARY KEY,
            source TEXT,
            fetched_utc TIMESTAMP,
            content_hash TEXT,
            ok BOOLEAN,
            error TEXT
        );
        """)
        try:
            self._conn.execute("INSTALL sqlite_scannable")
        except Exception:
            pass

    def write_table(self, table_name: str, rows: list[dict[str,Any]], overwrite: bool = False):
        if not rows:
            return
        if overwrite:
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (data JSON, updated_utc TIMESTAMP)")
        for r in rows:
            self._conn.execute("INSERT INTO {tn} VALUES (?, ?)".format(tn=table_name), [json.dumps(r, default=str), datetime.now(timezone.utc)])

    def record_snapshot(self, snapshot_id: str, source: str, ok: bool, content_hash: Optional[str] = None, error: Optional[str] = None):
        self._conn.execute(
            "INSERT OR REPLACE INTO snapshots VALUES (?, ?, ?, ?, ?, ?)",
            [snapshot_id, source, datetime.now(timezone.utc), content_hash, ok, error],
        )
