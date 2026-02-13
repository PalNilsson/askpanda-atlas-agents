from __future__ import annotations
import uuid
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from askpanda_atlas_agents.agents.base import Agent
from askpanda_atlas_agents.common.storage.duckdb_store import DuckDBStore
from askpanda_atlas_agents.common.panda.source import BaseSource, RawSnapshot
from datetime import datetime, timezone

@dataclass
class SourceConfig:
    name: str
    type: str
    mode: str = 'file'  # or 'url'
    path: Optional[str] = None
    url: Optional[str] = None
    interval_s: int = 300

@dataclass
class IngestionAgentConfig:
    sources: List[SourceConfig]
    duckdb_path: str = ':memory:'
    tick_interval_s: float = 1.0

class IngestionAgent(Agent):
    def __init__(self, name: str = 'ingestion-agent', config: Optional[IngestionAgentConfig] = None):
        super().__init__(name=name)
        self.config = config or IngestionAgentConfig(sources=[])
        self.store = None
        self._source_last = {}

    def _start_impl(self) -> None:
        self.store = DuckDBStore(self.config.duckdb_path)

    def _tick_impl(self) -> None:
        now = time.time()
        for s in self.config.sources:
            last = self._source_last.get(s.name, 0)
            if now - last < s.interval_s:
                continue
            try:
                raw = self._fetch_source(s)
                aid = str(uuid.uuid4())
                self.store.record_snapshot(aid, s.name, True, raw.content_hash, None)
                rows = self._normalize(s, raw)
                table = f"{s.name}_history"
                self.store.write_table(table, rows)
                self._source_last[s.name] = now
            except Exception as exc:
                if self.store:
                    self.store.record_snapshot(str(uuid.uuid4()), s.name, False, None, str(exc))

    def _stop_impl(self) -> None:
        self.store = None

    def _fetch_source(self, s: SourceConfig) -> RawSnapshot:
        src = BaseSource()
        if s.mode == 'file' and s.path:
            return src.fetch_from_file(s.path)
        if s.mode == 'url' and s.url:
            return src.fetch_from_url(s.url)
        raise RuntimeError('invalid source config')

    def _normalize(self, s: SourceConfig, raw: RawSnapshot) -> List[Dict[str,Any]]:
        return [{'payload': raw.raw, 'fetched_utc': datetime.now(timezone.utc).isoformat()}]
