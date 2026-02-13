"""Ingestion agent for fetching and normalizing PanDA data sources."""
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
    """Configuration for a single data source.

    Attributes:
        name: Unique identifier for this source.
        type: Type of source (e.g., 'cric', 'bigpanda').
        mode: Fetch mode - 'file' for local files or 'url' for HTTP/HTTPS.
        path: File system path (used when mode='file').
        url: Remote URL (used when mode='url').
        interval_s: Minimum seconds between fetches for this source.
    """
    name: str
    type: str
    mode: str = 'file'  # or 'url'
    path: Optional[str] = None
    url: Optional[str] = None
    interval_s: int = 300


@dataclass
class IngestionAgentConfig:
    """Configuration for the IngestionAgent.

    Attributes:
        sources: List of data sources to ingest.
        duckdb_path: Path to DuckDB database file or ':memory:'.
        tick_interval_s: Seconds to sleep between tick() calls.
    """
    sources: List[SourceConfig]
    duckdb_path: str = ':memory:'
    tick_interval_s: float = 1.0

class IngestionAgent(Agent):
    """Agent for periodic ingestion of PanDA data sources.

    The ingestion agent fetches data from configured sources (files or URLs),
    normalizes the data, and stores it in a DuckDB database. It tracks fetch
    intervals to avoid redundant fetches and records metadata for each snapshot.
    """

    def __init__(self, name: str = 'ingestion-agent', config: Optional[IngestionAgentConfig] = None) -> None:
        """Initialize the ingestion agent.

        Args:
            name: Agent name (default: 'ingestion-agent').
            config: Optional IngestionAgentConfig. If not provided, uses
                default configuration with no sources.
        """
        super().__init__(name=name)
        self.config = config or IngestionAgentConfig(sources=[])
        self.store = None
        self._source_last = {}

    def _start_impl(self) -> None:
        """Initialize the DuckDB store."""
        self.store = DuckDBStore(self.config.duckdb_path)

    def _tick_impl(self) -> None:
        """Fetch and ingest data from all configured sources.

        For each source, checks if the interval has elapsed since the last
        fetch. If so, fetches the data, records a snapshot, normalizes the
        data, and stores it in a history table.
        """
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
        """Release the DuckDB store."""
        self.store = None

    def _fetch_source(self, s: SourceConfig) -> RawSnapshot:
        """Fetch data from a single source.

        Args:
            s: Source configuration.

        Returns:
            RawSnapshot containing the fetched data.

        Raises:
            RuntimeError: If the source configuration is invalid.
        """
        src = BaseSource()
        if s.mode == 'file' and s.path:
            return src.fetch_from_file(s.path)
        if s.mode == 'url' and s.url:
            return src.fetch_from_url(s.url)
        raise RuntimeError('invalid source config')

    def _normalize(self, s: SourceConfig, raw: RawSnapshot) -> List[Dict[str, Any]]:
        """Normalize raw snapshot data into structured rows.

        Args:
            s: Source configuration.
            raw: Raw snapshot to normalize.

        Returns:
            List of normalized data dictionaries.
        """
        return [{'payload': raw.raw, 'fetched_utc': datetime.now(timezone.utc).isoformat()}]
