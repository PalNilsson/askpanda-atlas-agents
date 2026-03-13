"""Document monitor agent implementation.

This agent watches a directory (polling by default), processes new documents,
splits them into chunks, computes deterministic IDs, embeds them using a pluggable
embedder, and stores vectors+metadata into ChromaDB.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

from askpanda_atlas_agents.agents.base import Agent
from .utils import (
    extract_text_from_file,
    chunk_text,
    content_hash,
    deterministic_chunk_id,
    CheckpointStore,
)
from .storage import ChromaWrapper

LOG = logging.getLogger(__name__)


class DocumentMonitorAgent(Agent):
    """Agent that monitors a directory and ingests new files into ChromaDB.

    The agent lifecycle integrates with the project's Base Agent: it must
    implement start/tick/stop hooks via the base class (the names used here
    match a thin adapter to your existing base).

    Args:
        name: Agent name.
        directory: Directory to monitor (create if missing).
        poll_interval_sec: Polling interval in seconds.
        chunk_size: Character chunk size.
        chunk_overlap: Chunk overlap in characters.
        checkpoint_file: Path to JSON checkpoint file.
        chroma_dir: Directory for ChromaDB persistence.
        embedder: Object with an .encode(list[str], show_progress_bar=False) -> np.ndarray interface.
                  If None, a default local sentence-transformers embedder will be created lazily.
    """

    def __init__(
        self,
        name: str,
        directory: str,
        poll_interval_sec: int = 10,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        checkpoint_file: str = ".document_monitor/checkpoints.json",
        chroma_dir: str = ".chromadb",
        embedder: Optional[object] = None,
        embedding_model_name: str = "all-MiniLM-L6-v2",
    ) -> None:
        super().__init__(name=name)
        self.directory = Path(directory)
        self.poll_interval_sec = poll_interval_sec
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.checkpoint = CheckpointStore(checkpoint_file)
        self.chroma = ChromaWrapper(persist_directory=chroma_dir)
        self.collection = self.chroma.get_or_create_collection(name)
        self._last_processed_file: Optional[str] = None
        self._last_error: Optional[str] = None
        self._embedder = embedder
        self._embedding_model_name = embedding_model_name

    # ---------------------- embedder ---------------------------------------
    def _ensure_embedder(self) -> None:
        """Ensure an embedder is available; instantiate default if not provided."""
        if self._embedder is not None:
            return
        try:
            # Lazy import to avoid hard requirement in test/mocked environments
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._embedder = SentenceTransformer(self._embedding_model_name)
        except Exception as exc:
            LOG.exception("Failed to create default embedder: %s", exc)
            raise

    # ---------------------- lifecycle hooks --------------------------------
    def _start_impl(self) -> None:
        """Start hook called by base Agent.start().

        Creates the monitored directory if missing and performs any one-time init.
        """
        LOG.info("document_monitor_agent starting. Monitoring: %s", self.directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _tick_impl(self) -> None:
        """Perform one polling cycle: detect new/changed files, ingest chunks into ChromaDB.

        Workflow:
        1. List files (non-recursive) in the monitored directory.
        2. For each file:
           - Extract text.
           - Compute content hash.
           - If previously processed and hash unchanged -> skip.
           - Otherwise chunk text, compute deterministic stable chunk IDs (based on path+index),
             compute embeddings, delete previous chunks (if any), add new chunks to Chroma,
             persist chroma and update checkpoint with new chunk_ids + content_hash.
        3. Sleep for `self.poll_interval_sec` at the end of the cycle.

        The function is resilient: it logs errors per-file and continues processing other files.
        It also records `self._last_processed_file` and `self._last_error` for health reporting.
        """
        try:
            files = sorted([p for p in self.directory.iterdir() if p.is_file()])
        except Exception as exc:  # defensive: directory listing may fail
            LOG.exception("Failed listing directory %s: %s", self.directory, exc)
            self._last_error = str(exc)
            time.sleep(self.poll_interval_sec)
            return

        for p in files:
            path_str = str(p.resolve())
            try:
                # 1) extract text and compute content hash
                text = extract_text_from_file(path_str)
                if not text:
                    LOG.debug("No text extracted from %s; skipping.", path_str)
                    continue
                h = content_hash(text)

                # 2) check checkpoint to decide whether to skip or update
                prev = self.checkpoint._data.get("processed", {}).get(path_str)
                prev_hash = prev.get("content_hash") if prev else None
                prev_chunk_ids = prev.get("chunk_ids", []) if prev else []

                if prev_hash == h:
                    LOG.debug("File unchanged since last processing: %s", path_str)
                    continue  # nothing to do

                # 3) chunk the text
                chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
                if not chunks:
                    LOG.debug("No chunks generated for %s; recording empty checkpoint.", path_str)
                    self.checkpoint.mark_processed(
                        path_str,
                        {
                            "content_hash": h,
                            "processed_ts": datetime.now(timezone.utc).isoformat(),
                            "chunks": 0,
                            "chunk_ids": [],
                        },
                    )
                    self._last_processed_file = path_str
                    self._last_error = None
                    continue

                # 4) deterministic stable IDs (based on path + chunk index only)
                ids: List[str] = [deterministic_chunk_id(path_str, "", i) for i in range(len(chunks))]
                metadatas: List[Dict] = [
                    {
                        "source_file": path_str,
                        "chunk_index": i,
                        "content_hash": h,
                        "processed_ts": datetime.now(timezone.utc).isoformat(),
                    }
                    for i in range(len(chunks))
                ]

                # 5) if previous chunk ids exist, delete them first to avoid stale vectors
                if prev_chunk_ids:
                    try:
                        self.chroma.delete_documents_by_ids(self.collection, prev_chunk_ids)
                        LOG.debug("Deleted %d previous chunk ids for %s", len(prev_chunk_ids), path_str)
                    except Exception:
                        LOG.exception("Failed to delete previous chunk ids for %s (best-effort)", path_str)

                # 6) ensure embedder and compute embeddings
                self._ensure_embedder()
                raw_embeddings = self._embedder.encode(chunks, show_progress_bar=False)

                # normalize embeddings to python lists for chroma
                try:
                    embeddings = raw_embeddings.tolist()  # type: ignore[attr-defined]
                except Exception:
                    embeddings = [list(map(float, v)) for v in raw_embeddings]

                # 7) add to chroma and persist
                self.chroma.add_documents(self.collection, ids=ids, documents=chunks, metadatas=metadatas,
                                          embeddings=embeddings)
                self.chroma.persist()

                # 8) update checkpoint with new chunk ids and metadata
                self.checkpoint.mark_processed(
                    path_str,
                    {
                        "content_hash": h,
                        "processed_ts": datetime.now(timezone.utc).isoformat(),
                        "chunks": len(chunks),
                        "chunk_ids": ids,
                    },
                )

                self._last_processed_file = path_str
                self._last_error = None
                LOG.info("Processed file %s -> chunks=%d", path_str, len(chunks))

            except Exception as exc:
                # per-file failure should not stop the tick loop
                LOG.exception("Error processing file %s: %s", path_str, exc)
                self._last_error = str(exc)
                # continue with next file

        # Sleep for the configured poll interval before the next tick run.
        time.sleep(self.poll_interval_sec)

    def _stop_impl(self) -> None:
        """Stop hook called by base Agent.stop().

        Persist chroma and perform cleanup.
        """
        LOG.info("document_monitor_agent stopping. Persisting Chroma.")
        try:
            self.chroma.persist()
        except Exception:
            LOG.exception("Failed persisting chroma on stop")

    def _health_details(self) -> Dict:
        """Return agent-specific health details for monitoring dashboards.

        Returns:
            Dictionary with last processed file, last error, checkpoint location and collection name.
        """
        return {
            "last_processed_file": self._last_processed_file,
            "last_error": self._last_error,
            "checkpoint_file": str(self.checkpoint.path),
            "chroma_collection": getattr(self.collection, "name", "<unknown>"),
        }
