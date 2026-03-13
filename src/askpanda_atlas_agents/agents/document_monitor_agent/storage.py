"""ChromaDB wrapper utilities (robust across Chroma versions)."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import chromadb
from chromadb.config import Settings
from chromadb.api import Collection

LOG = logging.getLogger(__name__)


class ChromaWrapper:
    """Small wrapper around chromadb.Client to centralize creation and persistence.

    This wrapper attempts to use the Settings-based client construction (recommended).
    If the installed chromadb package refuses that configuration (legacy vs new API),
    it falls back to a simpler client() call (best-effort). When falling back, persistent
    storage behavior may differ depending on the installed chromadb version.
    """

    def __init__(self, persist_directory: str = ".chromadb", settings_kwargs: Optional[Dict] = None) -> None:
        """Initialize the Chroma client.

        Args:
            persist_directory: Local directory where Chroma will persist data.
            settings_kwargs: Optional extra kwargs forwarded to Settings.
        """
        settings_kwargs = settings_kwargs or {}
        try:
            # Preferred: explicit, newer Settings-based construction
            settings = Settings(chroma_db_impl="duckdb+parquet", persist_directory=persist_directory, **settings_kwargs)
            self.client = chromadb.Client(settings=settings)
            LOG.info("Created chromadb.Client using Settings (persist_directory=%s)", persist_directory)
        except Exception as exc:
            # If the installed chromadb version rejects the Settings shape, fall back.
            LOG.warning(
                "Failed to create chromadb.Client with Settings (falling back to chromadb.Client()). "
                "This may mean you have a different chromadb release that requires migration. Error: %s", exc
            )
            try:
                # Best-effort fallback: plain client constructor (version-dependent behavior)
                self.client = chromadb.Client()
                LOG.info("Created chromadb.Client using fallback no-arg constructor")
            except Exception as exc2:
                LOG.exception("Failed to create fallback chromadb.Client() - chroma is not usable: %s", exc2)
                raise

    def get_or_create_collection(self, name: str) -> Collection:
        """Get or create a collection.

        Args:
            name: Collection name.

        Returns:
            chromadb.api.Collection instance
        """
        return self.client.get_or_create_collection(name)

    def add_documents(
        self,
        collection: Collection,
        ids: List[str],
        documents: List[str],
        metadatas: List[Dict],
        embeddings: Optional[List[List[float]]] = None,
    ) -> None:
        """Add documents to the provided collection.

        Args:
            collection: Chromadb collection instance.
            ids: List of deterministic IDs.
            documents: List of document text bodies.
            metadatas: List of metadata dictionaries.
            embeddings: Optional list of embeddings (if provided, they must align).
        """
        if embeddings is None:
            collection.add(ids=ids, documents=documents, metadatas=metadatas)
        else:
            collection.add(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)

    def delete_documents_by_ids(self, collection: Collection, ids: List[str]) -> None:
        """Delete documents from a collection by their ids (best-effort).

        Different chromadb releases expose different APIs for deletion; attempt
        the common `collection.delete(ids=...)` and fall back gracefully.
        """
        if not ids:
            return
        try:
            # modern API: collection.delete(ids=[...]) or collection.delete(ids=ids)
            collection.delete(ids=ids)
            LOG.debug("Deleted %d documents from chroma collection.", len(ids))
            return
        except Exception:
            LOG.debug("collection.delete(ids=...) failed; trying client-level or per-id delete", exc_info=True)

        # Some older/newer versions may provide client.delete_collection or require different calls.
        # Try per-id deletion as a best-effort loop (some clients support it).
        for _id in ids:
            try:
                collection.delete(ids=[_id])
            except Exception:
                # If even this fails, log and continue (do not crash agent)
                LOG.exception("Failed to delete id %s from chroma collection (best-effort)", _id)

    def persist(self) -> None:
        """Persist the client's state to disk, ignoring errors."""
        try:
            # The Client API provides persist() in common versions
            self.client.persist()
        except Exception:
            LOG.debug("Chroma persist() failed or not supported in this client release.", exc_info=True)
            # best-effort; do not raise here
