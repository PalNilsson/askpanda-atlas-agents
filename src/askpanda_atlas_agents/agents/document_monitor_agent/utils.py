"""Utilities for the document monitor agent.

This module provides text extraction for common document formats,
chunking, deterministic id generation, and a small JSON checkpoint
store used to avoid re-processing files.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import List, Dict

from pdfminer.high_level import extract_text as pdf_extract_text
import docx


def extract_text_from_file(path: str) -> str:
    """Extract text content from a file.

    Supported extensions: .pdf, .docx, .txt, .md. For unknown extensions
    will attempt a best-effort text decode.

    Args:
        path: Path to the file.

    Returns:
        Extracted text. Empty string on failure or if no text found.
    """
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix == ".pdf":
        return _extract_pdf(path)
    if suffix == ".docx":
        return _extract_docx(path)
    if suffix in (".txt", ".md"):
        try:
            return Path(path).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""
    # Fallback: try reading bytes and decode
    try:
        b = Path(path).read_bytes()
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _extract_pdf(path: str) -> str:
    """Extract text from PDF using pdfminer.six.

    Args:
        path: Path to the PDF file.

    Returns:
        Extracted text or empty string on error.
    """
    try:
        return pdf_extract_text(path) or ""
    except Exception:
        return ""


def _extract_docx(path: str) -> str:
    """Extract text from DOCX using python-docx.

    Args:
        path: Path to the DOCX file.

    Returns:
        Extracted text or empty string on error.
    """
    try:
        doc = docx.Document(path)
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception:
        return ""


def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """Split text into overlapping chunks.

    Args:
        text: The input text.
        chunk_size: Maximum size of each chunk (characters).
        overlap: Number of characters to overlap between successive chunks.

    Returns:
        List of text chunks. Returns empty list if input is empty.
    """
    if not text:
        return []
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")
    chunks: List[str] = []
    start = 0
    L = len(text)
    while start < L:
        end = start + chunk_size
        chunks.append(text[start:end])
        # Move start forward but keep overlap
        start = end - overlap
        if start < 0:
            start = 0
    return chunks


def deterministic_chunk_id(file_path: str, content_hash: str, chunk_index: int) -> str:
    """Create a deterministic ID for a chunk using file path, content hash, and index.

    The returned ID is a SHA256 hex digest of the canonical input, prefixed
    with "doc:" to make stored IDs easily recognizable.

    Args:
        file_path: Original file path (string).
        content_hash: Hex digest of the whole file content (e.g., SHA256).
        chunk_index: Index of the chunk (0-based).

    Returns:
        Deterministic string ID suitable for vector DB primary key.
    """
    base = f"{file_path}|{content_hash}|{chunk_index}"
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return f"doc:{h}"


def content_hash(text: str) -> str:
    """Compute SHA256 hex digest of the provided text.

    Args:
        text: Input text.

    Returns:
        Hex string of SHA256(text).
    """
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()


class CheckpointStore:
    """JSON-backed checkpoint store that records processed files and metadata.

    This lightweight store maps absolute file paths to metadata such as
    content hash and timestamp. It is intentionally minimal so it is easy
    to swap out for DuckDB or another store later.

    Attributes:
        path: Path to the JSON checkpoint file.
        _data: Internal dict containing checkpoint data.
    """

    def __init__(self, path: str) -> None:
        """Initialize the checkpoint store.

        Args:
            path: Filesystem path where the checkpoint JSON will be saved.
        """
        self.path = Path(path)
        self._data: Dict[str, Dict] = {"processed": {}}
        self._load()

    def _load(self) -> None:
        """Load checkpoint file if it exists; otherwise start fresh."""
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {"processed": {}}

    def save(self) -> None:
        """Persist the checkpoint store to disk (atomic-ish)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def mark_processed(self, filename: str, snapshot: Dict) -> None:
        """Mark a file as processed.

        Args:
            filename: Absolute filename.
            snapshot: Metadata dict to store (e.g., content_hash, processed_ts).
        """
        self._data.setdefault("processed", {})[filename] = snapshot
        self.save()

    def is_processed(self, filename: str, content_hash_str: str) -> bool:
        """Check whether a file has been processed with a given content hash.

        If the file exists in the store and the stored content_hash matches
        the provided hash, returns True.

        Args:
            filename: Absolute filename.
            content_hash_str: SHA256 hash of the file content.

        Returns:
            True if processed and hashes match, False otherwise.
        """
        meta = self._data.get("processed", {}).get(filename)
        if not meta:
            return False
        return meta.get("content_hash") == content_hash_str
