# document_monitor_agent

This document describes the `document_monitor_agent` --- a
production-oriented agent that watches a directory for new or changed
documents, extracts and chunks text, computes deterministic chunk IDs,
embeds chunks, and stores vectors + metadata in a local ChromaDB
collection.

------------------------------------------------------------------------

## What it does

-   Monitors a directory (non-recursive) for files (polling-based).
-   Extracts text from `.pdf`, `.docx`, `.txt`, `.md` files.
-   Splits text into overlapping character chunks.
-   Generates deterministic chunk IDs (stable across re-ingestion).
-   Embeds chunks using a pluggable embedder (default:
    `sentence-transformers`).
-   Stores vectors and metadata in ChromaDB (`duckdb+parquet` backend).
-   Maintains a JSON checkpoint store to prevent re-processing unchanged
    files.
-   Replaces old vectors when file content changes to prevent stale RAG
    context.

------------------------------------------------------------------------

## Design guarantees

### Deterministic IDs

Chunk IDs are derived from:

    absolute_file_path + chunk_index

They are hashed (SHA256) and prefixed with `doc:`.

This ensures:

-   Stable IDs across re-ingestion.
-   Replace-in-place behavior when content changes.

### Replace-on-change strategy

When a file's content hash changes:

1.  Previous chunk IDs (stored in checkpoint) are deleted from Chroma.
2.  New chunks and embeddings are computed.
3.  New vectors are inserted under the same stable ID scheme.
4.  Checkpoint is updated.

This prevents stale vectors from being retrieved by RAG and reduces
hallucination risk.

------------------------------------------------------------------------

## Installation

Add dependencies:

    chromadb>=0.4.0
    sentence-transformers>=2.2.2
    pdfminer.six>=20221105
    python-docx>=0.8.11

Install:

    pip install -r requirements.txt
    pip install -e .

------------------------------------------------------------------------

## Running the agent

Example CLI:

    askpanda-document-monitor-agent --dir ./documents --poll-interval 10 --chroma-dir .chromadb

Or:

    python -m askpanda_atlas_agents.agents.document_monitor_agent.cli --dir ./documents

------------------------------------------------------------------------

## Configuration options

  -----------------------------------------------------------------------------------
  Option                Default                              Meaning
  --------------------- ------------------------------------ ------------------------
  `--dir`               required                             Directory to monitor

  `--poll-interval`     10                                   Poll interval (seconds)

  `--chroma-dir`        .chromadb                            Chroma persistence
                                                             directory

  `--checkpoint-file`   .document_monitor/checkpoints.json   JSON checkpoint path

  `--chunk-size`        1000                                 Characters per chunk

  `--chunk-overlap`     200                                  Overlap between chunks
  -----------------------------------------------------------------------------------

------------------------------------------------------------------------

## Checkpoint format

Example:

``` json
{
  "processed": {
    "/abs/path/to/file.pdf": {
      "content_hash": "sha256...",
      "processed_ts": "2026-03-12T12:34:56Z",
      "chunks": 5,
      "chunk_ids": ["doc:...", "doc:..."]
    }
  }
}
```

------------------------------------------------------------------------

## CI and testing

Use a dummy embedder in tests to avoid model downloads:

``` python
class DummyEmbedder:
    def encode(self, texts, show_progress_bar=False):
        return [[0.0] * 8 for _ in texts]
```

------------------------------------------------------------------------

# Why Conda is Recommended

This agent frequently depends on machine learning libraries such as:

- sentence-transformers
- transformers
- torch
- scikit-learn
- numpy

These packages contain compiled native extensions and can cause dependency
conflicts when installed with pip alone.

Typical issues:

- PyTorch wheels missing for a Python version
- NumPy binary incompatibilities
- SciPy / sklearn ABI conflicts
- Apple Silicon architecture differences

Conda distributes precompiled binaries that are tested together, which makes
the environment much more stable.

For this reason **Conda is strongly recommended for running this agent.**

---

# Conda Setup

## Apple Silicon (M1 / M2 / M3)

Install Miniforge:

https://github.com/conda-forge/miniforge

Then:

conda create -n askpanda python=3.10 -y
conda activate askpanda

conda install -c conda-forge -c pytorch pytorch cpuonly -y

pip install sentence-transformers langchain langchain-community chromadb pdfminer.six python-docx

---

## Intel macOS

conda create -n askpanda python=3.10 -y
conda activate askpanda

conda install -c pytorch -c conda-forge pytorch -y

pip install sentence-transformers langchain langchain-community chromadb pdfminer.six python-docx

---

# Running the Agent

conda activate askpanda
askpanda-document-monitor-agent --dir ./documents --poll-interval 10

---

# Virtualenv vs Conda

Only use one environment manager at a time.

If a virtualenv is active:

deactivate

Then activate Conda:

conda activate askpanda

Your virtualenv will remain on disk but inactive.