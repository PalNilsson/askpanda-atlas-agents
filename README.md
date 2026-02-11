# AskPanDA-ATLAS Agents

**AskPanDA-ATLAS Agents** is a collection of cooperative, Python-based agents that power the *AskPanDA-ATLAS* plugin for the **Bamboo Framework**, supporting the ATLAS Experiment.

> ⚠️ **Status note**
> This repository is a **preliminary architectural plan** and scaffolding for later development.
> Interfaces, agents, and repository layout are intentionally designed to be stable, but implementations will evolve.

---

## Goals

- Provide a modular, testable, and pip-installable agent system for AskPanDA.
- Separate responsibilities across focused agents (metadata, DAST, indexing, metrics, etc.).
- Share common tooling (storage, embeddings, email parsing, metrics).
- Integrate cleanly with the Bamboo Framework via a plugin adapter.
- Support both lightweight local deployments and service-based execution.

---

## Agents Overview

### `metadata-agent`
- Periodically fetches ATLAS queue/site metadata.
- Normalizes and loads data into **DuckDB** for fast local queries.
- Optionally pulls BigPanDA task/job metadata snapshots for debugging or analytics.

### `dast-agent`
- Extracts DAST help-list email threads (e.g., via Outlook).
- Converts threads into structured JSON.
- Runs a daily *digest* pass producing:
  - Cleaned Q/A pairs
  - Thread summary
  - Tags
  - Resolution status
- Feeds RAG corpora and optional fine-tuning datasets.

### `supervisor-agent`
- Acts as a control plane.
- Ensures required agents/services are running.
- Restarts agents on failure.
- Enforces schedules.
- Provides a single entry point to bring up the full system.

### `index-builder-agent`
- Builds embedding indices for plugin corpora.
- Sources include DAST digests, documentation, and curated knowledge.
- Supports pluggable vector stores (e.g., ChromaDB).

### `feedback-agent`
- Captures user feedback from AskPanDA (e.g., *helpful / not helpful*).
- Stores feedback in structured form for later analysis.

### `metrics-agent`
- Collects structured metrics from Bamboo and agents:
  - Latency
  - Tool usage
  - Failures
- Exports metrics to JSON and optionally Grafana / Prometheus-compatible backends.

---

## Minimal Agent Lifecycle Interface

All agents follow a **minimal, consistent lifecycle interface** to simplify supervision, testing, and orchestration.

```python
class Agent:
    def start(self) -> None:
        """Initialize resources and enter running state."""

    def tick(self) -> None:
        """Execute one scheduled unit of work (poll, sync, digest, etc.)."""

    def health(self) -> dict:
        """Return lightweight health/status information."""

    def stop(self) -> None:
        """Gracefully release resources and shut down."""
```

- **Long-running agents** typically run a scheduler loop calling `tick()`.
- **Batch agents** may run `start() → tick() → stop()` once.
- The `supervisor-agent` interacts only through this interface.

---

## Repository Layout

```text
askpanda-atlas-agents/
├─ README.md
├─ pyproject.toml
├─ src/
│  └─ askpanda_atlas_agents/
│     ├─ common/                # shared utilities (storage, panda, email, metrics)
│     ├─ agents/                # individual agents
│     │  ├─ metadata_agent/
│     │  ├─ dast_agent/
│     │  ├─ supervisor_agent/
│     │  ├─ index_builder_agent/
│     │  ├─ feedback_agent/
│     │  └─ metrics_agent/
│     ├─ plugin/                # Bamboo / AskPanDA plugin adapter
│     └─ resources/             # default configs and schemas
│
├─ tests/
│  ├─ common/
│  ├─ agents/
│  └─ plugin/
│
├─ deployments/
│  ├─ docker/
│  ├─ systemd/
│  └─ k8s/
│
└─ .github/
   └─ workflows/
      └─ ci.yml
```

---

## Shared Tooling

Agents may rely on shared components located under `common/`, including:

- **Storage**
  - DuckDB
  - SQLite
  - Filesystem helpers
- **Vector stores**
  - ChromaDB
  - Embedding adapters
- **PanDA / BigPanDA**
  - Metadata fetching
  - Snapshot downloads
- **Email**
  - Local Microsoft Outlook access
  - Thread reconstruction and parsing
- **Metrics**
  - Structured event schemas
  - JSON and Grafana-compatible exporters

---

## Packaging & Installation

- The repository builds a single Python package:
  **`askpanda-atlas-agents`**
- Each agent provides a CLI entry point (e.g. `askpanda-metadata-agent`).
- Optional dependencies are exposed via extras (e.g. `.[email]`, `.[vector]`).

---

## Development & Testing

This project uses a **`src/` layout**, so the package must be installed before running tests or tools.

### Local development setup

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e ".[dev]"
```

This installs the package in **editable mode** and pulls in development dependencies
(pytest, flake8, pylint, etc.).

## Dummy Agent (template)

A minimal no-op agent is included as a template and for validating the agent lifecycle and supervisor integration.

- Package: `askpanda_atlas_agents.agents.dummy_agent`
- CLI: `askpanda-dummy-agent`

Run it locally:

```bash
python3 -m pip install -e ".[dev]"
askpanda-dummy-agent --tick-interval 1.0
```
Stop with Ctrl+C (SIGINT) or by sending SIGTERM.

When a new agent is added, remember to make an entry in pyproject.toml under [project.scripts].

### Running tests

Run the full unit test suite:

```bash
pytest
```

Run tests with coverage:

```bash
pytest --cov=askpanda_atlas_agents --cov-report=term-missing
```

### Linting

```bash
flake8 src tests
pylint src/askpanda_atlas_agents
```

### Common pitfalls

- **`ModuleNotFoundError: askpanda_atlas_agents`**
  - Ensure you ran:
    ```bash
    pip install -e .
    ```
  - Ensure you are in the repository root (where `pyproject.toml` lives).

- **Editable install fails**
  - Confirm the directory `src/askpanda_atlas_agents/` exists and contains
    an `__init__.py` file.

### Why editable installs?

Editable installs (`pip install -e .`) are required because:
- the project uses a `src/` layout,
- agents are developed incrementally,
- tests must import the package exactly as it will be installed in production.


---

## Continuous Integration

GitHub Actions are used for:

- **Linting**
  - `pylint`
  - `flake8`
- **Unit tests**
  - `pytest`
- All agents and shared tools must have corresponding unit tests.

---

## Relationship to Bamboo

The `plugin/` package provides the integration layer between:
- AskPanDA-ATLAS Agents
- The Bamboo Framework

This keeps agent logic independent of the UI or orchestration framework.

---

## Next Steps (Planned)

- Implement the supervisor loop.
- Add concrete metadata normalization and DuckDB schemas.
- Prototype DAST email digestion on a limited dataset.
- Define stable plugin contracts with Bamboo.

---

## Disclaimer

This repository currently represents an **architectural blueprint** and initial scaffolding.
It is intended to guide development and review before full implementation.

Contributions and design feedback are welcome.
