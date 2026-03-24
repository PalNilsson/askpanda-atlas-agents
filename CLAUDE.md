# CLAUDE.md

This file gives Claude (and any other AI assistant) the context needed to work
effectively in this repository.

---

## What this repository is

**AskPanDA-ATLAS Agents** is a collection of Python agents that feed data into
the *AskPanDA-ATLAS* plugin for the Bamboo Toolkit, which supports ATLAS
Experiment computing operations at CERN.

Two agents are production-ready; others are planned:

| Agent | Status | Entry point |
|---|---|---|
| `ingestion-agent` | ✅ Ready | `askpanda-ingestion-agent` |
| `document-monitor-agent` | ✅ Ready | `askpanda-document-monitor-agent` |
| `dast-agent` | 📋 Planned | — |
| `supervisor-agent` | 📋 Planned | — |

---

## Install and setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .          # installs runtime deps + registers CLI entry points
pip install -e ".[dev]"   # adds flake8, pytest, pytest-cov
```

Runtime dependencies (declared in `pyproject.toml [project.dependencies]`):
`duckdb>=0.10`, `requests>=2.31`, `pyyaml>=6.0`, `pandas>=2.0`.

The project uses a `src/` layout — `pip install -e .` must be run before
importing the package or running tests.

---

## Running the agents

```bash
# Ingestion agent — download all queues once and exit:
askpanda-ingestion-agent \
  --config src/askpanda_atlas_agents/resources/config/ingestion-agent.yaml \
  --once

# Ingestion agent — daemon mode (polls every 30 minutes):
askpanda-ingestion-agent \
  --config src/askpanda_atlas_agents/resources/config/ingestion-agent.yaml

# Useful debug flags:
#   --log-level DEBUG
#   --inter-queue-delay 0     # skip the 60s wait between queues
#   --log-file ""             # disable file logging

# Inspect the resulting database:
python scripts/dump_ingestion_db.py --count
python scripts/dump_ingestion_db.py --table jobs --queue BNL --limit 5
python scripts/dump_ingestion_db.py --table jobs --queue BNL --format json | jq '.pandaid'
```

---

## Tests and linting

```bash
pytest                                              # run all 29 tests
pytest --cov=askpanda_atlas_agents --cov-report=term-missing
pytest tests/agents/ingestion_agent/ -v            # ingestion agent tests only
flake8 src tests                                    # must be clean before commit
```

Linting rules (`.flake8`):
- Max line length: **160**
- Ignored: E262, E265, E266, N804, W504, B902, N818
- **E241 (multiple spaces after `:`) is NOT ignored** — do not align dict values

Pre-commit hooks run trailing-whitespace, large-file checks, flake8, and a
circular-import detector.  Run manually with `pre-commit run --all-files`.

---

## Docstring style

All docstrings must use **Google style**.  Every public function, method, and
class requires a docstring.  Scripts (`scripts/`) follow the same convention.

```python
def my_function(x: int, y: str) -> bool:
    """One-line summary.

    Longer description if needed.

    Args:
        x: Description of x.
        y: Description of y.

    Returns:
        Description of the return value.

    Raises:
        ValueError: If x is negative.
    """
```

---

## Repository layout

```
askpanda-atlas-agents/
├─ CLAUDE.md                          ← you are here
├─ README.md                          ← project overview and quick-start
├─ README-ingestion_agent.md          ← ingestion agent full docs
├─ README-document_monitor_agent.md   ← document monitor full docs
├─ HANDOVER-bamboo-sql-tool.md        ← handover notes for the Bamboo SQL tool
├─ pyproject.toml                     ← dependencies, entry points, build config
├─ requirements.txt                   ← flat dep list (mirrors pyproject.toml)
├─ .flake8                            ← linting config
├─ .pre-commit-config.yaml            ← pre-commit hooks
├─ scripts/
│  └─ dump_ingestion_db.py            ← CLI tool to inspect jobs.duckdb
├─ src/askpanda_atlas_agents/
│  ├─ agents/
│  │  ├─ base.py                      ← Agent ABC and lifecycle state machine
│  │  ├─ ingestion_agent/
│  │  │  ├─ agent.py                  ← IngestionAgent, config dataclasses
│  │  │  ├─ bigpanda_jobs_fetcher.py  ← BigPanda download loop + DB writes
│  │  │  └─ cli.py                    ← CLI entry point
│  │  ├─ document_monitor_agent/      ← ChromaDB-backed document watcher
│  │  └─ dummy_agent/                 ← minimal no-op agent (template + tests)
│  └─ common/
│     ├─ panda/
│     │  └─ source.py                 ← file/URL fetch with content hashing
│     └─ storage/
│        ├─ duckdb_store.py           ← low-level DuckDB helpers
│        ├─ schema.py                 ← DDL + apply_schema() + migration
│        └─ schema_annotations.py    ← field descriptions + get_schema_context()
├─ tests/
│  └─ agents/
│     ├─ ingestion_agent/
│     │  ├─ test_bigpanda_jobs_fetcher.py   ← 18 tests
│     │  └─ test_ingestion_agent.py
│     ├─ dummy_agent/test_dummy_agent.py
│     └─ test_base_agent.py                 ← 8 lifecycle tests
└─ src/askpanda_atlas_agents/resources/config/
   └─ ingestion-agent.yaml            ← default agent configuration
```

---

## Ingestion agent — key design decisions

**BigPanda source**: `https://bigpanda.cern.ch/jobs/?computingsite=<QUEUE>&json&hours=1`
Returns jobs active in the last hour for one queue.  Hardcoded queues for now:
`SWT2_CPB`, `BNL` (configured in `ingestion-agent.yaml`).

**Database**: DuckDB file (`jobs.duckdb` by default).  Three data tables:
- `jobs` — one row per PanDA job, upserted on `pandaid`; accumulates history
- `selectionsummary` — facet counts per queue, replaced each cycle
- `errors_by_count` — ranked error frequency per queue, replaced each cycle

**Bulk inserts**: All DB writes use `pandas.DataFrame` + `INSERT … SELECT * FROM df`
rather than `executemany`.  This is ~4000× faster for 10k-row payloads (DuckDB
is a columnar engine optimised for bulk operations, not row-by-row inserts).

**Inter-queue delay**: 60 seconds between queue downloads in daemon mode, to
avoid overloading the server.  Skipped automatically in `--once` mode and
overridable with `--inter-queue-delay 0`.

**Ctrl-C handling**: DuckDB converts `KeyboardInterrupt` into
`RuntimeError("Query interrupted")` during query execution.  The fetcher detects
this via `exc.__context__` and re-raises as `KeyboardInterrupt` so the CLI
shutdown path fires correctly.

**Schema migrations**: `apply_schema()` in `schema.py` runs a migration check
before creating tables.  Currently handles one historical migration: the
`selectionsummary` and `errors_by_count` tables had a single-column `id`
primary key that caused constraint violations when two queues were inserted;
this was fixed to a composite `PRIMARY KEY (id, _queue)`.

---

## Annotated schema for LLM context

`schema_annotations.py` provides plain-English descriptions of every database
column, intended for injection into LLM system prompts:

```python
from askpanda_atlas_agents.common.storage.schema_annotations import get_schema_context

# Returns a multi-line "Table: … column TYPE description" block
context = get_schema_context()                  # all three tables
context = get_schema_context(["jobs"])          # jobs only
```

See `HANDOVER-bamboo-sql-tool.md` for how to use this when building the
Bamboo text-to-SQL tool.

---

## Agent lifecycle

All agents implement the `Agent` ABC from `agents/base.py`:

```python
agent.start()   # → RUNNING; initialises resources
agent.tick()    # → executes one unit of work; raises if not RUNNING
agent.health()  # → HealthReport (state, last tick/success/error timestamps)
agent.stop()    # → STOPPED; releases resources
```

`tick_once()` is an `IngestionAgent`-specific variant that passes `one_shot=True`
to the fetcher, suppressing the inter-queue delay for one-shot CLI invocations.

When adding a new agent:
1. Subclass `Agent` and implement `_start_impl`, `_tick_impl`, `_stop_impl`
2. Add a `cli.py` entry point
3. Register in `pyproject.toml` under `[project.scripts]`
4. Add tests (follow `tests/agents/dummy_agent/` as a template)

---

## Common pitfalls

**`ModuleNotFoundError: askpanda_atlas_agents`** — run `pip install -e .` from
the repository root.

**DuckDB constraint errors after schema changes** — delete `jobs.duckdb` and
let the agent recreate it, or rely on `apply_schema()` which runs migrations
automatically.

**`flake8` E241 errors** — do not align dict values with extra spaces; use a
single space after `:` in all dict literals.

**`time.sleep` mock in tests causes infinite loop** — `_interruptible_sleep`
loops on `time.monotonic()`; mocking `time.sleep` without also mocking
`time.monotonic` causes an infinite loop.  Always mock
`BigPandaJobsFetcher._interruptible_sleep` directly instead.

**`json.dumps` emitting `NaN`/`Infinity`** — DuckDB returns Python `float('nan')`
for null-ish float values.  `json.dumps` emits bare `NaN` which is not valid
JSON and breaks `jq`.  Use `_to_json_safe()` from `dump_ingestion_db.py` to
convert non-finite floats to `None` before serialising.
