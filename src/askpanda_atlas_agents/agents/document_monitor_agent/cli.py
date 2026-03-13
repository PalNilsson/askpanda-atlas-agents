"""CLI entrypoint for document_monitor_agent."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from typing import Optional

from .agent import DocumentMonitorAgent
from .agent.embedder_langchain_hf import LangchainHuggingFaceAdapter

LOG = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    p = argparse.ArgumentParser(prog="askpanda-document-monitor-agent")
    p.add_argument("--dir", "-d", required=True, help="Directory to monitor (e.g. ./documents)")
    p.add_argument("--poll-interval", type=int, default=10, help="Poll interval seconds")
    p.add_argument("--chroma-dir", default=".chromadb", help="ChromaDB persist directory")
    p.add_argument("--checkpoint-file", default=".document_monitor/checkpoints.json", help="Checkpoint file path")
    p.add_argument("--chunk-size", type=int, default=1000, help="Chunk size in characters")
    p.add_argument("--chunk-overlap", type=int, default=200, help="Chunk overlap in characters")
    return p


def main(argv: Optional[list[str]] = None) -> None:
    """Run the agent CLI.

    Args:
        argv: Optional argument list (defaults to sys.argv[1:]).
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    embedder = LangchainHuggingFaceAdapter(model_name="all-MiniLM-L6-v2")
    agent = DocumentMonitorAgent(
        name="document_monitor_agent",
        directory=args.dir,
        poll_interval_sec=args.poll_interval,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        checkpoint_file=args.checkpoint_file,
        chroma_dir=args.chroma_dir,
        embedder=embedder,
    )

    def _handler(_signum, _frame):
        LOG.info("Signal received; attempting graceful shutdown.")
        # Prefer request_stop if available; else call stop()
        try:
            if hasattr(agent, "request_stop"):
                agent.request_stop()
            elif hasattr(agent, "stop"):
                agent.stop()
            else:
                LOG.warning("Agent has no request_stop/stop; nothing to call.")
        except Exception:
            LOG.exception("Error while requesting agent to stop.")

    # Helper to check agent run state robustly across different Agent implementations
    def agent_is_running(obj) -> bool:
        """Return True if the agent appears to be in a running state.

        This function is defensive: it supports several common state shapes:
        - Enum-like object with .name attribute (e.g., AgentState.RUNNING)
        - Instance attribute e.g. obj.RUNNING and obj.state == obj.RUNNING
        - String-ish state where str(state).upper() == "RUNNING"
        """
        state = getattr(obj, "state", None)
        if state is None:
            return False
        # Case 1: state is an Enum-like with .name
        name = getattr(state, "name", None)
        if isinstance(name, str):
            return name.upper() == "RUNNING"
        # Case 2: instance has a RUNNING attribute constant and state equals it
        if hasattr(obj, "RUNNING"):
            try:
                if state == getattr(obj, "RUNNING"):
                    return True
            except Exception:
                pass
        # Case 3: fallback to string comparison of state
        try:
            return str(state).upper().endswith("RUNNING") or str(state).upper() == "RUNNING"
        except Exception:
            return False

    agent.start()
    try:
        # Run until the agent's state becomes non-running (or request_stop() is called)
        while agent_is_running(agent):
            agent.tick()
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt received")
        agent.request_stop()
    finally:
        agent.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
