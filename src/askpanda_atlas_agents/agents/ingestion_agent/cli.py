"""Command-line interface for the ingestion agent."""
from __future__ import annotations
import argparse
import yaml
import sys
from typing import Optional, Sequence
from askpanda_atlas_agents.agents.ingestion_agent.agent import IngestionAgent, IngestionAgentConfig, SourceConfig


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    p = argparse.ArgumentParser(prog='askpanda-ingestion-agent')
    p.add_argument('--config', '-c', default='src/askpanda_atlas_agents/resources/config/ingestion-agent.yaml',
                   help='Path to YAML configuration file')
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for the ingestion agent.

    Args:
        argv: Command-line arguments. If None, uses sys.argv.

    Returns:
        Exit code (0 for success, 2 for error).
    """
    args = build_parser().parse_args(argv)
    with open(args.config, 'r') as fh:
        cfg = yaml.safe_load(fh)
    sources = []
    for s in cfg.get('sources', []):
        sources.append(SourceConfig(**s))
    mac = IngestionAgentConfig(sources=sources, duckdb_path=cfg.get('duckdb_path', ':memory:'), tick_interval_s=cfg.get('tick_interval_s', 1.0))
    agent = IngestionAgent(config=mac)
    try:
        agent.start()
        agent.tick()
        print('tick complete')
        agent.stop()
        return 0
    except Exception as exc:
        print('error', exc, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
