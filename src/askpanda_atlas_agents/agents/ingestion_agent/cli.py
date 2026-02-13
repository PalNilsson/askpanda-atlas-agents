from __future__ import annotations
import argparse
import yaml
import sys
from askpanda_atlas_agents.agents.ingestion_agent.agent import IngestionAgent, IngestionAgentConfig, SourceConfig
def build_parser():
    p = argparse.ArgumentParser(prog='askpanda-ingestion-agent')
    p.add_argument('--config', '-c', default='src/askpanda_atlas_agents/resources/config/ingestion-agent.yaml')
    return p

def main(argv=None):
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
