"""Runs enabled domains in dependency order. Failures are isolated: a failed domain
only causes its *dependents* to be skipped; everything else still runs."""
from __future__ import annotations

import argparse
import logging
from graphlib import TopologicalSorter

from .config import load_configs
from .context import IngestResult
from .hooks import Hooks
from .registry import discover_domains, get_ingestor_cls


def run_all(config_path: str, only: list[str] | None = None, hooks: Hooks | None = None) -> list[IngestResult]:
    discover_domains()
    configs = load_configs(config_path)
    graph = {n: set(c.depends_on) for n, c in configs.items()}
    results: dict[str, IngestResult] = {}

    for name in TopologicalSorter(graph).static_order():
        cfg = configs[name]
        if not cfg.enabled or (only and name not in only):
            continue
        blocked = [d for d in cfg.depends_on if d in results and results[d].status != "success"]
        if blocked:
            results[name] = IngestResult(name, "-", "skipped", error=f"upstream failed: {blocked}")
            continue
        ingestor = get_ingestor_cls(cfg.ingestor)(cfg, hooks=hooks)
        results[name] = ingestor.run()
    return list(results.values())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="domains.yaml")
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()
    for r in run_all(args.config, args.only):
        print(f"{r.domain:<15} {r.status:<8} {r.counts} {r.error or ''}")
