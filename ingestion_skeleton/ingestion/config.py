"""Per-domain configuration. Adding a domain = one YAML entry (+ optional thin subclass)."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Placeholder tier names: replace with whatever Caleb's tiered layers end up being.
DEFAULT_TIERS = {"landing": "raw", "staged": "staged", "curated": "curated"}


@dataclass(frozen=True)
class DomainConfig:
    name: str
    source_dsn_env: str                # env var holding the source DB DSN (never put secrets in YAML)
    source_table: str
    source_schema_ref: str             # path/ID of source schema definition
    target_schema_ref: str             # path/ID of target schema definition
    primary_keys: list[str]
    ingestor: str = "generic_sql"      # key in the registry
    watermark_column: str | None = None
    foreign_keys: dict[str, str] = field(default_factory=dict)  # "col": "other_domain.col"
    depends_on: list[str] = field(default_factory=list)
    schedule: str = "daily"
    prompt_overrides: dict[str, str] = field(default_factory=dict)  # for the LLM mapping prompt
    tiers: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_TIERS))
    enabled: bool = True


def load_configs(path: str | Path) -> dict[str, DomainConfig]:
    raw = yaml.safe_load(Path(path).read_text())
    configs: dict[str, DomainConfig] = {}
    for name, body in (raw.get("domains") or {}).items():
        cfg = DomainConfig(name=name, **body)
        configs[name] = cfg
    # fail fast on bad dependency references
    for cfg in configs.values():
        for dep in cfg.depends_on:
            if dep not in configs:
                raise ValueError(f"{cfg.name} depends_on unknown domain '{dep}'")
    return configs
