"""Hook points where teammates plug in their pieces. Every hook defaults to a
no-op / pass-through so the skeleton runs before anyone's piece exists.

    map_schema      -> Antony & Jordan (single configurable LLM prompt)
    coerce_types    -> Mercy
    handle_nulls    -> Mercy
    dedupe          -> Seka
    detect_drift    -> Jordan
    checkpoint      -> Marylene
    record_lineage  -> Caleb
    reconcile       -> Martin
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from .config import DomainConfig
from .context import RunContext

Transform = Callable[[pd.DataFrame, DomainConfig, RunContext], pd.DataFrame]


def _identity(df, cfg, ctx):
    return df


@dataclass
class Hooks:
    map_schema: Transform = _identity
    coerce_types: Transform = _identity
    handle_nulls: Transform = _identity
    dedupe: Transform = _identity
    detect_drift: Callable[[pd.DataFrame, DomainConfig, RunContext], None] = lambda df, cfg, ctx: None
    checkpoint: Callable[[RunContext, str], None] = lambda ctx, stage: None
    record_lineage: Callable[[RunContext, str, int, int], None] = lambda ctx, step, n_in, n_out: None
    reconcile: Callable[[RunContext], None] = lambda ctx: None

    def transform_steps(self) -> list[tuple[str, Transform]]:
        # Order matters: map -> cast -> nulls -> dedupe (dedupe last so it sees clean values)
        return [
            ("map_schema", self.map_schema),
            ("coerce_types", self.coerce_types),
            ("handle_nulls", self.handle_nulls),
            ("dedupe", self.dedupe),
        ]
