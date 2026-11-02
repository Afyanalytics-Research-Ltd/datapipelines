"""Config-only domains: most domains need nothing beyond this."""
from __future__ import annotations

import os
import re

import pandas as pd
from sqlalchemy import create_engine, text

from ..base import BaseIngestor
from ..context import RunContext
from ..registry import register

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _safe_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier in config: {name!r}")
    return name


@register("generic_sql")
class GenericSqlIngestor(BaseIngestor):
    def extract(self, ctx: RunContext) -> pd.DataFrame:
        cfg = self.cfg
        engine = create_engine(os.environ[cfg.source_dsn_env])
        sql = f"SELECT * FROM {_safe_ident(cfg.source_table)}"
        params: dict = {}
        if cfg.watermark_column:
            col = _safe_ident(cfg.watermark_column)
            if ctx.watermark_in is not None:
                sql += f" WHERE {col} > :wm"
                params["wm"] = ctx.watermark_in
            sql += f" ORDER BY {col}"
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
