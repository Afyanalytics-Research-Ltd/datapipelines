"""The ingestion contract. Every domain runs the same template:

    watermark -> extract -> drift check -> validate -> land(raw)
      -> map -> cast -> nulls -> dedupe -> land(staged) -> commit watermark -> reconcile

Domains override extract() (and optionally validate()); everything else is shared.
Watermark is committed only after a successful load (at-least-once semantics),
so a failed run is simply re-run from the same watermark.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import pandas as pd

from .config import DomainConfig
from .context import IngestResult, RunContext
from .hooks import Hooks
from .stores import FileWatermarkStore, ParquetSink

log = logging.getLogger("ingestion")


class ValidationError(Exception):
    pass


class BaseIngestor(ABC):
    def __init__(self, cfg: DomainConfig, hooks: Hooks | None = None,
                 watermarks: FileWatermarkStore | None = None,
                 sink: ParquetSink | None = None):
        self.cfg = cfg
        self.hooks = hooks or Hooks()
        self.watermarks = watermarks or FileWatermarkStore()
        self.sink = sink or ParquetSink()

    # ---- the part each domain owns -------------------------------------
    @abstractmethod
    def extract(self, ctx: RunContext) -> pd.DataFrame:
        """Return rows newer than ctx.watermark_in (or everything on first run)."""

    def validate(self, df: pd.DataFrame, ctx: RunContext) -> None:
        """Structural checks shared by all domains. Override to add domain rules."""
        required = set(self.cfg.primary_keys)
        if self.cfg.watermark_column:
            required.add(self.cfg.watermark_column)
        missing = required - set(df.columns)
        if missing:
            raise ValidationError(f"{self.cfg.name}: missing required columns {sorted(missing)}")
        if len(df) and df[self.cfg.primary_keys].isna().any().any():
            raise ValidationError(f"{self.cfg.name}: null values in primary key columns")

    # ---- the shared template -------------------------------------------
    def run(self) -> IngestResult:
        cfg, ctx = self.cfg, RunContext(domain=self.cfg.name)
        try:
            ctx.watermark_in = self.watermarks.get(cfg.name)
            df = self.extract(ctx)
            ctx.counts["extracted"] = len(df)
            self.hooks.checkpoint(ctx, "extracted")

            self.hooks.detect_drift(df, cfg, ctx)
            self.validate(df, ctx)

            self.sink.write(cfg.tiers["landing"], cfg.name, ctx.batch_id, df)
            self.hooks.record_lineage(ctx, "landing", len(df), len(df))
            self.hooks.checkpoint(ctx, "landed")

            for step, fn in self.hooks.transform_steps():
                n_in = len(df)
                df = fn(df, cfg, ctx)
                ctx.counts[step] = len(df)
                self.hooks.record_lineage(ctx, step, n_in, len(df))
                self.hooks.checkpoint(ctx, step)

            self.sink.write(cfg.tiers["staged"], cfg.name, ctx.batch_id, df)
            ctx.counts["loaded"] = len(df)

            if cfg.watermark_column and len(df):
                ctx.watermark_out = str(df[cfg.watermark_column].max())
                self.watermarks.set(cfg.name, ctx.watermark_out)

            self.hooks.reconcile(ctx)
            log.info("%s ok run=%s counts=%s", cfg.name, ctx.run_id, ctx.counts)
            return IngestResult(cfg.name, ctx.run_id, "success", ctx.counts, ctx.watermark_out)
        except Exception as exc:  # isolate: one domain's failure must not kill the others
            log.exception("%s failed run=%s", cfg.name, ctx.run_id)
            return IngestResult(cfg.name, ctx.run_id, "failed", ctx.counts, None, str(exc))
