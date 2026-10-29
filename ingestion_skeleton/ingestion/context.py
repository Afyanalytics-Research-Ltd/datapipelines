"""Shared run metadata. Every domain run gets one RunContext; run_id and batch_id
are the join keys for lineage (Caleb) and reconciliation (Martin)."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RunContext:
    domain: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    watermark_in: str | None = None
    watermark_out: str | None = None
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def batch_id(self) -> str:
        return self.run_id[:8]


@dataclass
class IngestResult:
    domain: str
    run_id: str
    status: str  # "success" | "failed" | "skipped"
    counts: dict[str, int] = field(default_factory=dict)
    watermark_out: str | None = None
    error: str | None = None
