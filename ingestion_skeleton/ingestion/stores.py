"""Minimal watermark store and tier sink. Both are stand-ins: swap for Martin's
watermark/CDC store and Caleb's tier layout by implementing the same methods."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


class FileWatermarkStore:
    def __init__(self, path: str | Path = "state/watermarks.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, domain: str) -> str | None:
        return self._load().get(domain)

    def set(self, domain: str, value: str) -> None:
        data = self._load()
        data[domain] = value
        self.path.write_text(json.dumps(data, indent=2))


class ParquetSink:
    """Writes <root>/<tier>/<domain>/<batch_id>.parquet"""

    def __init__(self, root: str | Path = "data"):
        self.root = Path(root)

    def write(self, tier: str, domain: str, batch_id: str, df: pd.DataFrame) -> Path:
        out = self.root / tier / domain
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{batch_id}.parquet"
        df.to_parquet(path, index=False)
        return path
