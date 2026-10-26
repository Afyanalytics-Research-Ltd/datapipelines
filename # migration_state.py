# migration_state.py
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

# Default root: same directory as the migration scripts, overridable by env
_DEFAULT_ROOT = Path(os.getenv("MIGRATION_STATE_ROOT") or Path(__file__).resolve().parent)
STATE_ROOT = _DEFAULT_ROOT / ".migration_state"


@dataclass
class StatePaths:
    """Immutable paths for one facility (or the shared root).

    Do not mutate these after creation. Create a new StatePaths per facility.
    """
    root: Path
    watermark: Path
    progress: Path
    record_progress: Path
    id_map: Path
    visit_patient: Path
    visit_admission: Path
    done: Path
    dead_letter: Path
    pii_corruption: Path

    @classmethod
    def for_facility(cls, facility: str | None = None, base: Path | None = None) -> "StatePaths":
        base = base or _DEFAULT_ROOT
        if facility:
            d = (base / ".migration_state" / facility)
        else:
            d = base
        d.mkdir(parents=True, exist_ok=True)
        return cls(
            root=d,
            watermark=base / ".migration_watermarks.json",  # shared across facilities
            progress=d / ".migration_progress.json",
            record_progress=d / ".migration_record_progress.json",
            id_map=d / ".migration_id_map.json",
            visit_patient=d / ".migration_visit_patient.json",
            visit_admission=d / ".migration_visit_admission.json",
            done=d / ".migration_done.json",
            dead_letter=base / ".migration_failures.jsonl",  # shared append-only
            pii_corruption=base / ".migration_pii_corruption.jsonl",
        )


# Thread-local "current" paths so existing call sites can keep working
# while you migrate them one by one.
_tls = threading.local()


def get_paths() -> StatePaths:
    p = getattr(_tls, "paths", None)
    if p is None:
        p = StatePaths.for_facility(None)
        _tls.paths = p
    return p


def use_state_dir(facility: str | None) -> Path:
    """Set the current facility's paths for this thread. Returns the state dir."""
    paths = StatePaths.for_facility(facility)
    _tls.paths = paths
    return paths.root