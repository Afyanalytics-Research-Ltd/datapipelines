# migration_state.py
"""
Where migration state lives, and who is allowed to write it.

Layout
  MIGRATION_ENV unset (legacy, unchanged):
      <root>/.migration_state/<facility>/...      per-facility state
      <root>/.migration_watermarks.json ...       shared files
  MIGRATION_ENV=prod|staging|local:
      <root>/.migration_state/<env>/<facility>/...
      <root>/.migration_state/<env>/.migration_watermarks.json ...

Set MIGRATION_STATE_ROOT to a directory OUTSIDE the repo (e.g. /var/lib/migration)
and give the server and every laptop a different MIGRATION_ENV, so a deploy,
rsync or git pull can never replace one machine's state with another's.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os
import re
import socket
import threading
import time
import json
import contextlib

_DEFAULT_ROOT = Path(os.getenv("MIGRATION_STATE_ROOT") or Path(__file__).resolve().parent)
MIGRATION_ENV = (os.getenv("MIGRATION_ENV") or "").strip()          # "local" | "staging" | "prod" | ""
STATE_ROOT = _DEFAULT_ROOT / ".migration_state"

_SAFE = re.compile(r"[^A-Za-z0-9_.-]")

def _safe(name: str) -> str:
    s = _SAFE.sub("_", name).strip(".")
    if not s:
        raise ValueError(f"Invalid state name: {name!r}")
    return s


@dataclass(frozen=True)
class StatePaths:
    """
    Immutable paths for one (env, facility, pipeline).

    Layout when MIGRATION_ENV is set (recommended):
        <root>/.migration_state/<env>/<facility>/<pipeline>/...

    Layout when MIGRATION_ENV is empty (legacy):
        <root>/.migration_state/<facility>/...
    """
    root: Path                 # the pipeline directory
    env: str
    facility: str | None
    pipeline: str

    # Core shared files (still useful)
    watermark: Path            # usually shared at env level
    lock: Path

    def file(self, name: str, ext: str = "json") -> Path:
        """Dynamic file under this pipeline's directory."""
        name = name.lstrip(".").removeprefix("migration_")
        return self.root / f".migration_{name}.{ext}"

    # Convenience aliases (optional – you can delete them later)
    @property
    def progress(self) -> Path:
        return self.file("progress")

    @property
    def record_progress(self) -> Path:
        return self.file("record_progress")

    @property
    def id_map(self) -> Path:
        return self.file("id_map")

    @property
    def done(self) -> Path:
        return self.file("done")

    @property
    def dead_letter(self) -> Path:
        return self.file("dead_letter", "jsonl")

    @classmethod
    def create(
        cls,
        facility: str | None = None,
        pipeline: str = "default",
        *,
        base: Path | None = None,
        env: str | None = None,
    ) -> "StatePaths":
        base = Path(base) if base else _DEFAULT_ROOT
        env = (env if env is not None else MIGRATION_ENV).strip()
        pipeline = _safe(pipeline)
        facility = _safe(facility) if facility else None

        if env:
            # Recommended layout → completely isolates local / staging / prod
            env_root = base / ".migration_state" / _safe(env)
            facility_root = env_root / facility if facility else env_root
            pipeline_root = facility_root / pipeline
        else:
            # Legacy layout
            facility_root = base / ".migration_state" / facility if facility else base
            pipeline_root = facility_root
            env_root = base

        pipeline_root.mkdir(parents=True, exist_ok=True)

        return cls(
            root=pipeline_root,
            env=env or "legacy",
            facility=facility,
            pipeline=pipeline,
            watermark=env_root / ".migration_watermarks.json",   # shared per env
            lock=pipeline_root / ".migration_run.lock",
        )

# ── "current" paths ──────────────────────────────────────────────────────────
# Lookup order: this thread's paths -> the process-wide paths -> unscoped default.
# The process-wide step matters: worker threads (record POSTs, page fetches)
# never call use_state_dir themselves, and used to fall back to the unscoped
# default, i.e. the repo root, instead of the facility being migrated.

# ── thread / process current paths ──────────────────────────────────────────
_tls = threading.local()
_process_paths: StatePaths | None = None


def get_paths() -> StatePaths:
    p = getattr(_tls, "paths", None)
    if p is not None:
        return p
    if _process_paths is not None:
        return _process_paths
    return StatePaths.create()          # unscoped fallback


def use_state_dir(
    facility: str | None,
    pipeline: str = "default",
    *,
    process_wide: bool = False,
) -> Path:
    """Point this thread (and optionally the whole process) at a specific pipeline."""
    global _process_paths
    paths = StatePaths.create(facility=facility, pipeline=pipeline)
    _tls.paths = paths
    if process_wide:
        _process_paths = paths
    return paths.root


# ── run lock (unchanged logic, just uses new paths) ──────────────────────────
class StateLockedError(RuntimeError):
    pass


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_run_lock(paths: StatePaths, *, force: bool = False) -> None:
    me = {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "pipeline": paths.pipeline,
    }
    for _ in range(2):
        try:
            fd = os.open(paths.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                owner = json.loads(paths.lock.read_text())
            except (OSError, ValueError):
                owner = {}
            if owner.get("host") == me["host"] and owner.get("pid") == me["pid"]:
                return
            same_host = owner.get("host") == me["host"]
            pid = owner.get("pid")
            dead = same_host and isinstance(pid, int) and not _pid_alive(pid)
            if force or dead:
                paths.lock.unlink(missing_ok=True)
                continue
            raise StateLockedError(
                f"{paths.root} is in use by {owner or 'an unknown process'} "
                f"(lock: {paths.lock}). Wait or use force=True."
            )
        else:
            with os.fdopen(fd, "w") as f:
                json.dump(me, f)
            return
    raise StateLockedError(f"could not take {paths.lock}")


def release_run_lock(paths: StatePaths) -> None:
    try:
        owner = json.loads(paths.lock.read_text())
    except (OSError, ValueError):
        return
    if owner.get("host") == socket.gethostname() and owner.get("pid") == os.getpid():
        paths.lock.unlink(missing_ok=True)


@contextlib.contextmanager
def run_lock(paths: StatePaths, *, force: bool = False):
    acquire_run_lock(paths, force=force)
    try:
        yield
    finally:
        release_run_lock(paths)