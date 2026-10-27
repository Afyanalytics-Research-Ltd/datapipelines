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

import contextlib
import json
import os
import re
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_ROOT = Path(os.getenv("MIGRATION_STATE_ROOT") or Path(__file__).resolve().parent)
MIGRATION_ENV = (os.getenv("MIGRATION_ENV") or "").strip()
STATE_ROOT = _DEFAULT_ROOT / ".migration_state"

_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


def _safe(name: str) -> str:
    """Facility / env names become directory names: no separators, no '..'."""
    s = _SAFE.sub("_", name).strip(".")
    if not s:
        raise ValueError(f"Invalid state name: {name!r}")
    return s


@dataclass(frozen=True)
class StatePaths:
    """Immutable paths for one (env, facility)."""
    root: Path
    watermark: Path
    progress: Path
    record_progress: Path
    id_map: Path
    visit_patient: Path
    visit_admission: Path
    done: Path
    dead_letter: Path
    lock: Path
    # pii_corruption removed

    def file(self, name: str, ext: str = "json") -> Path:
        name = name.lstrip(".")
        if name.startswith("migration_"):
            name = name[len("migration_"):]

        if name in ("dead_letter", "failures"):
            return self.dead_letter

        return self.root / f".migration_{name}.{ext}"

    @classmethod
    def for_facility(cls, facility: str | None = None, base: Path | None = None,
                     env: str | None = None) -> "StatePaths":
        base = Path(base) if base else _DEFAULT_ROOT
        env = MIGRATION_ENV if env is None else (env or "").strip()

        if env:
            shared = base / ".migration_state" / _safe(env)
            d = shared / _safe(facility) if facility else shared
        else:
            shared = base
            d = base / ".migration_state" / _safe(facility) if facility else base

        d.mkdir(parents=True, exist_ok=True)
        shared.mkdir(parents=True, exist_ok=True)

        return cls(
            root=d,
            watermark=shared / ".migration_watermarks.json",
            progress=d / ".migration_progress.json",
            record_progress=d / ".migration_record_progress.json",
            id_map=d / ".migration_id_map.json",
            visit_patient=d / ".migration_visit_patient.json",
            visit_admission=d / ".migration_visit_admission.json",
            done=d / ".migration_done.json",
            dead_letter=shared / ".migration_failures.jsonl",
            lock=d / ".migration_run.lock",
    
        )

# ── "current" paths ──────────────────────────────────────────────────────────
# Lookup order: this thread's paths -> the process-wide paths -> unscoped default.
# The process-wide step matters: worker threads (record POSTs, page fetches)
# never call use_state_dir themselves, and used to fall back to the unscoped
# default, i.e. the repo root, instead of the facility being migrated.
_tls = threading.local()
_process_paths: StatePaths | None = None


def get_paths() -> StatePaths:
    p = getattr(_tls, "paths", None)
    if p is not None:
        return p
    if _process_paths is not None:
        return _process_paths
    return StatePaths.for_facility(None)


def use_state_dir(facility: str | None, *, process_wide: bool = False) -> Path:
    """Point this thread (and, with process_wide=True, every thread that has not
    chosen its own) at the facility's state dir. Returns the state dir."""
    global _process_paths
    paths = StatePaths.for_facility(facility)
    _tls.paths = paths
    if process_wide:
        _process_paths = paths
    return paths.root


# ── run lock ─────────────────────────────────────────────────────────────────
class StateLockedError(RuntimeError):
    pass


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":      # os.kill(pid, 0) would TERMINATE the process on Windows
        return True          # be conservative: assume alive
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_run_lock(paths: StatePaths, *, force: bool = False) -> None:
    """Exclusive claim on one facility's state dir. Fails fast if another process
    (on this or any other machine sharing the dir) holds it. A lock from a dead
    process on THIS host is taken over; a lock from another host is never
    stolen automatically (can't tell if it's alive) — pass force=True."""
    me = {"host": socket.gethostname(), "pid": os.getpid(),
          "started": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    for _ in range(2):
        try:
            fd = os.open(paths.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                owner = json.loads(paths.lock.read_text())
            except (OSError, ValueError):
                owner = {}
            if owner.get("host") == me["host"] and owner.get("pid") == me["pid"]:
                return                                   # already ours
            same_host = owner.get("host") == me["host"]
            pid = owner.get("pid")
            dead = same_host and isinstance(pid, int) and not _pid_alive(pid)
            if force or dead:
                paths.lock.unlink(missing_ok=True)
                continue
            raise StateLockedError(
                f"{paths.root} is in use by {owner or 'an unknown process'} "
                f"(lock: {paths.lock}). Wait for it, or re-run with force if you are "
                f"sure it is dead."
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