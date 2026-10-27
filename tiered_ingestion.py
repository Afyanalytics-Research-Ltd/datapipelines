#!/usr/bin/env python3
"""
tiered_ingestion.py — tiered (bronze → silver → gold) ingestion layers for the
data mapping pipeline: hospital source data → V3 structure.

TIERS   (Snowflake schemas per connection/facility NAME)
  BRONZE   <NAME>_BRONZE.RECORDS      source rows exactly as extracted (secrets
                                      stripped), append-only: every extraction is
                                      kept, so any later tier can be rebuilt.
  SILVER   <NAME>_SILVER.RECORDS      conformed source rows — still the source
                                      schema, after the silver stages (type casting,
                                      null handling, dedup, drift checks …).
  GOLD     <NAME>_GOLD.EVENTS_RAW     V3-shaped records (one per V3 model a source
                                      row maps to) in the EVENTS_RAW layout, so the
                                      existing flattening step builds <NAME>_CLEAN
                                      from it unchanged (SCHEMA_PAIRS entry
                                      ("<NAME>_GOLD", "<NAME>_CLEAN")).
  CONTROL  <NAME>_CONTROL.TIER_BATCHES one row per batch per tier: parent batch,
                                      rows in / out / rejected, status, error.
  Each tier also has a QUARANTINE table: rows a stage rejected, with the reason —
  never silently dropped.

BATCHES
  A batch is one source table's rows in one tier for one run. Its id is
  "<run_id>.<tier>.<source_table>" and each tier's batch names its parent
  (silver → bronze, gold → silver), so a gold record traces back to the exact
  extraction it came from. Writing a batch first deletes that batch id, so
  re-running a run id is idempotent.

LOAD MODES (silver / gold; bronze is always append)
  full         the new batch replaces older batches of the same source table
  incremental  rows replace only older rows with the same key (CDC / watermark runs)

STAGES
  Promotion into SILVER and GOLD runs an ordered list of stages. A stage takes
  the batch's rows and returns the rows to keep, optionally with Rejects:

      from tiered_ingestion import SILVER, Reject, stage

      @stage(SILVER, "coerce_types", order=20)
      def coerce_types(rows, ctx):
          ...
          return kept, [Reject(row, "unparseable date")]

  Slots this sprint (design doc): SILVER — coerce_types / impute_nulls (Mercy),
  detect_drift (Jordan), dedupe (Seka). GOLD — map_to_v3 is built in (order 0,
  from the mappings JSON); target-schema validation stages run after it.

RESUME
  `--resume` promotes each source table's latest finished batch whose next
  tier is missing or failed — after a crash between silver and gold nothing is
  re-extracted. Older unpromoted batches are left alone (promoting them would
  overwrite newer data); they stay in bronze for a deliberate replay.

USAGE
  python tiered_ingestion.py --mappings dags/jsons/wema_medical_data_mappings.json
  python tiered_ingestion.py --mappings … --tables patients visits --through silver
  python tiered_ingestion.py --mappings … --mode incremental
  python tiered_ingestion.py --mappings … --resume
  python tiered_ingestion.py --mappings … --dry-run          # in memory, write nothing

ENV  as for from_json_mappings_to_snowflake.py (AFYA_EXTRACTION_*, SNOWFLAKE_*, AWS_*)
"""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import logging
import os
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Callable, Protocol

log = logging.getLogger("tiered_ingestion")

S3_PREFIX = "raw/tiers"

BRONZE, SILVER, GOLD = "bronze", "silver", "gold"
TIERS = (BRONZE, SILVER, GOLD)
LOAD_MODES = ("full", "incremental")


def next_tier(tier: str) -> str | None:
    i = TIERS.index(tier)
    return TIERS[i + 1] if i + 1 < len(TIERS) else None


def row_hash(payload: dict) -> str:
    """Content hash of a row — stable across key order."""
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def batch_id(run_id: str, tier: str, source_table: str) -> str:
    return f"{run_id}.{tier}.{source_table}"


# ─── STAGES ──────────────────────────────────────────────────────────────

@dataclass
class Reject:
    row: dict
    reason: str
    stage: str = ""


@dataclass
class StageContext:
    """What a stage knows about the batch it is working on."""
    name: str                     # connection / facility name
    tier: str                     # tier being built
    source_table: str
    run_id: str
    mode: str
    mappings: object | None = None   # from_json_mappings_to_snowflake.Mappings


StageFn = Callable[[list[dict], StageContext], "list[dict] | tuple[list[dict], list[Reject]]"]


@dataclass(order=True)
class Stage:
    order: int
    name: str
    fn: StageFn = field(compare=False)


_STAGES: dict[str, list[Stage]] = {SILVER: [], GOLD: []}


def stage(tier: str, name: str, order: int = 100):
    """Register a stage for promotion into `tier`; lower `order` runs first.
    Registering a name again replaces the earlier stage of that name."""
    if tier not in _STAGES:
        raise ValueError(f"stages run on promotion into {sorted(_STAGES)}, not {tier!r}")

    def register(fn: StageFn) -> StageFn:
        _STAGES[tier] = sorted([s for s in _STAGES[tier] if s.name != name] + [Stage(order, name, fn)])
        return fn
    return register


def stages(tier: str) -> list[Stage]:
    return list(_STAGES.get(tier, []))


def run_stages(tier: str, rows: list[dict], ctx: StageContext) -> tuple[list[dict], list[Reject]]:
    rejects: list[Reject] = []
    for s in stages(tier):
        out = s.fn(rows, ctx)
        rows, rejected = out if isinstance(out, tuple) else (out, [])
        for r in rejected:
            r.stage = r.stage or s.name
        rejects.extend(rejected)
        log.debug("  %s/%s %s: %d kept, %d rejected", tier, ctx.source_table, s.name, len(rows), len(rejected))
    return rows, rejects


@stage(GOLD, "map_to_v3", order=0)
def map_to_v3(rows: list[dict], ctx: StageContext) -> list[dict]:
    """Silver rows → one gold record per V3 model the source table maps to
    (the mappings JSON's tables / fields, via V3Model.to_v3)."""
    if ctx.mappings is None:
        raise RuntimeError("gold needs the mappings (pass mappings= to TieredIngestion)")
    out = []
    for m in ctx.mappings.models_for(ctx.source_table):
        for r in rows:
            if m.accepts(r["payload"]):
                out.append({"payload": m.to_v3(r["payload"]), "source_pk": r["source_pk"],
                            "v3_table": m.table, "namespace": m.namespace, "module": m.module})
    return out


# ─── STORES ──────────────────────────────────────────────────────────────

@dataclass
class BatchRecord:
    batch_id: str
    tier: str
    source_table: str
    run_id: str
    mode: str
    parent_batch_id: str | None = None
    status: str = "running"            # running | done | failed
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    targets: str = ""                  # gold: the V3 tables written
    started_at: str = ""
    finished_at: str | None = None
    error: str | None = None


class TierStore(Protocol):
    def ensure(self) -> None: ...
    def write(self, batch: BatchRecord, rows: list[dict]) -> None: ...
    def quarantine(self, batch: BatchRecord, rejects: list[Reject]) -> None: ...
    def read(self, tier: str, batch_id: str) -> list[dict]: ...
    def record_batch(self, batch: BatchRecord) -> None: ...
    def batches(self) -> list[BatchRecord]: ...


def _key(tier: str, row: dict) -> tuple:
    """Identity of a row within its tier, for incremental replacement."""
    return (row.get("v3_table"), row["source_pk"]) if tier == GOLD else (row["source_pk"],)


class MemoryTierStore:
    """In-process store: dry runs and tests. Same semantics as Snowflake."""

    def __init__(self):
        self.tables: dict[str, list[dict]] = {t: [] for t in TIERS}
        self.quarantined: dict[str, list[dict]] = {t: [] for t in TIERS}
        self.control: dict[str, BatchRecord] = {}

    def ensure(self) -> None:
        pass

    def write(self, batch: BatchRecord, rows: list[dict]) -> None:
        kept = [r for r in self.tables[batch.tier] if r["batch_id"] != batch.batch_id]
        if batch.tier != BRONZE:
            same_table = lambda r: r["source_table"] == batch.source_table
            if batch.mode == "full":
                kept = [r for r in kept if not same_table(r)]
            else:
                new = {_key(batch.tier, r) for r in rows}
                kept = [r for r in kept if not (same_table(r) and _key(batch.tier, r) in new)]
        self.tables[batch.tier] = kept + rows

    def quarantine(self, batch: BatchRecord, rejects: list[Reject]) -> None:
        q = [r for r in self.quarantined[batch.tier] if r["batch_id"] != batch.batch_id]
        self.quarantined[batch.tier] = q + [_quarantine_row(batch, r) for r in rejects]

    def read(self, tier: str, batch_id: str) -> list[dict]:
        return [r for r in self.tables[tier] if r["batch_id"] == batch_id]

    def record_batch(self, batch: BatchRecord) -> None:
        self.control[batch.batch_id] = BatchRecord(**asdict(batch))

    def batches(self) -> list[BatchRecord]:
        return list(self.control.values())


def _quarantine_row(batch: BatchRecord, r: Reject) -> dict:
    return {"batch_id": batch.batch_id, "source_table": batch.source_table, "stage": r.stage,
            "reason": r.reason, "source_pk": r.row.get("source_pk"), "payload": r.row.get("payload", r.row),
            "quarantined_at": _now()}


def _sq(value) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _sql_val(value) -> str:
    return "NULL" if value is None else f"'{_sq(value)}'"


class SnowflakeTierStore:
    """The tiers in Snowflake: rows go to S3 as gzipped JSONL, then COPY INTO."""

    # tier → (table, column definitions); every tier row carries its lineage
    TABLES = {
        BRONZE: ("RECORDS", "batch_id STRING, source_table STRING, source_pk STRING, row_hash STRING, "
                            "loaded_at TIMESTAMP_TZ, payload VARIANT"),
        SILVER: ("RECORDS", "batch_id STRING, parent_batch_id STRING, source_table STRING, source_pk STRING, "
                            "row_hash STRING, loaded_at TIMESTAMP_TZ, payload VARIANT"),
        # EVENTS_RAW layout (facility_id … payload) + lineage: flattening reads it as is
        GOLD: ("EVENTS_RAW", "facility_id STRING, ingested_at TIMESTAMP_TZ, module_source STRING, "
                             "source_table STRING, namespace STRING, payload VARIANT, batch_id STRING, "
                             "parent_batch_id STRING, origin_table STRING, source_pk STRING"),
    }
    # gold's source_table is the V3 table (for flattening); the source table is origin_table
    ORIGIN = {BRONZE: "source_table", SILVER: "source_table", GOLD: "origin_table"}
    KEYS = {SILVER: ("source_pk",), GOLD: ("source_table", "source_pk")}
    TIMESTAMPS = {"loaded_at", "ingested_at", "quarantined_at"}

    def __init__(self, name: str):
        from from_json_mappings_to_snowflake import slug
        from facility_to_snowflake_fast_resume import SF_DB
        self.name = slug(name)
        base = f"{SF_DB}.{self.name.upper()}"
        self.schemas = {t: f"{base}_{t.upper()}" for t in TIERS}
        self.schemas["control"] = f"{base}_CONTROL"
        self.control = f"{self.schemas['control']}.TIER_BATCHES"
        self._sf = None
        self._sf_lock = threading.Lock()

    def table(self, tier: str) -> str:
        return f"{self.schemas[tier]}.{self.TABLES[tier][0]}"

    def _client(self):
        with self._sf_lock:
            if self._sf is None:
                from facility_to_snowflake_fast_resume import SnowflakeClient
                self._sf = SnowflakeClient()
            return self._sf

    def close(self) -> None:
        if self._sf is not None:
            self._sf.close()
            self._sf = None

    def _query(self, sql: str, params=None) -> list[tuple]:
        sf = self._client()
        with sf._lock, sf._cursor() as cur:     # SnowflakeClient.query returns pandas; this keeps params
            cur.execute(sql, params)
            return cur.fetchall()

    def ensure(self) -> None:
        sf = self._client()
        for schema in self.schemas.values():
            sf.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}", label="ensure_schema")
        for tier, (table, cols) in self.TABLES.items():
            sf.execute(f"CREATE TABLE IF NOT EXISTS {self.schemas[tier]}.{table} ({cols})", label=f"ensure_{tier}")
            sf.execute(f"""CREATE TABLE IF NOT EXISTS {self.schemas[tier]}.QUARANTINE (
                batch_id STRING, source_table STRING, stage STRING, reason STRING, source_pk STRING,
                payload VARIANT, quarantined_at TIMESTAMP_TZ)""", label=f"ensure_{tier}_quarantine")
        sf.execute(f"""CREATE TABLE IF NOT EXISTS {self.control} (
            batch_id STRING, tier STRING, source_table STRING, run_id STRING, mode STRING,
            parent_batch_id STRING, status STRING, rows_in NUMBER, rows_out NUMBER, rows_rejected NUMBER,
            targets STRING, started_at TIMESTAMP_TZ, finished_at TIMESTAMP_TZ, error STRING)""",
                   label="ensure_control")

    def _copy(self, target: str, columns: list[str], key: str, lines: list[dict], label: str) -> None:
        from facility_to_snowflake_fast_resume import S3_BUCKET, SF_FILE_FORMAT, SF_STAGE, _dumps_bytes, _s3_client
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for line in lines:
                gz.write(_dumps_bytes(line) + b"\n")
        _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        select = ", ".join("$1:payload" if c == "payload" else
                           f"$1:{c}::{'TIMESTAMP_TZ' if c in self.TIMESTAMPS else 'STRING'}" for c in columns)
        self._client().execute(f"""
            COPY INTO {target} ({", ".join(columns)})
            FROM (SELECT {select} FROM @{SF_STAGE})
            FILES = ('{_sq(key)}') FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT}) ON_ERROR = 'ABORT_STATEMENT'
        """, label=label)

    def _lines(self, batch: BatchRecord, rows: list[dict]) -> tuple[list[str], list[dict]]:
        now = _now()
        if batch.tier == GOLD:
            cols = ["facility_id", "ingested_at", "module_source", "source_table", "namespace", "payload",
                    "batch_id", "parent_batch_id", "origin_table", "source_pk"]
            return cols, [{"facility_id": self.name, "ingested_at": now, "module_source": r["module"],
                           "source_table": r["v3_table"], "namespace": r["namespace"], "payload": r["payload"],
                           "batch_id": r["batch_id"], "parent_batch_id": r["parent_batch_id"],
                           "origin_table": r["source_table"], "source_pk": r["source_pk"]} for r in rows]
        cols = ["batch_id", "source_table", "source_pk", "row_hash", "loaded_at", "payload"]
        if batch.tier == SILVER:
            cols.insert(1, "parent_batch_id")
        return cols, [{c: (now if c == "loaded_at" else r.get(c)) for c in cols} for r in rows]

    def write(self, batch: BatchRecord, rows: list[dict]) -> None:
        target, origin = self.table(batch.tier), self.ORIGIN[batch.tier]
        sf = self._client()
        sf.execute(f"DELETE FROM {target} WHERE batch_id = '{_sq(batch.batch_id)}'", label=f"rerun:{batch.batch_id}")
        if rows:
            cols, lines = self._lines(batch, rows)
            key = f"{S3_PREFIX}/{self.name}/tier={batch.tier}/table={batch.source_table}/{batch.run_id}.jsonl.gz"
            self._copy(target, cols, key, lines, label=f"copy:{batch.batch_id}")
        if batch.tier == BRONZE:
            return
        older = f"t.{origin} = '{_sq(batch.source_table)}' AND t.batch_id <> '{_sq(batch.batch_id)}'"
        if batch.mode == "full":
            sf.execute(f"DELETE FROM {target} t WHERE {older}", label=f"replace:{batch.batch_id}")
        else:
            keys = self.KEYS[batch.tier]
            match = " AND ".join(f"t.{k} = n.{k}" for k in keys)
            sf.execute(f"""DELETE FROM {target} t USING (SELECT DISTINCT {", ".join(keys)} FROM {target}
                           WHERE batch_id = '{_sq(batch.batch_id)}') n WHERE {match} AND {older}""",
                       label=f"merge:{batch.batch_id}")

    def quarantine(self, batch: BatchRecord, rejects: list[Reject]) -> None:
        target = f"{self.schemas[batch.tier]}.QUARANTINE"
        self._client().execute(f"DELETE FROM {target} WHERE batch_id = '{_sq(batch.batch_id)}'",
                               label=f"rerun_q:{batch.batch_id}")
        if rejects:
            lines = [_quarantine_row(batch, r) for r in rejects]
            key = f"{S3_PREFIX}/{self.name}/tier={batch.tier}/quarantine/{batch.source_table}/{batch.run_id}.jsonl.gz"
            self._copy(target, list(lines[0]), key, lines, label=f"quarantine:{batch.batch_id}")

    def read(self, tier: str, batch_id: str) -> list[dict]:
        if tier == GOLD:
            raise ValueError("gold is the last tier — nothing reads it back for promotion")
        rows = self._query(f"SELECT source_table, source_pk, row_hash, payload FROM {self.table(tier)} "
                           f"WHERE batch_id = %s", (batch_id,))
        return [{"batch_id": batch_id, "source_table": t, "source_pk": pk, "row_hash": h,
                 "payload": json.loads(p) if isinstance(p, str) else p} for t, pk, h, p in rows]

    def record_batch(self, b: BatchRecord) -> None:
        values = ", ".join(_sql_val(v) for v in (b.batch_id, b.tier, b.source_table, b.run_id, b.mode,
                                                 b.parent_batch_id, b.status))
        self._client().execute(f"""
            MERGE INTO {self.control} c USING (SELECT {_sql_val(b.batch_id)} AS batch_id) s ON c.batch_id = s.batch_id
            WHEN MATCHED THEN UPDATE SET status = {_sql_val(b.status)}, rows_in = {b.rows_in},
                rows_out = {b.rows_out}, rows_rejected = {b.rows_rejected}, targets = {_sql_val(b.targets)},
                finished_at = {_sql_val(b.finished_at)}::TIMESTAMP_TZ, error = {_sql_val((b.error or "")[:2000] or None)}
            WHEN NOT MATCHED THEN INSERT (batch_id, tier, source_table, run_id, mode, parent_batch_id, status,
                rows_in, rows_out, rows_rejected, targets, started_at, finished_at, error)
                VALUES ({values}, {b.rows_in}, {b.rows_out}, {b.rows_rejected}, {_sql_val(b.targets)},
                        {_sql_val(b.started_at)}::TIMESTAMP_TZ, {_sql_val(b.finished_at)}::TIMESTAMP_TZ,
                        {_sql_val(b.error)})""", label=f"control:{b.batch_id}")

    def batches(self) -> list[BatchRecord]:
        cols = [f.name for f in BatchRecord.__dataclass_fields__.values()]
        rows = self._query(f"SELECT {', '.join(cols)} FROM {self.control}")
        return [BatchRecord(**{c: (str(v) if isinstance(v, datetime) else v) for c, v in zip(cols, r)})
                for r in rows]


# ─── TIERED INGESTION ────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TieredIngestion:
    """Land source rows in BRONZE, then promote batch by batch through SILVER
    to GOLD, recording every batch in the control table."""

    def __init__(self, name: str, store: TierStore, *, mappings=None, pk: str = "id"):
        from facility_to_snowflake_fast_resume import _strip_secrets
        self._strip_secrets = _strip_secrets
        self.name, self.store, self.mappings, self.pk = name, store, mappings, pk

    def _run_batch(self, batch: BatchRecord,
                   work: Callable[[], tuple[list[dict], list[Reject]]]) -> tuple[BatchRecord, list[dict]]:
        batch.started_at = _now()
        self.store.record_batch(batch)
        try:
            rows, rejects = work()
            self.store.write(batch, rows)
            self.store.quarantine(batch, rejects)
            batch.rows_out, batch.rows_rejected, batch.status = len(rows), len(rejects), "done"
            if batch.tier == GOLD:
                batch.targets = ",".join(sorted({r["v3_table"] for r in rows}))
        except Exception as e:
            batch.status, batch.error = "failed", f"{type(e).__name__}: {e}"
            raise
        finally:
            batch.finished_at = _now()
            self.store.record_batch(batch)
        log.info("  %-6s %-32s %6d in → %6d out, %d quarantined", batch.tier, batch.source_table,
                 batch.rows_in, batch.rows_out, batch.rows_rejected)
        return batch, rows

    def _land(self, source_table: str, rows: list[dict], run_id: str, mode: str):
        batch = BatchRecord(batch_id(run_id, BRONZE, source_table), BRONZE, source_table, run_id, mode,
                            rows_in=len(rows))

        def work():
            out = []
            for row in rows:
                payload = self._strip_secrets(row)
                h = row_hash(payload)
                pk = payload.get(self.pk)
                out.append({"batch_id": batch.batch_id, "source_table": source_table, "row_hash": h,
                            "source_pk": str(pk) if pk not in (None, "") else f"hash:{h[:16]}", "payload": payload})
            return out, []
        return self._run_batch(batch, work)

    def _promote(self, parent: BatchRecord, rows: list[dict] | None):
        tier = next_tier(parent.tier)
        if tier is None:
            raise ValueError(f"{parent.batch_id}: {parent.tier} is the last tier")
        if parent.status != "done":
            raise ValueError(f"{parent.batch_id} is {parent.status}, not done — not promoting it")
        rows = self.store.read(parent.tier, parent.batch_id) if rows is None else rows
        batch = BatchRecord(batch_id(parent.run_id, tier, parent.source_table), tier, parent.source_table,
                            parent.run_id, parent.mode, parent_batch_id=parent.batch_id, rows_in=len(rows))
        ctx = StageContext(self.name, tier, parent.source_table, parent.run_id, parent.mode, self.mappings)

        def work():
            # deep copy: a stage editing a payload in place must not touch the parent tier
            kept, rejects = run_stages(tier, copy.deepcopy(rows), ctx)
            for r in kept:
                # lineage on every row: this batch, the batch it came from, its source table
                r.update(batch_id=batch.batch_id, parent_batch_id=parent.batch_id, source_table=parent.source_table)
                if tier != GOLD:
                    r["row_hash"] = row_hash(r["payload"])
            return kept, rejects
        return self._run_batch(batch, work)

    def land(self, source_table: str, rows: list[dict], run_id: str, mode: str = "full") -> BatchRecord:
        """Source rows → BRONZE, untouched apart from secret stripping. Rows
        without a primary key get a content-hash key so they stay traceable."""
        return self._land(source_table, rows, run_id, mode)[0]

    def promote(self, parent: BatchRecord) -> BatchRecord:
        """One finished batch into the next tier, read back from its own tier."""
        return self._promote(parent, None)[0]

    def ingest(self, source_table: str, rows: list[dict], run_id: str, *, through: str = GOLD,
               mode: str = "full") -> list[BatchRecord]:
        """Extracted rows → BRONZE → … → `through`; each tier's rows go
        straight on to the next, without reading them back."""
        if mode not in LOAD_MODES:
            raise ValueError(f"mode must be one of {LOAD_MODES}, not {mode!r}")
        batch, rows = self._land(source_table, rows, run_id, mode)
        done = [batch]
        while batch.tier != through:
            batch, rows = self._promote(batch, rows)
            done.append(batch)
        return done

    def pending(self, through: str = GOLD) -> list[BatchRecord]:
        """Each source table's latest-run batches that finished but whose next
        tier (up to `through`) is missing or failed."""
        all_batches = self.store.batches()
        latest = defaultdict(str)
        for b in all_batches:
            latest[b.source_table] = max(latest[b.source_table], b.run_id)
        promoted = {b.parent_batch_id for b in all_batches if b.status == "done" and b.parent_batch_id}
        stop = TIERS.index(through)
        return sorted((b for b in all_batches
                       if b.status == "done" and b.run_id == latest[b.source_table]
                       and TIERS.index(b.tier) < stop and b.batch_id not in promoted),
                      key=lambda b: (b.source_table, TIERS.index(b.tier)))

    def resume(self, through: str = GOLD) -> list[BatchRecord]:
        """Finish interrupted promotions, reading each parent batch back from its
        tier. A table that fails again is logged and left for the next resume."""
        attempted = []
        for parent in self.pending(through):
            ids = [batch_id(parent.run_id, t, parent.source_table)
                   for t in TIERS[TIERS.index(parent.tier) + 1:TIERS.index(through) + 1]]
            attempted += ids
            batch = parent
            try:
                while batch.tier != through:
                    batch = self.promote(batch)
            except Exception as e:
                log.error("  %-32s resume FAILED: %s", parent.source_table, e)
        recorded = {b.batch_id: b for b in self.store.batches()}
        return [recorded[i] for i in attempted if i in recorded]


# ─── CLI ─────────────────────────────────────────────────────────────────

def summarize(batches: list[BatchRecord]) -> None:
    """Rows out per tier per source table: "120 (-3)" = 3 quarantined."""
    by_table = defaultdict(dict)
    for b in batches:
        by_table[b.source_table][b.tier] = b

    def cell(b: BatchRecord | None) -> str:
        if b is None:
            return "-"
        if b.status != "done":
            return b.status.upper()
        return f"{b.rows_out} (-{b.rows_rejected})" if b.rows_rejected else str(b.rows_out)

    print(f"\n  {'source table':32s} " + " ".join(f"{t:>14s}" for t in TIERS) + "  V3 tables")
    for table, tiers in sorted(by_table.items()):
        gold = tiers.get(GOLD)
        print(f"  {table:32s} " + " ".join(f"{cell(tiers.get(t)):>14s}" for t in TIERS)
              + f"  {gold.targets if gold else ''}")


def run(mappings: Path, *, tables: list[str] | None = None, name: str | None = None, through: str = GOLD,
        mode: str = "full", resume: bool = False, dry_run: bool = False, workers: int = 6,
        page_workers: int = 4) -> list[BatchRecord]:
    import from_json_mappings_to_snowflake as fj

    spec = fj.Mappings.load(mappings)
    unknown = sorted(set(tables or []) - set(spec.source_tables))
    if unknown:
        raise SystemExit(f"Not in {Path(mappings).name}: {unknown}")
    client = fj.ExtractionClient(spec.connection_id, page_workers=page_workers)
    name = name or fj.resolve_name(spec, client)
    store = MemoryTierStore() if dry_run else SnowflakeTierStore(name)
    tiered = TieredIngestion(fj.slug(name), store, mappings=spec)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        store.ensure()
        if resume:
            log.info("%s: resuming unfinished promotions through %s", name, through)
            batches = tiered.resume(through)
        else:
            selected = [t for t in spec.source_tables if not tables or t in tables]
            log.info("%s: %d source tables → %s · mode %s · run %s%s", name, len(selected), through, mode,
                     run_id, " (dry run)" if dry_run else "")

            def one(table: str) -> None:
                fj.load_lookups(spec.models_for(table), client)
                tiered.ingest(table, client.fetch_all(table), run_id, through=through, mode=mode)

            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                futures = {pool.submit(one, t): t for t in selected}
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        log.error("  %-32s FAILED: %s", futures[f], e)
            # from the control table, so a table that failed half-way shows its finished tiers too
            batches = [b for b in store.batches() if b.run_id == run_id]
        summarize(batches)
        return batches
    finally:
        if isinstance(store, SnowflakeTierStore):
            store.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mappings", type=Path, required=True, help="The connection's mappings export (JSON)")
    ap.add_argument("--tables", nargs="+", help="Only these source tables")
    ap.add_argument("--name", help="Override the schema name (default: the connection's name)")
    ap.add_argument("--through", choices=TIERS, default=GOLD, help="Last tier to build")
    ap.add_argument("--mode", choices=LOAD_MODES, default="full")
    ap.add_argument("--resume", action="store_true", help="Only finish unfinished promotions")
    ap.add_argument("--dry-run", action="store_true", help="Extract and run every tier in memory; write nothing")
    ap.add_argument("--workers", type=int, default=int(os.getenv("EXTRACTION_WORKERS", "6")))
    ap.add_argument("--page-workers", type=int, default=int(os.getenv("EXTRACTION_PAGE_WORKERS", "4")))
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    for noisy in ("snowflake.connector", "botocore", "urllib3", "facility_pipeline"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    batches = run(args.mappings, tables=args.tables, name=args.name, through=args.through, mode=args.mode,
                  resume=args.resume, dry_run=args.dry_run, workers=args.workers, page_workers=args.page_workers)
    sys.exit(1 if any(b.status != "done" for b in batches) else 0)


if __name__ == "__main__":
    main()
