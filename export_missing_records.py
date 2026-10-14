"""
export_missing_records.py — V2 rows that an audit says are missing from V3, exported as CSV + MySQL load
scripts (one pair per V3 table) for a DBA to bulk-load, instead of posting them through the gateway.

Per V2 table:
  1. the migration pipeline reads and transforms the rows exactly as it would post them (renames,
     defaults, V2 → V3 id remapping; rows whose parent isn't in V3 are held back, never exported) —
     posting is replaced by capture, against a throwaway copy of the migration state, so nothing is
     posted and the real state is untouched;
  2. V3 is read (pages in parallel, retried) and every row whose uuid it already holds is dropped;
  3. duplicate guard: if more rows remain than the audit's gap (V2 rows − V3 rows) allows, V3 holds
     some of them under other uuids — the CSV goes to quarantine/ with no load script;
  4. <table>.csv + <table>_load.sql (LOAD DATA LOCAL INFILE in a transaction, with a count check).

Used by dags/export_missing_records_dag.py (one task per table); also runnable by hand:
    python export_missing_records.py kisumu_v3 audit.csv /tmp/out [table …]
"""
from __future__ import annotations

import csv
import io
import json
import logging
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("export_missing_records")

V3_READ_THREADS = 6
DUPLICATE_TOLERANCE = 1.05      # export may exceed the audit gap by 5 % (+5 rows) before it is quarantined


# ─── audit ──────────────────────────────────────────────────────────────────

def parse_audit(text: str) -> list[dict]:
    """Rows of a reconcile summary CSV where V3 is short of V2: [{v2_table, target, tier, gap}].
    The per-tier totals block after the blank line is ignored."""
    body = text.replace("\r\n", "\n").split("\n\n", 1)[0]
    out, by_table = [], {}
    for r in csv.DictReader(io.StringIO(body)):
        try:
            gap = int(r.get("v2_rows") or 0) - int(r.get("v3_rows") or 0)
        except ValueError:
            continue
        table, target = (r.get("table") or "").strip(), (r.get("target") or "").strip()
        if gap <= 0 or not table or target in ("", "-") or (r.get("why") or "").strip():
            continue
        if table in by_table:                   # one V2 table, several targets: keep the largest gap
            by_table[table]["gap"] = max(by_table[table]["gap"], gap)
            continue
        by_table[table] = {"v2_table": table, "target": target, "tier": (r.get("tier") or "").strip(), "gap": gap}
        out.append(by_table[table])
    return out


# ─── output helpers ─────────────────────────────────────────────────────────

def csv_cell(v) -> str:
    """One CSV field for MySQL LOAD DATA … ENCLOSED BY '"' ESCAPED BY '': NULL unquoted, else quoted."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        v = int(v)
    if isinstance(v, (dict, list)):
        v = json.dumps(v, default=str)
    return '"' + str(v).replace('"', '""') + '"'


def load_sql(db_table: str, service: str, cols: list[str], n: int, org: int, fac: int | None, when: str) -> str:
    return (f"-- {service.upper()} service database: load {n} V2 rows missing from V3 into {db_table} "
            f"(organization {org}{f', facility {fac}' if fac else ''}). Generated {when} UTC.\n"
            f"-- Run with {db_table}.csv in the working directory:  mysql --local-infile=1 <db> < {db_table}_load.sql\n"
            f"SELECT COUNT(*) AS before_load FROM {db_table} WHERE organization_id = {org};\n"
            f"START TRANSACTION;\n"
            f"LOAD DATA LOCAL INFILE '{db_table}.csv' INTO TABLE {db_table}\n"
            f"  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY ''\n"
            f"  LINES TERMINATED BY '\\n' IGNORE 1 LINES\n  ({', '.join(cols)});\n"
            f"SELECT ROW_COUNT() AS loaded, {n} AS expected;\n"
            f"-- loaded = expected → COMMIT;   otherwise → ROLLBACK;\n")


def is_suspect(n_export: int, gap: int | None) -> bool:
    """More rows than the audit says are missing → V3 has some under other uuids."""
    return gap is not None and n_export > max(gap, 0) * DUPLICATE_TOLERANCE + 5


# ─── V3 ─────────────────────────────────────────────────────────────────────

def v3_uuids(client, service: str, alias: str) -> tuple[set[str], set[str]]:
    """(every uuid V3 holds for the client's org, the model's columns) — pages read in parallel."""
    def page(n):
        err = None
        for attempt in range(5):
            try:
                return client.read_page(service, alias, n, per_page=500)
            except Exception as e:        # gateway timeouts under load: back off and retry
                err = e
                time.sleep(5 * (attempt + 1))
        raise err

    rows, meta = page(1)
    cols = set(rows[0]) if rows else set()
    uuids = {str(r.get("uuid") or "").lower() for r in rows}
    with ThreadPoolExecutor(V3_READ_THREADS) as ex:
        for rs, _ in ex.map(page, range(2, (meta.get("last_page") or 1) + 1)):
            uuids |= {str(r.get("uuid") or "").lower() for r in rs}
    uuids.discard("")
    return uuids, cols


# ─── export ─────────────────────────────────────────────────────────────────

class _Every:
    """REPROCESS_JOB_KEYS stand-in: the local "already inserted" list can claim rows V3 doesn't have,
    so every row is considered — V3's own uuids decide what is missing."""
    def __contains__(self, key):
        return True


def export_table(facility: str, v2_table: str, gap: int | None, out_dir: Path, *,
                 organization_id: int, facility_id: int, state_root: Path) -> list[dict]:
    """Export one V2 table's missing rows. Returns one report dict per V3 table written (a V2 table can
    feed more than one, e.g. vitals → outpatient + inpatient)."""
    import v2_to_v3_api_migration as v2v3
    import snowflake_to_v3_migration as m
    from v3_db_tables import V3_DB_TABLES
    from v3_to_v3_client import V3Client

    out_dir = Path(out_dir)
    (out_dir / "quarantine").mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f"export_{v2_table}_"))
    live = Path(state_root) / facility
    if live.exists():
        shutil.copytree(live, work / facility)
    (work / facility).mkdir(parents=True, exist_ok=True)
    (work / facility / ".migration_done.json").unlink(missing_ok=True)

    captured: dict[tuple, list[dict]] = defaultdict(list)
    fac_cols: dict[tuple, str | None] = {}

    def capture(v3_namespace, org_cfg, payload, alias_override=None, service_override=None, **kw):
        alias = alias_override or v2v3._v3_alias(v3_namespace)
        svc = service_override or v2v3._alias_to_service.get(alias, "core")
        key = (svc, alias, v3_namespace.rsplit("\\", 1)[-1])
        fac_cols[key] = (v2v3._gateway_model_meta.get(alias) or {}).get("facility")
        captured[key].append(dict(payload))
        return None

    saved = (v2v3.STATE_ROOT, v2v3._post_to_v3_batch, m.REPROCESS_JOB_KEYS, m.RECORD_LIMIT)
    v2v3.STATE_ROOT, v2v3._post_to_v3_batch = work, capture
    m.REPROCESS_JOB_KEYS, m.RECORD_LIMIT = _Every(), 0
    t0 = time.time()
    tier = None
    try:
        run = m.run_one_table(facility, v2_table, dry_run=False)
        try:
            entries, _, _ = m._select_entries(facility, [v2_table], None, set(v2v3._gateway_model_meta))
            tier = m._tier(entries[0]) if entries else None
        except Exception:
            pass
    finally:
        v2v3.STATE_ROOT, v2v3._post_to_v3_batch, m.REPROCESS_JOB_KEYS, m.RECORD_LIMIT = saved
        shutil.rmtree(work, ignore_errors=True)
    log.info("%s: transformed (%s) in %.0fs %s", v2_table, run["status"], time.time() - t0, str(run.get("detail", ""))[:200])

    if not captured:
        return [{"v2_table": v2_table, "status": "nothing to export", "pipeline": run["status"],
                 "detail": run.get("detail", ""), "audit_gap": gap, "to_load": 0, "tier": tier}]

    client = V3Client(facility, organization_id=organization_id, facility_id=facility_id)
    reports = []
    for (svc, alias, cls), rows in captured.items():
        db_table = V3_DB_TABLES.get(f"{svc}:{cls}") or alias
        try:
            existing, cols_v3 = v3_uuids(client, svc, alias)
        except Exception as e:
            reports.append({"v2_table": v2_table, "service": svc, "db_table": db_table, "status": "ERROR",
                            "detail": f"could not read V3: {str(e)[:300]}", "audit_gap": gap, "to_load": 0, "tier": tier})
            continue
        fresh, seen = [], set()
        for p in rows:
            u = str(p.get("uuid") or "").lower()
            if u and u not in existing and u not in seen:
                seen.add(u)
                fresh.append(p)
        suspect = is_suspect(len(fresh), gap)
        fac_col = fac_cols.get((svc, alias, cls))
        skip = {"id", "organization_id", "deleted_at"} | ({fac_col} if fac_col else set())
        cols = sorted({c for p in fresh for c in p} & cols_v3 - skip) + ["organization_id"] + ([fac_col] if fac_col else [])
        when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        files = []
        if fresh:
            folder = out_dir / "quarantine" if suspect else out_dir
            with open(folder / f"{db_table}.csv", "w", encoding="utf-8", newline="") as f:
                f.write(",".join(cols) + "\n")
                for p in fresh:
                    row = {**p, "organization_id": organization_id}
                    if fac_col:
                        row[fac_col] = facility_id
                    row.setdefault("created_at", when)
                    f.write(",".join(csv_cell(row.get(c)) for c in cols) + "\n")
            files.append(str((folder / f"{db_table}.csv").relative_to(out_dir)))
            if not suspect:
                (out_dir / f"{db_table}_load.sql").write_text(
                    load_sql(db_table, svc, cols, len(fresh), organization_id, facility_id if fac_col else None, when))
                files.append(f"{db_table}_load.sql")
        reports.append({"v2_table": v2_table, "service": svc, "alias": alias, "db_table": db_table, "tier": tier,
                        "transformed": len(rows), "already_in_v3": len(rows) - len(fresh), "to_load": len(fresh),
                        "audit_gap": gap, "pipeline": run["status"], "detail": run.get("detail", ""), "files": files,
                        "status": ("QUARANTINED — more than the audit gap, likely already in V3 under other uuids"
                                   if suspect else "ready" if fresh else "nothing missing")})
        log.info("%s → %s/%s: transformed %d, already in V3 %d, to load %d (audit gap %s) — %s", v2_table, svc,
                 db_table, len(rows), len(rows) - len(fresh), len(fresh), gap, reports[-1]["status"])
    return reports


def readme(reports: list[dict], facility: str, organization_id: int, audit_name: str) -> str:
    """Load order (parents first: pipeline tier), counts, and what was not exported and why."""
    ready = sorted((r for r in reports if r.get("status") == "ready"), key=lambda r: (r.get("tier") or 99, r["db_table"]))
    lines = [f"V2 rows missing from V3 ({facility} → organization {organization_id}) — from audit {audit_name}",
             f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC", "",
             "LOAD IN THIS ORDER (parents before children), each in its service's database:",
             "  mysql --local-infile=1 <db> < <table>_load.sql   (with <table>.csv in the same folder)", ""]
    for i, r in enumerate(ready, 1):
        lines.append(f"  {i:>3}. [{r['service']:10}] {r['db_table']:36} {r['to_load']:>8} rows  (V2 {r['v2_table']}, tier {r.get('tier')})")
    lines.append(f"\n  total: {sum(r['to_load'] for r in ready)} rows in {len(ready)} table(s)\n")
    other = [r for r in reports if r.get("status") != "ready"]
    if other:
        lines.append("NOT LOADED:")
        for r in sorted(other, key=lambda r: r["v2_table"]):
            why = r.get("status")
            if r.get("status") in ("nothing to export", "nothing missing") and r.get("detail"):
                why += f" — {str(r['detail'])[:160]}"
            lines.append(f"  {r['v2_table']:42} {('→ ' + str(r.get('db_table'))) if r.get('db_table') else '':38} "
                         f"to_load {r.get('to_load', 0):>7} gap {r.get('audit_gap')!s:>7}  {why}")
    lines += ["", "Rows whose parent (patient, visit, admission, user …) isn't in V3 are never exported — load the",
              "parents, then re-run the export to pick their children up. quarantine/ holds CSVs NOT safe to load."]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    facility, audit_file, out = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    only = set(sys.argv[4:])
    import v2_to_v3_api_migration as v2v3
    jobs = [j for j in parse_audit(Path(audit_file).read_text()) if not only or j["v2_table"] in only]
    reps = []
    for j in jobs:
        reps += export_table(facility, j["v2_table"], j["gap"], out, organization_id=4, facility_id=4,
                             state_root=v2v3.STATE_ROOT)
    (out / "README.txt").write_text(readme(reps, facility, 4, Path(audit_file).name))
    print((out / "README.txt").read_text())
