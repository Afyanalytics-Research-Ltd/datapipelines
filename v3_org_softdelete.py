"""
v3_org_softdelete.py — soft-delete listed rows of one V3 org through the model gateway, the way a DBA's
<db>_softdelete.sql would (deleted_at = now; tables without deleted_at, e.g. finance patient_accounts,
get status = 'closed'). Written for org 168 (2026-10-30: the V2-imported rows of 2026-10-13 hidden
before the org goes live on V3-only data).

The gateway refuses deleted_at on its bulk `update` ("protected"), but an `insert` matched on uuid
sets it, so those rows are written one by one (in parallel); status tables go through `update` by ids.

Every run, in order:
  1. reads the org's rows of each listed table: every listed id must be the org's (else that table
     stops — the SQL's "backed_up = expected"), and writes a backup CSV (id, uuid, current value)
  2. skips rows already soft-deleted (re-runnable / resumable; a dry run stops here)
  3. writes children first (investigations, notes, admissions, invoices, then visits)
  4. re-reads and counts what is still not soft-deleted

Id lists: a JSON file {"<service>.<sql table>": [ids]} (mapping/softdelete/*.json) or the directory /
zip of the DBA's *_softdelete.sql files (STEP 1 "INSERT … id IN (…)" lines).

    python v3_org_softdelete.py --target org168 --ids mapping/softdelete/org168_20261019.json --dry-run
    python v3_org_softdelete.py --target org168 --ids mapping/softdelete/org168_20261019.json
    python v3_org_softdelete.py --target org168 --rollback output/v3_org_softdelete/org168-<time>-backup.csv

Writes output/v3_org_softdelete/<target>-<time>-backup.csv and <target>-<time>.json (counts).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

log = logging.getLogger("v3_org_softdelete")

OUT_DIR = Path("output") / "v3_org_softdelete"
TZ = ZoneInfo("Africa/Nairobi")              # the gateway stores what it is sent as wall time
CLOSED = "closed"
# "<service>.<sql table>" → (gateway alias, field written); children before parents
TABLES = {
    "finance.patient_accounts":   ("patient_account", "status"),
    "evaluation.investigations":  ("investigations", "deleted_at"),
    "evaluation.doctor_notes":    ("doctor_notes", "deleted_at"),
    "evaluation.prescriptions":   ("prescriptions", "deleted_at"),
    "evaluation.vitals":          ("vitals", "deleted_at"),
    "inpatient.inp_admissions":   ("admission", "deleted_at"),
    "finance.invoices":           ("invoice", "deleted_at"),
    "evaluation.visits":          ("visits", "deleted_at"),
    "reception.patients":         ("patient", "deleted_at"),
}
_SQL_IDS = re.compile(r"INSERT INTO \S+ SELECT .*? FROM (\w+) WHERE organization_id = (\d+) AND id IN \(([\d,\s]+)\)")


# ── id lists ────────────────────────────────────────────────────────────────
def ids_from_sql(files: dict[str, str]) -> tuple[dict[str, list[int]], set[int]]:
    """{"evaluation_softdelete.sql": text} → ({"evaluation.investigations": [ids]}, {org ids named})."""
    out: dict[str, set[int]] = {}
    orgs: set[int] = set()
    for name, text in files.items():
        m = re.match(r"(?:.*/)?(\w+?)_softdelete\.sql$", name)
        if not m:                                       # rollback / move files carry no new ids
            continue
        for t, org, ids in _SQL_IDS.findall(text):
            out.setdefault(f"{m.group(1)}.{t}", set()).update(int(x) for x in ids.replace(" ", "").split(",") if x)
            orgs.add(int(org))
    return {k: sorted(v) for k, v in out.items()}, orgs


def load_ids(path: str | Path) -> dict[str, list[int]]:
    """A JSON id list, or a directory / zip of *_softdelete.sql files."""
    p = Path(path).expanduser()
    if p.is_dir():
        files = {str(f): f.read_text(encoding="utf-8") for f in p.rglob("*_softdelete.sql")}
    elif p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as z:
            files = {n: z.read(n).decode("utf-8") for n in z.namelist() if n.endswith("_softdelete.sql")}
    else:
        doc = json.loads(p.read_text(encoding="utf-8"))
        return {k: sorted({int(i) for i in v}) for k, v in (doc.get("tables") or doc).items()}
    ids, _ = ids_from_sql(files)
    if not ids:
        raise ValueError(f"{p}: no '<db>_softdelete.sql' STEP 1 id lists found")
    return ids


def order(tables) -> list[str]:
    """Known tables in TABLES order (children first), unknown ones after, before visits."""
    known = [t for t in TABLES if t in tables]
    rest = sorted(t for t in tables if t not in TABLES)
    if "evaluation.visits" in known:
        known.remove("evaluation.visits")
        return known + rest + ["evaluation.visits"]
    return known + rest


def spec(table: str) -> tuple[str, str, str]:
    """→ (service, alias, field). Unknown tables: the SQL name is the alias, deleted_at is written."""
    service = table.split(".", 1)[0]
    alias, field = TABLES.get(table, (table.split(".", 1)[1], "deleted_at"))
    return service, alias, field


def is_done(row: dict, field: str) -> bool:
    return row.get(field) == CLOSED if field == "status" else bool(row.get(field))


# ── run ─────────────────────────────────────────────────────────────────────
def softdelete(client, ids: dict[str, list[int]], *, dry_run: bool = True, workers: int = 12,
               deleted_at: str | None = None, tables: list[str] | None = None, backup_path: Path | None = None,
               progress_every: int = 5000) -> dict:
    org = client.organization_id
    when = deleted_at or datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    report = {"target": client.key, "organization_id": org, "deleted_at": when, "dry_run": dry_run, "tables": {}}
    lock = threading.Lock()
    bf = open(backup_path, "w", newline="", encoding="utf-8") if backup_path else None
    bw = csv.writer(bf) if bf else None
    if bw:
        bw.writerow(["table", "id", "uuid", "organization_id", "field", "value"])
    try:
        for table in order([t for t in ids if not tables or t in tables]):
            service, alias, field = spec(table)
            want = set(ids[table])
            rows = {r["id"]: r for r in client.read_all(service, alias, per_page=500, workers=4) if r.get("id") in want}
            rep = report["tables"][table] = {"listed": len(want), "found": len(rows), "missing": len(want - set(rows))}
            if bw:
                for i in sorted(rows):
                    bw.writerow([table, i, rows[i].get("uuid"), rows[i].get("organization_id"), field, rows[i].get(field)])
                bf.flush()
            wrong = sorted(i for i, r in rows.items() if r.get("organization_id") not in (None, org))
            if rep["missing"] or wrong:
                rep["error"] = (f"{rep['missing']} listed ids are not org {org}'s"
                                f" (e.g. {sorted(want - set(rows))[:5]}); wrong org: {wrong[:5]} — table left alone")
                log.error("%s: %s", table, rep["error"])
                continue
            todo = [rows[i] for i in sorted(rows) if not is_done(rows[i], field)]
            rep["already"] = len(rows) - len(todo)
            rep["todo"] = len(todo)
            log.info("%s: %d listed, %d already soft-deleted, %d to do", table, len(want), rep["already"], len(todo))
            if dry_run or not todo:
                continue
            ok, failed = 0, []
            t0 = time.time()
            if field == "status":
                for k in range(0, len(todo), 200):
                    chunk = [r["id"] for r in todo[k:k + 200]]
                    r = client.post(service, {"action": "update", "model": alias, "destination_tenant_id": org,
                                              "ids": chunk, "data": {"status": CLOSED}}, timeout=120)
                    found = {x.get("id") for x in (r.json().get("data") or [])} if r.ok else set()
                    ok += len(found & set(chunk))
                    failed += [(i, f"{r.status_code} {r.text[:150]}") for i in chunk if i not in found]
            else:
                def one(row):
                    note = ""
                    for attempt in range(4):
                        try:
                            resp = client.insert(service, alias, {"uuid": row["uuid"], "deleted_at": when}, match_on="uuid")
                            d = (resp.json().get("data") or {}) if resp.ok else {}
                            if d.get("id") == row["id"] and d.get("deleted_at"):
                                return None
                            note = f"{resp.status_code} {resp.text[:150]}"
                        except Exception as e:  # noqa: BLE001 — network: retried, then reported
                            note = f"{type(e).__name__}: {e}"[:150]
                        time.sleep(2 * (attempt + 1))
                    return note

                with ThreadPoolExecutor(max(1, workers)) as ex:
                    for n, (row, note) in enumerate(zip(todo, ex.map(lambda r: one(r) if r.get("uuid") else "no uuid", todo)), 1):
                        if note is None:
                            ok += 1
                        else:
                            failed.append((row["id"], note))
                        if progress_every and n % progress_every == 0:
                            log.info("  %s: %d/%d (%.0fs)", table, n, len(todo), time.time() - t0)
            rep.update(written=ok, failed=len(failed), failed_sample=failed[:10], seconds=round(time.time() - t0))
            log.info("%s: written %d, failed %d (%.0fs)", table, ok, len(failed), time.time() - t0)
            if not dry_run:
                again = {r["id"]: r for r in client.read_all(service, alias, per_page=500, workers=4) if r.get("id") in want}
                rep["still_not_deleted"] = sum(not is_done(r, field) for r in again.values())
    finally:
        if bf:
            bf.close()
    return report


def rollback(client, backup_csv: str | Path, *, workers: int = 12, dry_run: bool = True) -> dict:
    """Put every backed-up row back to its value before the soft delete."""
    rows = list(csv.DictReader(open(backup_csv, encoding="utf-8")))
    report = {"target": client.key, "rollback_of": str(backup_csv), "dry_run": dry_run, "tables": {}}
    org = client.organization_id
    for table in order({r["table"] for r in rows}):
        service, alias, field = spec(table)
        mine = [r for r in rows if r["table"] == table]
        rep = report["tables"][table] = {"rows": len(mine)}
        if dry_run:
            continue
        if field == "status":
            by_value: dict[str, list[int]] = {}
            for r in mine:
                by_value.setdefault(r["value"], []).append(int(r["id"]))
            restored = 0
            for value, idl in by_value.items():
                for k in range(0, len(idl), 200):
                    resp = client.post(service, {"action": "update", "model": alias, "destination_tenant_id": org,
                                                 "ids": idl[k:k + 200], "data": {"status": value}}, timeout=120)
                    restored += len(resp.json().get("data") or []) if resp.ok else 0
            rep["restored"] = restored
        else:
            def one(r):
                resp = client.insert(service, alias, {"uuid": r["uuid"], "deleted_at": r["value"] or None}, match_on="uuid")
                return resp.ok
            with ThreadPoolExecutor(max(1, workers)) as ex:
                rep["restored"] = sum(ex.map(one, mine))
        log.info("%s: restored %s of %d", table, rep.get("restored"), len(mine))
    return report


def run(target: str, *, ids_path: str | None = None, rollback_csv: str | None = None, dry_run: bool = True,
        workers: int = 12, deleted_at: str | None = None, tables: list[str] | None = None) -> dict:
    from v3_to_v3_client import V3Client
    client = V3Client(target)
    client.token()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(TZ).strftime("%Y%m%dT%H%M%S")
    if rollback_csv:
        rep = rollback(client, rollback_csv, workers=workers, dry_run=dry_run)
    else:
        if not ids_path:
            raise ValueError("give --ids (JSON / SQL dir / zip) or --rollback")
        rep = softdelete(client, load_ids(ids_path), dry_run=dry_run, workers=workers, deleted_at=deleted_at,
                         tables=tables, backup_path=OUT_DIR / f"{target}-{stamp}-backup.csv")
        rep["backup"] = str(OUT_DIR / f"{target}-{stamp}-backup.csv")
    rep["report"] = str(OUT_DIR / f"{target}-{stamp}.json")
    Path(rep["report"]).write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    return rep


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", required=True, help="V3 org key (AFYA_<KEY>_* / Connection afya_v3_<key>)")
    ap.add_argument("--ids", help="JSON id list, or directory / zip of *_softdelete.sql")
    ap.add_argument("--rollback", help="a backup CSV of an earlier run: restore its values")
    ap.add_argument("--tables", nargs="*", help="only these (e.g. finance.invoices)")
    ap.add_argument("--deleted-at", help="wall time to stamp (default: now, Africa/Nairobi)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass
    rep = run(a.target, ids_path=a.ids, rollback_csv=a.rollback, dry_run=a.dry_run, workers=a.workers,
              deleted_at=a.deleted_at, tables=a.tables)
    print(json.dumps(rep, indent=1, default=str))


if __name__ == "__main__":
    main()
