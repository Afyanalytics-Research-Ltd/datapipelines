"""
medhanit_v1_to_snowflake.py — Medhanit Healthcare's Collabmed V1 server → Snowflake HOSPITALS.MEDHANIT_V1, through
V1's read-only data gateway (Postman collection "v1 Data Gateway").

    POST {MEDHANIT_V1_BASE_URL}/api/access/data/login   {"username", "password"} → success.token (24 h)
    POST {MEDHANIT_V1_BASE_URL}/api/access/data/point   {"table": t, "action": "get", "after_id": n, "per_page": 100}

Every allowlisted table is read in table mode (raw rows, `id` keyset, 100 a page) into a VARCHAR table of the same
name. The gateway allows 60 requests a minute per IP (the 61st answers 429 + Retry-After), so requests are paced to
just under that: the whole server (~1.1M rows) takes about three hours. Progress per table is kept in
MEDHANIT_V1._EXTRACT_STATE, so a run resumes where the last one stopped; a chunk's id range is deleted before it is
written, so re-reading rows never duplicates them. Tables without an `id` (pivots, profiles) page by offset and are
re-read whole every run.

Patient names, phones, ID numbers and e-mails are encrypted in the table (reception_patients,
reception_patients_nok: 'CwDx2DVxu+KDrg0wr6PXSw=='); only the Eloquent model decrypts them. Those pages are read in
table mode and their encrypted columns replaced by the model's answer for the same ids. On 2026-10-29 every model
read of these two failed on the server ("Call to undefined method Illuminate\\Database\\Query\\Builder::decrypt()",
DataPointGatewayController.php:340): until that is fixed they stop with an error instead of storing ciphertext.

reception_patient_documents holds the scanned files themselves (base64): it is read only when named. SMS tables
(contact lists, logs) have no V3 home and are not copied.

    python medhanit_v1_to_snowflake.py                          # everything, resuming
    python medhanit_v1_to_snowflake.py --tables evaluation_visits evaluation_vitals --max-pages 3
    python medhanit_v1_to_snowflake.py --full                   # re-read every table from the start
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, "/opt/airflow/dags")          # pipelines_common
log = logging.getLogger("medhanit_v1")

SF_DB, SF_SCHEMA = "HOSPITALS", "MEDHANIT_V1"
STATE = f'{SF_DB}.{SF_SCHEMA}."_EXTRACT_STATE"'
PER_PAGE = 100                       # the gateway's cap
MIN_INTERVAL = 1.05                  # seconds between requests: 60 a minute is the limit
CHUNK_PAGES = 20                     # pages written to Snowflake (and checkpointed) at a time
# table → the model that decrypts it. The model's `ids` read is keyed on its primary key (id; patient for next of kin)
ENCRYPTED = {
    "reception_patients": r"Ignite\Reception\Entities\Patients",
    "reception_patients_nok": r"Ignite\Reception\Entities\NextOfKin",
}
ON_REQUEST = {"reception_patient_documents"}          # the files themselves: GBs through a 60/min API
# no V3 home: SMS contact lists / logs (28k names + phones), dashboard widgets, page counters — not copied at all
SKIP = {"page_count", "sms_contacts", "sms_journal", "sms_messages", "sms_prefs", "sms_responses", "sms_wallet",
        "core_dashboard_widgets", "core_notifications"}
_CIPHER = re.compile(r"^[A-Za-z0-9+/]{22}==$")        # one AES block, base64: what an encrypted column holds


class GatewayError(RuntimeError):
    pass


class V1Gateway:
    def __init__(self, base_url: str, username: str, password: str):
        self.base, self.username, self.password = base_url.rstrip("/"), username, password
        self.s = requests.Session()
        self.token: str | None = None
        self._last = 0.0
        self.calls = 0

    def login(self) -> None:
        r = self.s.post(f"{self.base}/api/access/data/login", json={"username": self.username, "password": self.password},
                        timeout=60)
        token = ((r.json() if r.headers.get("content-type", "").startswith("application/json") else {}) or {}) \
            .get("success", {}).get("token") if r.ok else None
        if not token:
            raise GatewayError(f"V1 gateway login failed: {r.status_code} {r.text[:200]}")
        self.token = token

    def call(self, body: dict) -> dict:
        """One gateway request, paced under the rate limit; 429 waits Retry-After, 401 logs in again, network errors
        and 5xx retry with back-off. A 4xx (or a 500 the server raises every time) is returned as an error."""
        if not self.token:
            self.login()
        waits = [10, 30, 60, 120, 300, 600]
        relogged = False
        while True:
            pause = MIN_INTERVAL - (time.time() - self._last)
            if pause > 0:
                time.sleep(pause)
            self._last = time.time()
            self.calls += 1
            try:
                r = self.s.post(f"{self.base}/api/access/data/point", json=body, timeout=180,
                                headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"})
            except requests.RequestException as e:
                if not waits:
                    raise GatewayError(f"{_what(body)}: {type(e).__name__}: {e}") from e
                w = waits.pop(0)
                log.warning("  %s: %s — retrying in %ds", _what(body), type(e).__name__, w)
                time.sleep(w)
                continue
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After") or 30) + 1)
                continue
            if r.status_code == 401 and not relogged:
                relogged = True
                self.login()
                continue
            if r.status_code in (502, 503, 504) and waits:
                w = waits.pop(0)
                log.warning("  %s: HTTP %d — retrying in %ds", _what(body), r.status_code, w)
                time.sleep(w)
                continue
            try:
                data = r.json()
            except ValueError:
                raise GatewayError(f"{_what(body)}: HTTP {r.status_code} {r.text[:300]}")
            if r.status_code != 200 or data.get("success") is False:
                raise GatewayError(f"{_what(body)}: HTTP {r.status_code} {str(data.get('message') or data)[:300]}")
            return data


def _what(body: dict) -> str:
    return body.get("table") or body.get("namespace") or body.get("action", "?")


def text(v):
    """A gateway value as the text Snowflake stores (None stays NULL; JSON stays JSON)."""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def looks_encrypted(rows: list[dict], cols) -> bool:
    """True when the named columns still hold ciphertext (an answer that didn't decrypt)."""
    vals = [r.get(c) for r in rows for c in cols if r.get(c)]
    return bool(vals) and sum(1 for v in vals if _CIPHER.match(str(v))) > len(vals) / 2


def decrypt_page(gw: V1Gateway, table: str, rows: list[dict], pk: str) -> list[dict]:
    """Replace a table-mode page's encrypted columns with the model's (decrypted) values for the same keys."""
    if not rows:
        return rows
    model = ENCRYPTED[table]
    keys = [r[pk] for r in rows if r.get(pk) is not None]
    try:
        got = gw.call({"namespace": model, "action": "get", "ids": keys, "with_trashed": True}).get("data") or []
    except GatewayError as e:
        raise GatewayError(f"{table}: the model {model} can't be read, so names / phones can't be decrypted — the V1 "
                           f"gateway must be fixed first ({e})") from e
    by = {str(r.get(pk)): r for r in got}
    enc = [c for c in rows[0] if any(_CIPHER.match(str(r.get(c) or "")) for r in rows)]
    out = []
    for r in rows:
        m = by.get(str(r.get(pk)))
        out.append({**r, **{c: m.get(c) for c in enc}} if m else r)
    if looks_encrypted(out, enc):
        raise GatewayError(f"{table}: the model answered but the values are still encrypted ({enc}) — not stored")
    return out


class Extractor:
    def __init__(self, full: bool = False, max_pages: int = 0):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([])
        import snowflake_to_v3_migration as s2v3
        from snowflake.connector.pandas_tools import write_pandas
        self.write_pandas = write_pandas
        self.gw = V1Gateway(os.environ.get("MEDHANIT_V1_BASE_URL", "https://medhanit.collabmed.net"),
                            os.environ["MEDHANIT_V1_USERNAME"], os.environ["MEDHANIT_V1_PASSWORD"])
        self.full, self.max_pages = full, max_pages
        self.sf = s2v3._snowflake_connect()
        self.ex(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_SCHEMA}")
        self.ex(f"CREATE TABLE IF NOT EXISTS {STATE} (SOURCE VARCHAR, AFTER_ID NUMBER, FINAL_ID NUMBER, ROWS_READ NUMBER, "
                f"COMPLETE BOOLEAN, UPDATED_AT TIMESTAMP_NTZ)")

    def ex(self, sql: str, params=None):
        cur = self.sf.cursor()
        cur.execute(sql, params)
        return cur

    def fq(self, table: str) -> str:
        return f'{SF_DB}.{SF_SCHEMA}."{table.upper()}"'

    def state(self, table: str) -> tuple[int, int]:
        row = self.ex(f"SELECT AFTER_ID, ROWS_READ FROM {STATE} WHERE SOURCE = %s", (table,)).fetchone()
        return (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)

    def save_state(self, table: str, after_id: int, final_id, rows: int, complete: bool) -> None:
        self.ex(f"DELETE FROM {STATE} WHERE SOURCE = %s", (table,))
        self.ex(f"INSERT INTO {STATE} VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())",
                (table, after_id, final_id, rows, complete))

    def create(self, table: str, cols: list[str], replace: bool) -> None:
        verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
        self.ex(f"{verb} {self.fq(table)} (" + ", ".join(f'"{c.upper()}" VARCHAR' for c in cols)
                + ', "_EXTRACTED_AT" VARCHAR)')
        have = {r[2].upper() for r in self.ex(f"SHOW COLUMNS IN TABLE {self.fq(table)}").fetchall()}
        for c in cols:                      # a column V1 gained since the table was made
            if c.upper() not in have:
                self.ex(f'ALTER TABLE {self.fq(table)} ADD COLUMN "{c.upper()}" VARCHAR')

    def write(self, table: str, cols: list[str], rows: list[dict], id_range: tuple[int, int] | None) -> None:
        import pandas as pd
        if id_range:                        # idempotent: whatever an earlier run stored for these ids goes first
            self.ex(f'DELETE FROM {self.fq(table)} WHERE TRY_TO_NUMBER("ID") > %s AND TRY_TO_NUMBER("ID") <= %s', id_range)
        if not rows:
            return
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame([[text(r.get(c)) for c in cols] + [now] for r in rows],
                          columns=[c.upper() for c in cols] + ["_EXTRACTED_AT"], dtype=object)
        ok, _, n, _ = self.write_pandas(self.sf, df, table_name=table.upper(), database=SF_DB, schema=SF_SCHEMA,
                                        quote_identifiers=True, auto_create_table=False)
        if not ok or n != len(rows):
            raise RuntimeError(f"{table}: Snowflake wrote {n} of {len(rows)} rows")

    def tables(self) -> list[dict]:
        return [t for t in self.gw.call({"action": "tables"})["tables"] if t.get("exists")]

    def extract(self, table: str) -> dict:
        d = self.gw.call({"table": table, "action": "describe"})
        cols = [c["name"] if isinstance(c, dict) else c for c in d["columns"]]
        cols = [c for c in cols if c not in (d.get("withheld_columns") or [])]
        pk = (d.get("primary_key") or ["id"])[0]
        final_id = d.get("final_id")
        if d.get("paging") == "offset":
            return self._offset(table, cols, pk)
        after, total = (0, 0) if self.full else self.state(table)
        self.create(table, cols, replace=self.full or (after == 0 and not total))
        if final_id is not None and after >= int(final_id):
            return {"rows": 0, "up_to_date": True, "final_id": final_id}
        read, pages, buf, start = 0, 0, [], after
        while True:
            res = self.gw.call({"table": table, "action": "get", "after_id": after, "per_page": PER_PAGE, "with_trashed": True})
            rows = res.get("data") or []
            if table in ENCRYPTED:
                rows = decrypt_page(self.gw, table, rows, pk)
            buf += rows
            pages += 1
            pg = res.get("pagination") or {}
            more = bool(pg.get("has_more_pages")) and bool(rows)
            after = int(pg.get("next_after_id") or (rows[-1]["id"] if rows else after))
            stop = self.max_pages and pages >= self.max_pages
            if len(buf) >= CHUNK_PAGES * PER_PAGE or not more or stop:
                self.write(table, cols, buf, (start, after))
                read += len(buf)
                self.save_state(table, after, final_id, total + read, not more)
                log.info("  %-40s %8d rows (after id %s of %s)", table, total + read, after, final_id)
                buf, start = [], after
            if not more or stop:
                return {"rows": read, "after_id": after, "final_id": final_id, "complete": not more}

    def _offset(self, table: str, cols: list[str], pk: str) -> dict:
        self.create(table, cols, replace=True)
        page, read, buf = 1, 0, []
        while True:
            res = self.gw.call({"table": table, "action": "get", "page": page, "per_page": PER_PAGE, "with_total": False})
            rows = res.get("data") or []
            if table in ENCRYPTED:
                rows = decrypt_page(self.gw, table, rows, pk)
            buf += rows
            more = bool((res.get("pagination") or {}).get("has_more_pages")) and bool(rows)
            if len(buf) >= CHUNK_PAGES * PER_PAGE or not more:
                self.write(table, cols, buf, None)
                read += len(buf)
                buf = []
            if not more or (self.max_pages and page >= self.max_pages):
                if buf:
                    self.write(table, cols, buf, None)
                    read += len(buf)
                self.save_state(table, 0, None, read, not more)
                log.info("  %-40s %8d rows (offset, re-read whole)", table, read)
                return {"rows": read, "paging": "offset", "complete": not more}
            page += 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tables", nargs="*", help="only these V1 tables (default: every allowlisted one)")
    ap.add_argument("--full", action="store_true", help="re-read every table from the start")
    ap.add_argument("--max-pages", type=int, default=0, help="at most N pages per table this run (canary)")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    x = Extractor(full=a.full, max_pages=a.max_pages)
    listed = x.tables()
    names = [t["table"] for t in listed]
    if a.tables:
        unknown = [t for t in a.tables if t not in names]
        if unknown:
            raise SystemExit(f"not allowlisted on the V1 gateway: {unknown}")
        todo = list(a.tables)
    else:
        todo = [t for t in names if t not in SKIP and t not in ON_REQUEST]
    log.info("V1 %s → %s.%s: %d table(s)", x.gw.base, SF_DB, SF_SCHEMA, len(todo))
    report, bad = {}, []
    for t in todo:
        t0 = time.time()
        try:
            report[t] = x.extract(t)
        except Exception as e:
            log.error("  %-40s FAILED: %s", t, str(e)[:400])
            report[t] = {"error": str(e)[:400]}
            bad.append(t)
        report[t]["seconds"] = round(time.time() - t0)
    log.info("done: %d table(s), %d gateway calls; failed: %s", len(todo), x.gw.calls, bad or "none")
    print(json.dumps(report, indent=1, default=str))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
