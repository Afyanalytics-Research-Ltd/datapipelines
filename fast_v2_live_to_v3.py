"""
fast_v2_live_to_v3.py — post the V2 rows V3 is missing, read from the LIVE V2 API, fast (2026-10-15).

For each table:
  1. read the V2 rows: the Snowflake CLEAN snapshot through the migration's own reader, plus every
     newer row from the live V2 API. The API pages in ascending id order and ignores updated_since,
     and serves only ~2 pages of 100 rows a second — so it is read only past the snapshot's highest id
     (minutes, where reading it whole takes hours);
  2. transform them exactly as the migration would post them (snowflake_to_v3_migration's table job,
     fed those rows, posting replaced by capture). Parent links
     go through the migration's id map, topped up from V3 itself (a parent row V3 holds under the
     migration's stable uuid); a row whose parent isn't in V3 is held back, never posted;
  3. drop every row V3 already holds — by uuid, and by content (V3 holds some kisumu_v3 rows under
     uuids the migration didn't make, e.g. 194,989 visit destinations);
  4. post the rest through the gateway, many at once (match_on uuid: a re-run updates, never duplicates).

Orphans can't be loaded: on 2026-10-15, 279,152 result details point at lab results and 567,655
visit destinations at visits that the live V2 API itself doesn't have.

    python fast_v2_live_to_v3.py --dry-run                     # counts only, posts nothing
    python fast_v2_live_to_v3.py --tables evaluation_procedure_prices --limit 3
    python fast_v2_live_to_v3.py --workers 48
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import tempfile
import threading
import time
import uuid as uuid_lib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "dags"))
sys.path.insert(0, "/opt/airflow/dags")

log = logging.getLogger("fast_v2_live_to_v3")

# V2 table → its V2 API namespace. Order matters — parents first: visits → investigations → lab
# results → result details → ranges; visit destinations hang off visits.
TABLES = {
    "evaluation_procedure_prices": r"Ignite\Evaluation\Entities\ProcedurePrices",
    # visits / investigations are the main V2 tables; evaluation_visits / evaluation_investigations
    # are their twins (v3_extra_mappings.TWINS) and post only the ids the main one lacks
    "visits": r"Ignite\Evaluation\Entities\Visit",
    "evaluation_visits": r"Ignite\Evaluation\Entities\Visit",
    "investigations": r"Ignite\Evaluation\Entities\Investigations",
    "evaluation_investigations": r"Ignite\Evaluation\Entities\Investigations",
    "evaluation_investigation_results": r"Ignite\Evaluation\Entities\InvestigationResult",
    "evaluation_investigation_result_details": r"Ignite\Evaluation\Entities\InvestigationResultDetails",
    "evaluation_result_details_ranges": r"Ignite\Evaluation\Entities\ResultDetailsReferenceRanges",
    "evaluation_visit_destinations": r"Ignite\Evaluation\Entities\VisitDestinations",
    "patients": r"Ignite\Reception\Entities\Patients",
    "inpatient_admissions": r"Ignite\Inpatient\Entities\Admission",
    "inpatient_visit_wards": r"Ignite\Inpatient\Entities\InpatientVisitWard",
    "inventory_stores": r"Ignite\Inventory\Entities\Store",
    "inventory_products": r"Ignite\Inventory\Entities\InventoryProducts",
    "inventory_store_products": r"Ignite\Inventory\Entities\StoreProduct",
    "inventory_stock_movements": r"Ignite\Inventory\Entities\StockMovement",
}
# parent alias (as the migration's FK maps name it) → (Snowflake CLEAN table, V2 API namespace)
PARENTS = {
    "investigation_results": ("EVALUATION_INVESTIGATION_RESULTS", r"Ignite\Evaluation\Entities\InvestigationResult"),
    "visit": ("EVALUATION_VISITS", r"Ignite\Evaluation\Entities\Visit"),
    "visits": ("EVALUATION_VISITS", r"Ignite\Evaluation\Entities\Visit"),
    "investigation_result_details": ("EVALUATION_INVESTIGATION_RESULT_DETAILS",
                                     r"Ignite\Evaluation\Entities\InvestigationResultDetails"),
    "investigations": ("INVESTIGATIONS", r"Ignite\Evaluation\Entities\Investigations"),
    "investigation": ("INVESTIGATIONS", r"Ignite\Evaluation\Entities\Investigations"),
    "admission": ("INPATIENT_ADMISSIONS", r"Ignite\Inpatient\Entities\Admission"),
    "patient": ("PATIENTS", r"Ignite\Reception\Entities\Patients"),
}
# FK maps name some parents in the singular (lab results → "investigation"), the poster in the plural:
# one V3 model, so they share V3 reads and the V2→V3 pairs this run finds
SAME_MODEL = {"investigation": "investigations", "visit": "visits"}
CHUNK = 5000


def _s(v) -> str:
    return "" if v is None else " ".join(str(v).split()).lower()


def _t(v) -> str:
    """A timestamp as 'YYYY-MM-DD HH:MM:SS' — V3 returns '2024-09-13T13:26:13.000000Z' for V2's
    '2024-09-13 13:26:13' (same clock)."""
    return "" if v is None else str(v).replace("T", " ")[:19]


def _n(v) -> str:
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return _s(v)


# content key per V3 model: a V2 row whose key V3 already has is already there (under another uuid)
CONTENT_KEY = {
    # older loads left destination_id / department_id unset or mapped differently: visit + arrival
    # second + department name is what every load wrote the same way
    "visit_destination": lambda r: (str(r.get("visit_id")), _s(r.get("arrived_at"))[:19],
                                    _s(r.get("department_name"))),
    "investigation_result_details": lambda r: (str(r.get("investigation_result_id")), _s(r.get("test")),
                                               _s(r.get("title")), _s(r.get("sub_title")), _s(r.get("result"))),
    "result_detail_ranges": lambda r: (str(r.get("result_detail_id")), _s(r.get("type")), _n(r.get("lower")),
                                       _n(r.get("upper")), _s(r.get("bio_reference")), _s(r.get("flag"))),
    "procedure_prices": lambda r: (str(r.get("evaluation_procedure_id")),),
    # V3's own unique keys, as the migration builds them ("kisumu_v3-org4-<no>-<V2 id>", "ADM-<V2 id>");
    # timestamps don't work here — some older rows were stored 3 h earlier (UTC)
    "patient": lambda r: (_s(r.get("patient_no")),),
    "admission": lambda r: (_s(r.get("admission_number")),),
    "stock": lambda r: (str(r.get("product_id")), str(r.get("store_id"))),
    # one profile per user (user_id unique in V3): a user that has one already is skipped
    "user_profile": lambda r: (str(r.get("user_id") or ""),),
    # V3's per-org unique code; names repeat (Main Store in several facilities)
    "product": lambda r: (_s(r.get("code")),),
    "store": lambda r: (_s(r.get("code") or r.get("name")),),
    "stock_movement": lambda r: (str(r.get("product_id")), str(r.get("store_id")), _s(r.get("type")),
                                 _n(r.get("quantity")), _t(r.get("created_at") or r.get("movement_date"))),
    "visit_ward": lambda r: (str(r.get("admission_id")), str(r.get("ward_id")), _t(r.get("allocation_date"))),
    # inpatient notes: 1,253 of V3's are under uuids the migration didn't make; note_date is shifted on
    # some older loads, the admission + title + text is what every load wrote the same way
    "note": lambda r: (str(r.get("admission_id")), _s(r.get("title")), _s(r.get("note_content"))[:200]),
    # V2's own visit key, carried into V3
    "visits": lambda r: (_s(r.get("unique_id")),),
    "investigations": lambda r: (str(r.get("visit") or r.get("visit_id")), _t(r.get("created_at")),
                                 _s(r.get("type")), _n(r.get("price"))),
    "investigation_results": lambda r: (_t(r.get("created_at")), _t(r.get("updated_at")),
                                        _t(r.get("approved_on"))),
}


def _reason(err) -> str:
    """A dead letter's error body, shortened to what's the same for every row it hits (ids, values and
    uuids stripped) so refusals can be counted by cause."""
    if isinstance(err, dict):
        err = err.get("reason") or err.get("message") or err.get("error") or json.dumps(err, default=str)
    s = " ".join(str(err).split())
    s = re.sub(r"\(SQL: .*", "", s)
    s = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>", s)
    s = re.sub(r"'[^']{0,80}'", "'…'", s)
    s = re.sub(r"(?<![a-z_])\d+(?![a-z_])", "N", s)
    return s[:220]


# identifying fields tried, in order, for a model with no CONTENT_KEY
NATURAL_KEYS = ("invoice_no", "receipt_no", "admission_number", "booking_number", "order_number", "number",
                "code", "patient_no", "name", "title")


class Loader:
    def __init__(self, facility: str, workers: int, page_workers: int, dry_run: bool, limit: int,
                 live: bool = False, gaps: dict[str, int] | None = None, report_path: Path | None = None):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([facility])
        import snowflake_to_v3_migration as s2v3
        import v2_to_v3_api_migration as v2v3
        from v3_to_v3_client import V3Client
        self.s2v3, self.m = s2v3, v2v3
        self.facility, self.workers, self.dry_run, self.limit = facility, workers, dry_run, limit
        self.page_workers = page_workers
        v2v3.set_v3_target_facility(facility)
        v2v3._fetch_available_models()
        self.org = v2v3.facility_v3_config(facility)
        login = v2v3.v3_login_org_cfg().get("organization_id")
        if login != self.org.get("organization_id"):
            raise SystemExit(f"V3 login is org {login}, {facility} is org {self.org.get('organization_id')}")
        self.client = V3Client(facility, organization_id=self.org["organization_id"],
                               facility_id=self.org.get("facility_id"))
        self.v2_cache: dict[str, list[dict]] = {}
        self.report: list[dict] = []
        # live: the whole table from the live V2 API, no snapshot — only rows V2 really has today count
        self.live, self.gaps, self.report_path = live, gaps or {}, report_path
        self._namespaces: dict[str, str] = {}
        self._held_why: Counter = Counter()
        self._live_rows = 0
        # V2 id → V3 id for parents found in V3 by uuid or content this run; the migration's id map
        # is reloaded from disk for every table, so these are re-applied before each one
        self.extra: dict[str, dict[int, int]] = defaultdict(dict)

    # ── V2: the Snowflake snapshot + the live API's newer rows ─────────────
    # The live API serves ~2 pages of 100 rows a second whatever the parallelism, ignores
    # updated_since, and pages in ascending id order — so the snapshot supplies the rows it has
    # and the API only its tail: every row with an id above the snapshot's highest.
    def v2_page(self, ns: str, page: int) -> tuple[list[dict], dict]:
        cfg = self.m.V2_FACILITIES[self.facility]
        bodies = [{"namespace": v, "action": "get", "database": cfg["db"], "updated_since": "1970-01-01T00:00:00Z",
                   "limit": 100, "page": page} for v in self.m._namespace_variants(ns)]
        headers = {"Authorization": f"Bearer {self.m._v2_token(self.facility)}", "Content-Type": "application/json"}
        r, _ = self.m._post_with_retry(url=self.m.V2_FACILITIES[self.facility]["base_url"].rstrip("/")
                                       + "/api/finance/access/data/point", headers=headers, bodies=bodies,
                                       facility=self.facility, session=self.m._v2_session(self.facility))
        p = r.json()
        return [{str(k).lower(): v for k, v in x.items()} for x in self.m._extract_rows_from_payload(p)], \
            p.get("pagination") or {}

    def api_tail(self, ns: str, after_id: int) -> list[dict]:
        """Live V2 API rows with id > after_id."""
        key = (ns, after_id)
        if key in self.v2_cache:
            return self.v2_cache[key]
        t0 = time.time()
        _, pg = self.v2_page(ns, 1)
        last = int(pg.get("last_page") or 1)
        lo, hi = 1, last                        # first page whose last id is above after_id
        while lo < hi:
            mid = (lo + hi) // 2
            rows, _ = self.v2_page(ns, mid)
            if rows and int(rows[-1]["id"]) > after_id:
                hi = mid
            else:
                lo = mid + 1
        with ThreadPoolExecutor(self.page_workers) as ex:
            pages = list(ex.map(lambda p: self.v2_page(ns, p)[0], range(lo, last + 1)))
        rows = [r for page in pages for r in page if int(r["id"]) > after_id]
        log.info("V2 API %s: %d rows newer than the snapshot (id > %d), pages %d–%d, in %.0fs",
                 ns.rsplit("\\", 1)[-1], len(rows), after_id, lo, last, time.time() - t0)
        self.v2_cache[key] = rows
        return rows

    def namespace(self, table: str) -> str | None:
        """The V2 class a table is read from: TABLES, else what the migration resolved for it."""
        if table in TABLES:
            return TABLES[table]
        # the V2 class the extraction queue reads it with (the class the API serves): the migration's
        # resolved name 404s for some (evaluation_procedure_tags is Tag, reception_patient_schemes
        # PatientInsurance)
        queue = getattr(self, "_queue_ns", None)
        if queue is None:
            import csv
            path = Path(__file__).resolve().parent / "mapping" / "v2_extraction_queue.csv"
            queue = self._queue_ns = ({r["table"]: r["namespace"] for r in csv.DictReader(path.open())}
                                      if path.exists() else {})
        if queue.get(table):
            return queue[table]
        if not self._namespaces:
            with self.s2v3._snowflake_connect() as conn:
                cur = conn.cursor()
                self._namespaces = {e["table"]: e.get("namespace") for e in self.s2v3.discover_tables(cur, self.facility)}
        return self._namespaces.get(table)

    def sf(self, sql: str) -> list[tuple]:
        with self.s2v3._snowflake_connect() as conn:
            return conn.cursor().execute(sql).fetchall()

    def snapshot_max_id(self, table: str) -> int:
        return int(self.sf(f'SELECT MAX("ID") FROM {self.s2v3.sf_schema(self.facility, "CLEAN")}.{table.upper()}')[0][0] or 0)

    def v2_ids(self, sf_table: str, ns: str) -> list[int]:
        """Every V2 id of a table: the snapshot's plus the live API's newer ones."""
        clean = self.s2v3.sf_schema(self.facility, "CLEAN")
        ids = [int(r[0]) for r in self.sf(f'SELECT DISTINCT "ID" FROM {clean}.{sf_table}') if r[0] is not None]
        return ids + [int(r["id"]) for r in self.api_tail(ns, max(ids, default=0))]

    # ── V3 reads (pages in parallel, retried) ──────────────────────────────
    def v3_rows(self, service: str, alias: str) -> list[dict]:
        def page(n):
            err = None
            for attempt in range(6):
                try:
                    return self.client.read_page(service, alias, n, per_page=500)
                except Exception as e:
                    err = e
                    time.sleep(3 * (attempt + 1))
            raise err
        t0 = time.time()
        rows, meta = page(1)
        last = meta.get("last_page") or 1
        with ThreadPoolExecutor(16) as ex:
            for rs, _ in ex.map(page, range(2, last + 1)):
                rows.extend(rs)
        log.info("V3 %s/%s: %d rows in %.0fs", service, alias, len(rows), time.time() - t0)
        return rows

    def stable_uuid(self, alias: str, v2_id) -> str:
        return str(uuid_lib.uuid5(self.s2v3._STABLE_UUID_NS, f"{self.facility}/{alias}/{v2_id}"))

    def top_up_parent_map(self, alias: str) -> None:
        """id_map[alias][V2 id] for every parent row V3 holds under the migration's stable uuid."""
        if alias not in PARENTS:
            return
        sf_table, ns = PARENTS[alias]
        model = SAME_MODEL.get(alias, alias)
        svc = self.m._alias_to_service.get(model, "core")
        try:
            by_uuid = {str(r.get("uuid") or "").lower(): r["id"] for r in self.v3_rows(svc, model) if r.get("uuid")}
        except Exception as e:
            log.warning("  parent %s: couldn't read V3 (%s) — id map as is", alias, e)
            return
        # write into the map the migration reads for this alias: _id_map_for takes the first
        # non-empty spelling ("investigation" → "investigations"); filling a second, smaller one
        # would shadow it (lab results linked 384 instead of 44,629)
        target = next((a for a in self.s2v3._alias_variants(alias) if self.m._id_map.get(a)), model)
        mp = self.m._id_map.setdefault(target, {})
        added = 0
        for src in {alias, model}:
            for v2_id, v3_id in self.extra.get(src, {}).items():
                if v2_id not in mp and str(v2_id) not in mp:
                    mp[v2_id] = v3_id
                    added += 1
        for v2_id in self.v2_ids(sf_table, ns):
            v3_id = by_uuid.get(self.stable_uuid(model, v2_id)) or by_uuid.get(self.stable_uuid(alias, v2_id))
            if v3_id and v2_id not in mp and str(v2_id) not in mp:
                mp[v2_id] = v3_id
                added += 1
        log.info("  parent %s: %d id-map entries, %d added from V3 by stable uuid", alias, len(mp), added)

    # ── transform through the migration, capturing instead of posting ──────
    def capture(self, table: str) -> tuple[dict, int]:
        m, s2v3 = self.m, self.s2v3
        ns = self.namespace(table)
        if not ns:
            raise LookupError(f"{table}: no V2 namespace known")
        live_ids: set[int] | None = None
        if self.live:
            # every row the live V2 API has: snapshot rows count only if V2 still has their id, and the
            # live rows the snapshot never got go through as API rows (like the tail)
            everything = self.api_tail(ns, 0)
            live_ids = {int(r["id"]) for r in everything}
            self._live_rows = len(live_ids)
            clean = s2v3.sf_schema(self.facility, "CLEAN")
            snap = {int(r[0]) for r in self.sf(f'SELECT DISTINCT "ID" FROM {clean}.{table.upper()}') if r[0] is not None}
            tail = [r for r in everything if int(r["id"]) not in snap]
            log.info("%s: live V2 has %d rows — %d also in the snapshot, %d only live; %d snapshot rows are gone "
                     "from V2", table, len(live_ids), len(live_ids & snap), len(tail), len(snap - live_ids))
        else:
            tail = self.api_tail(ns, self.snapshot_max_id(table))
        if table == "evaluation_visit_destinations" and tail:
            clean = s2v3.sf_schema(self.facility, "CLEAN")
            names = {str(i): (n, s) for i, n, s in self.sf(f'SELECT "ID", "NAME", "SLUG" FROM {clean}.SETTINGS_DESTINATIONS')}
            for r in tail:
                n, s = names.get(str(r.get("destination_id")), (None, None))
                r.setdefault("destination_setting_name", n)
                r.setdefault("destination_setting_slug", s)
        if table == "inpatient_visit_wards" and tail:
            # the snapshot's join (v3_extra_mappings._visit_wards_sql) for the API rows: the visit's
            # latest admission — its id, bed and discharge
            clean = s2v3.sf_schema(self.facility, "CLEAN")
            adm = {}
            for i, v, b, d in self.sf(f'SELECT "ID", "VISIT_ID", "BED_ID", TO_VARCHAR("DISCHARGED_AT") '
                                      f'FROM {clean}.INPATIENT_ADMISSIONS ORDER BY "ID"'):
                adm[str(v)] = (i, b, d)
            for a in sorted(self.api_tail(TABLES["inpatient_admissions"],
                                          0 if self.live else self.snapshot_max_id("inpatient_admissions")),
                            key=lambda x: int(x["id"])):
                adm[str(a.get("visit_id"))] = (a["id"], a.get("bed_id"), a.get("discharged_at"))
            for r in tail:
                a = adm.get(str(r.get("visit_id")))
                if a:
                    r.update({"admission_id": a[0], "adm_bed_id": a[1], "release_at": a[2],
                              "alloc_status": "discharged" if a[2] else "active"})
        snapshot_chunks = s2v3.iter_clean_chunks       # the migration's own reader: its joins + dedupe
        captured: dict[tuple, list[dict]] = defaultdict(list)
        held = {"n": 0}
        lock = threading.Lock()

        def cap(v3_namespace, org_cfg, payload, alias_override=None, service_override=None, **kw):
            alias = alias_override or m._v3_alias(v3_namespace)
            svc = service_override or m._alias_to_service.get(alias, "core")
            with lock:
                captured[(svc, alias, v3_namespace)].append(dict(payload))
            return None

        self._held_why = Counter()

        def count_held(v3_namespace=None, record=None, error_body=None, *a, **k):
            with lock:
                held["n"] += 1
                self._held_why[_reason(error_body)] += 1

        def chunks(facility, tbl, chunk_rows=CHUNK):
            for chunk in snapshot_chunks(facility, tbl, chunk_rows):
                if live_ids is not None:
                    chunk = [r for r in chunk if str(r.get("id") or r.get("ID") or "").isdigit()
                             and int(r.get("id") or r.get("ID")) in live_ids]
                if chunk:
                    yield chunk
            for i in range(0, len(tail), chunk_rows):
                yield [dict(r) for r in tail[i:i + chunk_rows]]

        orig_ensure = m._ensure_id_maps

        def ensure(tk, facility):
            orig_ensure(tk, facility)
            fks = {**m._FK_REMAP.get(tk, {})}
            for e in getattr(self, "_entry_fks", {}).values():
                fks.update(e)
            for alias in set(fks.values()):
                self.top_up_parent_map(alias)

        work = Path(tempfile.mkdtemp(prefix=f"fastload_{table}_"))
        live = Path(m.STATE_ROOT) / self.facility
        if live.exists():
            shutil.copytree(live, work / self.facility)
        (work / self.facility).mkdir(parents=True, exist_ok=True)
        (work / self.facility / ".migration_done.json").unlink(missing_ok=True)

        class Every:
            def __contains__(self, key):
                return True

        tk_skip = dict(s2v3._SKIP_IF_PARENT_NOT_IN_FACILITY)
        patched = {
            (m, "STATE_ROOT"): work, (m, "_post_to_v3_batch"): cap, (m, "_ensure_id_maps"): ensure,
            (m, "_mark_record_inserted"): lambda job_key, record_id: None,
            (m, "_flush_record_progress"): lambda: None, (m, "_flush_id_map"): lambda: None,
            (m, "_write_dead_letter"): count_held,
            (s2v3, "iter_clean_chunks"): chunks, (s2v3, "REPROCESS_JOB_KEYS"): Every(),
            (s2v3, "RECORD_LIMIT"): 0, (s2v3, "RECORD_LOG_EVERY"): 50000, (m, "RECORD_LOG_EVERY"): 50000,
            # parents come from the live API now, not the Snowflake snapshot: an unknown parent is held
            # by the FK remap instead of skipped against the snapshot's ids
            (s2v3, "_SKIP_IF_PARENT_NOT_IN_FACILITY"): {k: [] for k in tk_skip},
        }
        saved = {k: getattr(*k) for k in patched if hasattr(*k)}
        for (mod, attr), val in patched.items():
            setattr(mod, attr, val)
        try:
            entries, _, _ = s2v3._select_entries(self.facility, [table], None, set(m._gateway_model_meta))
            if entries:
                tk = entries[0]["transform"]
                self._entry_fks = {"x": {**m._NS_FK_REMAP.get(entries[0]["v3"] or "", {}), **m._FK_REMAP.get(tk, {})}}
            t0 = time.time()
            run = s2v3.run_one_table(self.facility, table, dry_run=False)
        finally:
            for k in patched:
                if k in saved:
                    setattr(k[0], k[1], saved[k])
            shutil.rmtree(work, ignore_errors=True)
        log.info("%s: transformed (%s) in %.0fs — %d captured, %d held (parent not in V3)", table, run["status"],
                 time.time() - t0, sum(len(v) for v in captured.values()), held["n"])
        self._run = {"status": run.get("status"), "detail": str(run.get("detail") or "")[:400]}
        return captured, held["n"]

    @staticmethod
    def natural_key(payloads: list[dict], existing: list[dict]):
        """A table with no CONTENT_KEY: its own identifying field (number, code, name …) when both sides
        fill it nearly always and V3 keeps it unique — the record's identity whatever its uuid."""
        for f in NATURAL_KEYS:
            ours = [p.get(f) for p in payloads]
            theirs = [r.get(f) for r in existing]
            if not payloads or not existing:
                return None
            filled = lambda xs: sum(x not in (None, "") for x in xs) / len(xs)
            vals = [_s(x) for x in theirs if x not in (None, "")]
            if filled(ours) >= 0.95 and filled(theirs) >= 0.95 and len(set(vals)) >= 0.98 * len(vals):
                log.info("  content key: %s (no CONTENT_KEY for this model)", f)
                return lambda r, f=f: (_s(r.get(f)),)
        return None

    # ── post what V3 lacks ─────────────────────────────────────────────────
    def post(self, table: str, svc: str, alias: str, ns: str, payloads: list[dict], held: int) -> None:
        existing = self.v3_rows(svc, alias)
        have_uuid = {str(r.get("uuid") or "").lower(): r["id"] for r in existing}
        key = CONTENT_KEY.get(alias) or self.natural_key(payloads, existing)
        # a multiset: two real rows with the same key (triage + consultation opened in the same second)
        # each use up one V3 match, so neither is dropped on account of the other
        have_key: dict[tuple, list] = defaultdict(list)
        if key:
            for r in sorted(existing, key=lambda x: int(x["id"])):
                have_key[key(r)].append(r["id"])
        for p in payloads:                    # rows matched by uuid don't consume a content match
            u = str(p.get("uuid") or "").lower()
            if key and u in have_uuid and have_uuid[u] in have_key.get(key(p), []):
                have_key[key(p)].remove(have_uuid[u])

        def remember(p, v3_id):
            if str(p.get("id") or "").isdigit():
                self.extra[alias][int(p["id"])] = v3_id
        unverifiable = by_map = 0
        # the migration's id map pairs V2 ids with V3 rows posted under other uuids (31,610 kisumu_v3
        # lab results): a V2 row whose paired V3 row exists is already there
        v3_ids = {int(r["id"]) for r in existing}
        id_map = {**{int(k): int(v) for k, v in (self.m._id_map.get(alias) or {}).items()
                     if str(k).isdigit() and str(v).isdigit()}, **self.extra.get(alias, {})}
        if alias == "procedure_prices":
            # V3: one price per org + procedure + facility — each procedure's latest V2 price
            payloads = sorted(payloads, key=lambda p: str(p.get("updated_at") or p.get("created_at") or ""),
                              reverse=True)
        fresh, seen_u, seen_k, by_uuid, by_content = [], set(), set(), 0, 0
        for p in payloads:
            u = str(p.get("uuid") or "").lower()
            k = key(p) if key else None
            if u and u in have_uuid:
                remember(p, have_uuid[u])
                by_uuid += 1
                continue
            v2_id = int(p["id"]) if str(p.get("id") or "").isdigit() else None
            if v2_id is not None and id_map.get(v2_id) in v3_ids:
                remember(p, id_map[v2_id])
                by_map += 1
                continue
            if k is not None and have_key.get(k):
                remember(p, have_key[k].pop(0))
                by_content += 1
                continue
            if k is not None and _s(k[0]) in ("", "none"):
                unverifiable += 1           # can't tell whether V3 has it — never risk a duplicate
                continue
            if (u and u in seen_u) or (k is not None and alias == "procedure_prices" and k in seen_k):
                continue
            seen_u.add(u)
            if k is not None:
                seen_k.add(k)
            fresh.append(p)
        rep = {"table": table, "v3": f"{svc}/{alias}", "transformed": len(payloads), "held": held,
               "held_why": dict(self._held_why.most_common(8)), "live_v2": self._live_rows if self.live else None,
               "in_v3_by_uuid": by_uuid + by_map, "in_v3_by_content": by_content, "unverifiable": unverifiable,
               "to_post": len(fresh), "posted": 0, "failed": 0, "duplicate_key": 0, "v3_before": len(existing),
               "failed_why": {}, "status": ""}
        self.report.append(rep)
        # duplicate guard: more rows to post than V3 can be short of means V3 holds some of them under
        # other uuids / content — post nothing, report it
        gap = max(self.gaps.get(table, 0), (self._live_rows - len(existing)) if self.live else 0)
        if fresh and gap >= 0 and len(fresh) > gap * 1.05 + 5:
            rep["status"] = (f"not posted: {len(fresh)} rows unmatched but V3 is short only {gap} — the rest are "
                             f"likely in V3 under other uuids/values (no safe key to tell them apart)")
            log.warning("%s → %s: %s", table, alias, rep["status"])
            self._save_report()
            return
        log.info("%s → %s: transformed %d, held %d, already in V3 %d (uuid) + %d (id map) + %d (content), "
                 "unverifiable %d, to post %d", table, alias, len(payloads), held, by_uuid, by_map, by_content,
                 unverifiable, len(fresh))
        if self.dry_run and fresh:
            # duplicate check: rows to post that share a created_at with a V3 row nothing matched
            matched_v3 = set(self.extra.get(alias, {}).values())
            spare = {_t(r.get("created_at")): r for r in existing if r["id"] not in matched_v3}
            twins = [p for p in fresh if _t(p.get("created_at")) in spare]
            log.info("  [dry-run] %s: %d of %d rows to post share a created_at with an unmatched V3 row",
                     alias, len(twins), len(fresh))
            for p in twins[:2]:
                log.info("  [dry-run]   V2 payload: %s", {k: p[k] for k in list(p)[:25]})
                log.info("  [dry-run]   V3 row:     %s", {k: v for k, v in list(spare[_t(p.get('created_at'))].items())[:25]})
            if not twins:
                log.info("  [dry-run]   sample payload: %s", {k: fresh[0][k] for k in list(fresh[0])[:25]})
        if self.limit:
            fresh = fresh[: self.limit]
        if self.dry_run or not fresh:
            rep["status"] = "dry run" if self.dry_run and fresh else "nothing to post"
            self._save_report()
            return
        lock = threading.Lock()
        t0 = time.time()
        why: Counter = Counter()
        real_dead_letter = self.m._write_dead_letter

        def dead_letter(v3_namespace, record, error_body):     # still written; the reason is kept too
            with lock:
                why[_reason(error_body)] += 1
            real_dead_letter(v3_namespace, record, error_body)

        def one(p):
            try:
                v3_id = self.m._post_to_v3_batch(ns, self.org, p, alias_override=alias, service_override=svc)
                if v3_id:
                    remember(p, int(v3_id))
                    ok = True
                else:
                    ok = None      # the gateway skipped it: a unique key says V3 already has it
            except self.m.RecordDeadLettered:
                ok = False
            except Exception as e:   # network, after the poster's retries
                log.error("  %s uuid=%s failed: %s", alias, p.get("uuid"), str(e)[:200])
                with lock:
                    why[_reason(f"{type(e).__name__}: {e}")] += 1
                ok = False
            with lock:
                rep["posted" if ok else "duplicate_key" if ok is None else "failed"] += 1
                n = rep["posted"] + rep["failed"] + rep["duplicate_key"]
                if n % 2000 == 0:
                    log.info("  %s: %d / %d (%.0f rows/s)", alias, n, len(fresh), n / max(time.time() - t0, 1))

        self.m._write_dead_letter = dead_letter
        try:
            with ThreadPoolExecutor(self.workers) as ex:
                list(ex.map(one, fresh))
        finally:
            self.m._write_dead_letter = real_dead_letter
        rep["failed_why"] = dict(why.most_common(8))
        rep["status"] = "posted"
        self._save_report()
        log.info("%s → %s: %d posted, %d already in V3 (unique key), %d failed in %.0fs", table, alias, rep["posted"], rep["duplicate_key"], rep["failed"],
                 time.time() - t0)

    def _save_report(self) -> None:
        if self.report_path:
            self.report_path.write_text(json.dumps(self.report, indent=1, default=str))

    def run(self, tables: list[str]) -> None:
        for table in tables:
            try:
                captured, held = self.capture(table)
            except Exception as e:                   # one table's failure doesn't stop the others
                log.exception("%s: failed", table)
                self.report.append({"table": table, "status": f"error: {type(e).__name__}: {str(e)[:300]}",
                                    "live_v2": self._live_rows if self.live else None})
                self._save_report()
                continue
            if not captured:
                run = getattr(self, "_run", {})
                self.report.append({"table": table, "status": f"nothing transformed — migration {run.get('status')}: "
                                                              f"{run.get('detail')}", "held": held,
                                    "held_why": dict(self._held_why.most_common(8)),
                                    "live_v2": self._live_rows if self.live else None})
                self._save_report()
            for (svc, alias, ns), payloads in captured.items():
                self.post(table, svc, alias, ns, payloads, held)
        log.info("──── summary ────")
        for r in self.report:
            if "v3" not in r:
                log.info("%-42s %s", r["table"], r["status"])
                continue
            log.info("%-42s %-38s transformed %-7d held %-7d in V3 %-7d to post %-7d posted %-7d failed %d  %s",
                     r["table"], r["v3"], r["transformed"], r["held"], r["in_v3_by_uuid"] + r["in_v3_by_content"],
                     r["to_post"], r["posted"], r["failed"], r.get("status", ""))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-f", "--facility", default="kisumu_v3")
    ap.add_argument("--tables", nargs="*", default=list(TABLES),
                    help="CLEAN table names (any the migration maps; default: the ones above)")
    ap.add_argument("--live", action="store_true",
                    help="read each table whole from the live V2 API: only rows V2 has today are loaded")
    ap.add_argument("--audit", type=Path, default=None,
                    help="reconcile summary CSV: its V2−V3 gaps bound how many rows a table may post")
    ap.add_argument("--report", type=Path, default=None, help="write the per-table report (JSON) here")
    ap.add_argument("--workers", type=int, default=48, help="parallel V3 posts")
    ap.add_argument("--page-workers", type=int, default=32, help="parallel V2 API page reads")
    ap.add_argument("--dry-run", action="store_true", help="read, transform and count; post nothing")
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per table (canary)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    gaps = {}
    if a.audit:
        from export_missing_records import parse_audit
        gaps = {j["v2_table"]: j["gap"] for j in parse_audit(a.audit.read_text())}
    Loader(a.facility, a.workers, a.page_workers, a.dry_run, a.limit, live=a.live, gaps=gaps,
           report_path=a.report).run(a.tables)


if __name__ == "__main__":
    main()
