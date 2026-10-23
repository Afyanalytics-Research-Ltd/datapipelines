# Runbook: moving a facility's data V2 → V3, and copying a V3 organisation into another

This is how the Kisumu migration was done end to end (October 2026): V2 → V3 org 4 on the migrate
servers, then org 4 → production org 284. Follow it in order to repeat it for Kisumu or another facility.
Every rule the pipelines depend on is in the code, with a comment saying why. This page covers what to
run, in what order, and what the results mean.

---

## 0. Before you start (once)

| Need | Where | Notes |
|---|---|---|
| A **superadmin** account in the source org and in the target org | the V3 admin | The gateway refuses everything else (`403 restricted to superadmin`). |
| The target org's **id** and its **facility id** | login response (`tenant.id`, `facility.id`) | For Kisumu: org **284**, facility **283**. A wrong facility gets stamped on every row and **can't be changed later** through the gateway. |
| The **X-Migration-Key** of each instance | backend team | Needed for theatre and dialysis. |
| Airflow Connection `afya_v3_<key>` for each side | Airflow → Admin → Connections | login/password = the account above; Extra `{"organization_id": 284, "facility_id": 283, "url_template": "https://{service}.afyaanalytics.ai/api/", "migration_key": "…"}`. Source (migrate servers): `url_template` `https://{service}migrate.afyaanalytics.ai/api/`. |
| The **V2 → V3 id map** of the source | `.migration_state/<source>/.migration_id_map.json` (written by the V2 → V3 load) | The V3 → V3 copy uses it to repair links the V2 load left as V2 ids. Use the **server's** copy, which is the newest. Never commit `.migration_state/`. |
| All models registered on the target gateway | backend team | A model the target lacks fails with `Model [x] is not registered`. The plan lists them. |

To run outside Airflow, put the same settings in `.env` as `AFYA_<KEY>_USERNAME / _PASSWORD /
_ORGANIZATION_ID / _FACILITY_ID / _URL_TEMPLATE / _MIGRATION_KEY` (git-ignored, never committed).

---

## 1. V2 → V3: load what V3 is missing, and explain what can't go in

1. **Reconcile.** Produce the summary CSV of V2 vs V3 counts per table, e.g.
   `output/org4_reload_20261015/4-reconcile-…-summary.csv`. A negative `difference` means V3 is short.
2. **Load the gaps from the live V2 API** (reads only rows V2 still has, posts only what V3 lacks):
   ```bash
   # dry run first: counts only, posts nothing
   python fast_v2_live_to_v3.py --live --dry-run --audit <reconcile.csv> --tables <table …>
   # then for real; --report writes per-table results (posted / held / refused + reasons)
   python fast_v2_live_to_v3.py --live --audit <reconcile.csv> --report output/<dir>/report_<name>.json \
          --workers 32 --tables <table …>
   ```
   The `--audit` gaps act as a guard: a table that would post more rows than V3 is short posts nothing,
   because the extras are already in V3 under other ids.
3. **Fill the `why` column** of the reconcile CSV from the reports:
   `output/org4_reload_20261015/write_why.py`.

Why rows end up **impossible** to load (from the Kisumu run):
- They belong to visits or lab results **V2 itself no longer has** (about 410k visit destinations and
  300k lab result details). That was most of the gap.
- **Price history.** V2 keeps every old price, V3 keeps one current price per procedure.
- **No V3 home** for the data (eyewear prescriptions, slit-lamp exams, IDSR symptoms, feeding charts …).
- **Incomplete** records (no admission, empty required fields) and **duplicates** already in V3.

---

## 2. V3 → V3: copy one organisation into another

DAG **`v3_to_v3_migration`** (or `python v3_to_v3_migration.py --source <src> --target <dst> <command>`):

| Step | Params | Done when |
|---|---|---|
| **plan** | `mode=plan`, `refresh_plan=true` | Green. A red plan lists every link rule problem; fix `v3_to_v3_links.py` first. |
| **canary** | `mode=migrate`, `record_limit=3` | Green. Its verify compares the copied rows field by field. |
| **full copy** | `mode=migrate`, `record_limit=0`, `workers=32`, `read_workers=8`, `skip_verify=true` | All tiers done. Check the logs for `stopped` / `refused`. |

Speed: the gateway takes one row per request, so `workers` (requests in flight per model) × the models
running at once (`V3V3_PARALLEL_MODELS`, default 4) is the throughput. About 150–200 rows/s per model is
what production sustained. If you see timeouts or `Connection reset`, lower `workers`/`read_workers`;
pushing harder only stalls the servers.

---

## 3. Audit the copy, reload what's missing, patch what differs

DAG **`v3_to_v3_audit`** (or `python v3_to_v3_audit.py --source <src> --target <dst> …`):

- **Full audit + repair:** `reload=true, sync=true, resume=false`. Reloads missing rows, relinks,
  compares every field, and patches differences.
- **Continue after an interruption, or after fixing something:** `resume=true`. Keeps every model already
  mirrored and redoes the rest.
- **Read-only check:** `reload=false, sync=false`.
- CLI extras: `--skip-reload --reload-only <models>` jumps straight to verify (reloading only the models
  named); `--heaviest-first` does the big tables first.

Output: `output/v3v3_audit/<src>-to-<dst>-audit-<time>.csv` (the CLI's `--out` sets the path). One row per
model: `source_rows, target_rows_before/after, posted_now, held, refused, still_missing, patched,
still_mismatched, facility_differs, mirror (yes / yes, except … / no / n/a), why`, plus a summary line.
It is rewritten after every model, so it's always current.

Repeat `resume=true` until the only models left are ones whose `why` says the source data is
incomplete. Those can't be loaded without inventing data.

---

## 4. What the code already handles (the reason each rule exists)

| Problem found | Where it's handled |
|---|---|
| Dates landed a day early, times 3 h early (gateway serialises UTC but stores what it's sent) | `to_wall_time` in `v3_to_v3_migration.py`: dates go out as Nairobi wall time (`V3V3_SOURCE_TZ`) |
| Links still holding **V2 ids** (invoice → visit 311001 …) | `V2_KEYED` in `v3_to_v3_links.py`, translated through the V2→V3 id map |
| ICD-10 variations / subcategories pointing at **another org's** copy of their parent | `LINK_BY_CODE`: by code prefix (A00.1 → A00) or by range (A00-A09 → A00-B99) |
| Core scheme pricing / exclusions belong to **core** schemes, not finance | `MODEL_LINKS["core/scheme_pricing"]`, `["core/scheme_exclusions"]` |
| Username / admission number / invoice no / code already used by **another org** (unique instance-wide) | `RENAME_ON_CLASH`: created as `<value>.<org>` |
| Record already in the target org under **another uuid** | `pair_by_key`: paired by its key, rewritten to the source's values |
| Required "who" column empty in the source (`created_by` …) | filled with the migration account, counted |
| Overloaded server answering 200 with **short pages** | `V3Client.read_all`: re-reads; raises rather than skip rows |
| `updated_at`-only differences (some services stamp their own) | counted, never patched |
| Fields the target computes (`visits.patient_full_name`) | `TARGET_COMPUTED`: reported only |
| One model's failure (network, refusal) | recorded in its CSV row; the run continues |
| Target down / rejecting everything | the model stops after 50 failures in a row; state is saved, re-run later |

---

## 5. Things only the backend team can fix (no SQL needed from us, but a code change from them)

- **Facility:** the gateway ignores `facility_id` on updates. Rows written with the wrong facility stay
  that way unless migration updates are allowed to set it.
- **uuid:** can't be changed on users (the only match key is uuid; `update` skips it as not fillable).
- **Models not registered** on the target, and inserts answered with a bare HTTP 500: the target's log
  has the exception under the `error_id` that the CSV shows.

---

## 6. Checks before committing changes to these pipelines

```bash
env/bin/python -m pytest tests/test_v3_to_v3_migration.py tests/test_v3_to_v3_scale.py tests/test_v3_to_v3_audit.py
docker exec -w /opt/airflow datapipelines-airflow-worker-1 python -c "
from airflow.models import DagBag
for f in ('v3_to_v3_migration_dag.py', 'v3_to_v3_audit_dag.py'):
    print(f, DagBag('/opt/airflow/dags/' + f, include_examples=False).import_errors or 'ok')"
```
Keep each file's line endings as committed (some are CRLF), or the diff fills with noise.
