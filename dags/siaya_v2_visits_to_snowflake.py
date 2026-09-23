# dags/siaya_v2_visits_to_snowflake.py
"""
V2 EMR (afyahmis / Ignite) Visit API -> Snowflake
HOSPITALS.SIAYA_MEDICAL_CAMP.SIAYA_MEDICAL_CAMP_COMPLETE_DATA

Converted from the standalone script siaya_v2_visits_to_snowflake.py at the
repo root. Same extraction/transform/upsert logic; only the orchestration
layer (CLI -> DAG, .env -> Airflow Connection/Variable, print -> log) changed.

BACKGROUND
  afyahmis.afyanalytics.ai is the Kisumu Specialists Hospital (KSH) v2 EMR --
  the same underlying facility already tracked as "kisumu" elsewhere in this
  repo (same sudo/newest!@ credentials, same clinics). It is NOT a dedicated
  "Siaya" instance: there is no Siaya clinic/region in the v2 system. The
  Siaya Medical Camp clinicians used the main KSH EMR (clinic_id=1) to log
  camp visits starting 2026-06-25. Everything before that date is unrelated
  KSH hospital traffic (~220k visits back to 2023) and must NOT be pulled in
  -- this DAG only ever looks at Visit rows with created_at >= SINCE_DATE.

  Because the volume of qualifying visits is a small, recent tail out of a
  ~220k-row table, extraction walks each namespace's LAST pages backwards
  (via `tail_scan`) rather than paginating from page 1 -- cheap even though
  the source table is huge.

KNOWN LIMITATION -- encrypted PII
  Patient first_name/middle_name/last_name/mobile/id_no come back as raw
  ciphertext through the /data/point endpoint (Laravel encrypted casts with
  no decryption key exposed to this API). PATIENT_NAME and PHONE_NUMBER are
  therefore always NULL for v2-sourced rows -- do not attempt to store the
  ciphertext bytes. sex/dob/patient_no and clinical-note fields are plain
  text and are populated normally. ATTENDING_CLINICIAN/CLINICIAN_NAME use
  the v2 username (Users has no name field at all).

IDEMPOTENCY (unchanged from the root script -- preserve exactly)
  Each row is tagged SOURCE_FILE = "v2_api:visit:<visit_id>". Every run
  deletes any existing rows with those SOURCE_FILE keys before inserting
  fresh ones, so reruns (e.g. picking up a doctor's note added after the
  first sync, or the daily schedule simply re-walking the tail) safely
  overwrite rather than duplicate. This is a delete+insert upsert keyed on
  SOURCE_FILE (== v2 visit id) -- functionally a MERGE on that natural key.

Airflow Connections required:
  siaya_v2_api   host=https://afyahmis.afyanalytics.ai
                 login=<v2 username>  password=<v2 password>
                 (root script used FACILITY_SIAYA_USERNAME/PASSWORD in .env)

Airflow Variables required:
  SIAYA_V2_SINCE_DATE   optional, default "2026-06-25" (YYYY-MM-DD).
                        Floor below which V2 visits are unrelated KSH
                        hospital traffic and must never be pulled in.

Env vars (from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE  SNOWFLAKE_ROLE
  SNOWFLAKE_PRIVATE_KEY_PATH  (or SNOWFLAKE_PASSWORD)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "siaya_v2_visits_to_snowflake"

V2_CONN_ID = "siaya_v2_api"
DEFAULT_SINCE = "2026-06-25"
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))

TARGET_DB = "HOSPITALS"
TARGET_SCHEMA = "SIAYA_MEDICAL_CAMP"
TARGET_TABLE = "SIAYA_MEDICAL_CAMP_COMPLETE_DATA"

# namespace strings confirmed against the live API -- several of these use
# non-obvious singular/plural forms the API accepts.
NS_VISIT = r"Ignite\Evaluation\Entities\Visit"
NS_PATIENT = r"Ignite\Reception\Entities\Patients"
NS_VITALS = r"Ignite\Evaluation\Entities\Vitals"
NS_DOCTORNOTES = r"Ignite\Evaluation\Entities\DoctorNotes"
NS_PRESCRIPT = r"Ignite\Evaluation\Entities\Prescriptions"
NS_INVEST = r"Ignite\Evaluation\Entities\Investigations"
NS_INVESTRES = r"Ignite\Evaluation\Entities\InvestigationResult"
NS_PROCEDURES = r"Ignite\Evaluation\Entities\Procedures"
NS_USERS = r"Ignite\Users\Entities\User"

TARGET_COLUMNS = [
    "RECORD_TYPE", "PATIENT_ID", "PATIENT_NAME", "PHONE_NUMBER", "SOURCE_FILE",
    "SEX", "AGE", "DATE_OF_BIRTH", "CAMP_FILE_NUMBER", "PATIENT_ID_FILE_NO",
    "ADDRESS", "VISIT_DATE", "TIME_IN", "TIME_OUT", "ATTENDING_CLINICIAN",
    "EMERGENCY_CONTACT", "BLOOD_PRESSURE", "HEART_RATE", "OXYGEN_SATURATION",
    "TEMPERATURE", "RESPIRATORY_RATE", "WEIGHT", "HEIGHT", "BODY_MASS_INDEX",
    "BLOOD_SUGAR", "NURSES_TRIAGE_NOTES", "CHIEF_COMPLAINTS",
    "HISTORY_OF_PRESENT_ILLNESS", "PAST_MEDICAL_SURGICAL_HISTORY",
    "KNOWN_ALLERGIES", "CURRENT_MEDICATIONS", "GENERAL_APPEARANCE",
    "SYSTEMIC_EXAM_CVS", "SYSTEMIC_EXAM_RESP", "SYSTEMIC_EXAM_GIT",
    "SYSTEMIC_EXAM_CNS", "SYSTEMIC_EXAM_MSK_SKIN", "PROVISIONAL_DIAGNOSIS",
    "LAB_IMAGING_INVESTIGATIONS_ORDERED", "TREATMENT_PLAN_PRESCRIPTIONS",
    "FOLLOW_UP_REFERRAL_NOTES", "DOCTORS_NOTES", "LAB_RESULTS", "RAW_NOTES",
    "DOCUMENT_TYPE", "DOCUMENT_DATE", "BABY_NAME", "GESTATIONAL_AGE",
    "AGE_OR_DAY_OF_LIFE", "BIRTH_WEIGHT", "CURRENT_WEIGHT", "DELIVERY_DETAILS",
    "APGAR_SCORES", "DIAGNOSES", "CLINICAL_SUMMARY", "VITALS",
    "EXAMINATION_FINDINGS", "INVESTIGATIONS", "TREATMENTS",
    "FEEDING_AND_SUPPORT", "DISCHARGE_PLAN", "CLINICIAN_NAME",
]


# ── V2 auth / HTTP ───────────────────────────────────────────────────────
_session: requests.Session | None = None
_token_cache: tuple[str, float] | None = None


def _http() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session


def _generate_token() -> str:
    conn = BaseHook.get_connection(V2_CONN_ID)
    base_url = conn.host.rstrip("/")
    r = _http().post(
        f"{base_url}/api/users/authenticate/user",
        json={"username": conn.login, "password": conn.password},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"V2 auth failed [{V2_CONN_ID}]: {r.status_code} · {r.text[:200]}")
    token = (r.json().get("success") or {}).get("token")
    if not token:
        raise RuntimeError(f"V2 token not found in auth response [{V2_CONN_ID}]")
    return token


def _token(invalidate: bool = False) -> str:
    global _token_cache
    if invalidate:
        _token_cache = None
    if _token_cache and (time.time() - _token_cache[1]) < TOKEN_TTL_SECONDS:
        return _token_cache[0]
    token = _generate_token()
    _token_cache = (token, time.time())
    return token


def _base_url() -> str:
    return BaseHook.get_connection(V2_CONN_ID).host.rstrip("/")


def _post(body: dict, *, max_retries: int = 6) -> dict:
    url = f"{_base_url()}/api/finance/access/data/point"
    attempt, wait = 0, 10
    while True:
        attempt += 1
        headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}
        try:
            r = _http().post(url, headers=headers, json=body, timeout=60)
        except (Timeout, ConnectionError) as e:
            if attempt >= max_retries:
                raise
            log.warning("network error %s -- retry in %ss (%s/%s)", e, wait, attempt, max_retries)
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue

        if r.status_code == 401 and attempt < max_retries:
            _token(invalidate=True)
            continue
        if r.status_code == 429 and attempt < max_retries:
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue
        if r.status_code in (500, 502, 503, 504) and attempt < max_retries:
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue

        r.raise_for_status()
        return r.json()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def tail_scan(namespace: str, cutoff: datetime, date_field: str = "created_at") -> list[dict]:
    """Walk a namespace's pages backward from the end, collecting rows whose
    date_field >= cutoff. Stops after two consecutive pages with no
    qualifying rows -- a cheap way to pull "the recent tail" out of a
    multi-hundred-thousand-row table without paginating through all of it.
    """
    first = _post({"namespace": namespace, "action": "get", "page": 1})
    last_page = (first.get("pagination") or {}).get("last_page", 1)

    qualifying: list[dict] = []
    empty_streak = 0
    page = last_page
    while page >= 1 and empty_streak < 2:
        payload = _post({"namespace": namespace, "action": "get", "page": page})
        rows = payload.get("data") or []
        hits = [row for row in rows if (dt := _parse_dt(row.get(date_field))) and dt >= cutoff]
        qualifying.extend(hits)
        empty_streak = empty_streak + 1 if not hits else 0
        page -= 1
    log.info("tail_scan %-45s -> %d rows since %s", namespace, len(qualifying), cutoff.date())
    return qualifying


def fetch_by_ids(namespace: str, ids: list[int], chunk_size: int = 100) -> dict[int, dict]:
    ids = sorted({i for i in ids if i is not None})
    out: dict[int, dict] = {}
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        payload = _post({"namespace": namespace, "action": "get", "page": 1, "ids": chunk})
        for row in payload.get("data") or []:
            out[row["id"]] = row
    return out


# ── field helpers ────────────────────────────────────────────────────────
def _join(*parts: str | None, sep: str = "; ") -> str | None:
    vals = [p.strip() for p in parts if p and str(p).strip()]
    return sep.join(vals) if vals else None


def _fmt(value: Any, suffix: str = "") -> str | None:
    if value is None or value == "":
        return None
    return f"{value}{suffix}"


def _latest_by(rows: list[dict], key_field: str) -> dict[Any, dict]:
    """Reduce a list of rows to the most-recent one per key_field value."""
    out: dict[Any, dict] = {}
    for row in rows:
        k = row.get(key_field)
        if k is None:
            continue
        prev = out.get(k)
        if prev is None or (row.get("id") or 0) > (prev.get("id") or 0):
            out[k] = row
    return out


def _group_by(rows: list[dict], key_field: str) -> dict[Any, list[dict]]:
    out: dict[Any, list[dict]] = {}
    for row in rows:
        k = row.get(key_field)
        if k is None:
            continue
        out.setdefault(k, []).append(row)
    return out


def build_row(
    visit: dict,
    patient: dict | None,
    vitals: dict | None,
    note: dict | None,
    investigations: list[dict],
    invest_results: list[dict],
    prescriptions: list[dict],
    procedure_names: dict[int, str],
    usernames: dict[int, str],
) -> dict:
    visit_id = visit["id"]
    created = _parse_dt(visit.get("created_at"))

    clinician = None
    for uid in (note and note.get("user"), *(inv.get("performing_doctor") for inv in investigations), visit.get("user")):
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            continue
        if uid_int in usernames:
            clinician = usernames[uid_int]
            break

    bp = _join(_fmt(vitals and vitals.get("bp_systolic")), _fmt(vitals and vitals.get("bp_diastolic")), sep="/")
    bp = f"{bp} mmHg" if bp else None
    hr = _fmt(vitals and vitals.get("pulse"), " bpm")
    spo2 = _fmt(vitals and vitals.get("oxygen"), " %")
    temp = _fmt(vitals and vitals.get("temperature"), " °C")
    resp = _fmt(vitals and vitals.get("respiration"), " breaths/min")
    wt = _fmt(vitals and vitals.get("weight"), " kg")
    ht = _fmt(vitals and vitals.get("height"), " cm")
    sugar = _join(_fmt(vitals and vitals.get("blood_sugar")), vitals and vitals.get("blood_sugar_units"), sep=" ")

    vitals_summary = _join(
        _fmt(bp, ""), _fmt(hr, ""), _fmt(temp, ""), _fmt(resp, ""), _fmt(spo2, ""),
        _fmt(wt, ""), _fmt(ht, ""), sep=", ",
    )

    proc_names = _join(*(procedure_names.get(inv.get("procedure")) for inv in investigations), sep=", ")
    rx_text = _join(*(
        _join(rx.get("drug_name"), rx.get("dosage"), rx.get("frequency"), rx.get("duration"), sep=" ")
        for rx in prescriptions
    ), sep="; ")
    treatment_plan = _join(note and note.get("treatment_plan"), rx_text, sep="; ")
    lab_results = _join(*(ir.get("results") for ir in invest_results), sep="; ")

    doctors_notes = _join(
        note and note.get("presenting_complaints"),
        note and note.get("examination"),
        note and note.get("diagnosis"),
        note and note.get("treatment_plan"),
        note and note.get("remarks"),
        sep="\n",
    )

    diagnosis = _join(note and note.get("diagnosis"), note and note.get("mohDiagnosis"), sep="; ")

    return {
        "RECORD_TYPE": "V2_EMR_VISIT",
        "PATIENT_ID": f"V2-P{visit.get('patient')}" if visit.get("patient") else None,
        "PATIENT_NAME": None,  # encrypted at source -- see module docstring
        "PHONE_NUMBER": None,  # encrypted at source -- see module docstring
        "SOURCE_FILE": f"v2_api:visit:{visit_id}",
        "SEX": patient and patient.get("sex"),
        "AGE": None,
        "DATE_OF_BIRTH": patient and patient.get("dob"),
        "CAMP_FILE_NUMBER": None,
        "PATIENT_ID_FILE_NO": str(patient.get("patient_no")) if patient and patient.get("patient_no") is not None else None,
        "ADDRESS": patient and patient.get("address"),
        "VISIT_DATE": created.strftime("%Y-%m-%d") if created else None,
        "TIME_IN": created.strftime("%H:%M:%S") if created else None,
        "TIME_OUT": None,
        "ATTENDING_CLINICIAN": clinician,
        "EMERGENCY_CONTACT": None,
        "BLOOD_PRESSURE": bp,
        "HEART_RATE": hr,
        "OXYGEN_SATURATION": spo2,
        "TEMPERATURE": temp,
        "RESPIRATORY_RATE": resp,
        "WEIGHT": wt,
        "HEIGHT": ht,
        "BODY_MASS_INDEX": _fmt(vitals and vitals.get("bmi")),
        "BLOOD_SUGAR": sugar,
        "NURSES_TRIAGE_NOTES": vitals and vitals.get("nurse_notes"),
        "CHIEF_COMPLAINTS": _join(note and note.get("chief_complaints"), note and note.get("presenting_complaints"), sep=" / "),
        "HISTORY_OF_PRESENT_ILLNESS": note and note.get("presenting_complaints"),
        "PAST_MEDICAL_SURGICAL_HISTORY": note and note.get("past_medical_history"),
        "KNOWN_ALLERGIES": vitals and vitals.get("allergies"),
        "CURRENT_MEDICATIONS": vitals and vitals.get("current_medication"),
        "GENERAL_APPEARANCE": None,
        "SYSTEMIC_EXAM_CVS": None,
        "SYSTEMIC_EXAM_RESP": None,
        "SYSTEMIC_EXAM_GIT": None,
        "SYSTEMIC_EXAM_CNS": None,
        "SYSTEMIC_EXAM_MSK_SKIN": None,
        "PROVISIONAL_DIAGNOSIS": diagnosis,
        "LAB_IMAGING_INVESTIGATIONS_ORDERED": proc_names,
        "TREATMENT_PLAN_PRESCRIPTIONS": treatment_plan,
        "FOLLOW_UP_REFERRAL_NOTES": _join(note and note.get("next_steps"), note and note.get("refereed_to"), sep="; "),
        "DOCTORS_NOTES": doctors_notes,
        "LAB_RESULTS": lab_results,
        "RAW_NOTES": None,
        "DOCUMENT_TYPE": None,
        "DOCUMENT_DATE": None,
        "BABY_NAME": None,
        "GESTATIONAL_AGE": None,
        "AGE_OR_DAY_OF_LIFE": None,
        "BIRTH_WEIGHT": None,
        "CURRENT_WEIGHT": None,
        "DELIVERY_DETAILS": None,
        "APGAR_SCORES": None,
        "DIAGNOSES": diagnosis,
        "CLINICAL_SUMMARY": doctors_notes,
        "VITALS": vitals_summary,
        "EXAMINATION_FINDINGS": note and note.get("examination"),
        "INVESTIGATIONS": proc_names,
        "TREATMENTS": treatment_plan,
        "FEEDING_AND_SUPPORT": None,
        "DISCHARGE_PLAN": None,
        "CLINICIAN_NAME": clinician,
    }


# ── Snowflake ─────────────────────────────────────────────────────────────
class SnowflakeClient:
    """Key-pair (or password) auth client scoped to the fixed
    HOSPITALS.SIAYA_MEDICAL_CAMP target -- this DAG writes to one table, not
    a per-facility schema, so database/schema are not parameterised.
    """

    def __init__(self):
        missing = [v for v in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE") if not os.getenv(v)]
        if missing:
            raise RuntimeError(f"Missing Snowflake env vars: {', '.join(missing)}")

        kwargs: dict[str, Any] = dict(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            role=(os.getenv("SNOWFLAKE_ROLE") or "").strip() or None,
            database=TARGET_DB,
            schema=TARGET_SCHEMA,
        )
        pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
        pwd = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
        if pk_path:
            kwargs["private_key_file"] = pk_path
        elif pwd:
            kwargs["password"] = pwd
        else:
            raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")

        self._conn = snowflake.connector.connect(**kwargs)

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def upsert_rows(rows: list[dict]) -> None:
    """DELETE any existing rows for these SOURCE_FILE keys, then INSERT the
    freshly-built rows, in one transaction. SOURCE_FILE == v2 visit id is the
    natural key, so this is a delete+insert upsert: reruns (e.g. to pick up a
    doctor's note filed after the first sync) overwrite rather than
    duplicate. Preserved unchanged from the root script.
    """
    if not rows:
        log.info("nothing to write")
        return

    source_files = [r["SOURCE_FILE"] for r in rows]
    with SnowflakeClient() as sf:
        cur = sf._conn.cursor()
        try:
            placeholders = ", ".join(["%s"] * len(source_files))
            cur.execute(
                f"DELETE FROM {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE} "
                f"WHERE SOURCE_FILE IN ({placeholders})",
                source_files,
            )
            log.info("cleared %d pre-existing row(s) for these visits (idempotent rerun)", cur.rowcount)

            col_list = ", ".join(TARGET_COLUMNS)
            val_placeholders = ", ".join(["%s"] * len(TARGET_COLUMNS))
            values = [[row[c] for c in TARGET_COLUMNS] for row in rows]
            cur.executemany(
                f"INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE} ({col_list}) "
                f"VALUES ({val_placeholders})",
                values,
            )
            sf._conn.commit()
            log.info("inserted %d row(s) into %s.%s.%s", len(rows), TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)
        finally:
            cur.close()


# ── DAG task callables ───────────────────────────────────────────────────
def extract_transform_v2_visits(**context) -> list[dict]:
    """Tail-scan the V2 visit tail since SIAYA_V2_SINCE_DATE, join in
    patient/vitals/notes/investigations/prescriptions/users, and build the
    SIAYA_MEDICAL_CAMP_COMPLETE_DATA rows. Returns the row list for the load
    task via XCom (the qualifying tail is small, per the module docstring).
    """
    since_str = Variable.get("SIAYA_V2_SINCE_DATE", default_var=DEFAULT_SINCE)
    since = datetime.strptime(since_str, "%Y-%m-%d")

    visits = tail_scan(NS_VISIT, since)
    visits.sort(key=lambda v: v["id"])
    if not visits:
        log.info("no v2 visits found since %s", since.date())
        return []

    patient_ids = [v.get("patient") for v in visits]

    patients = fetch_by_ids(NS_PATIENT, patient_ids)
    vitals_by_visit = _latest_by(tail_scan(NS_VITALS, since), "visit")
    notes_by_visit = _latest_by(tail_scan(NS_DOCTORNOTES, since), "visit")
    inv_by_visit = _group_by(tail_scan(NS_INVEST, since), "visit")
    ir_by_visit = _group_by(tail_scan(NS_INVESTRES, since), "visit_id")
    rx_by_visit = _group_by(tail_scan(NS_PRESCRIPT, since), "visit")

    proc_ids = {inv.get("procedure") for invs in inv_by_visit.values() for inv in invs}
    procedures = fetch_by_ids(NS_PROCEDURES, list(proc_ids))
    procedure_names = {pid: p.get("name") for pid, p in procedures.items()}

    user_ids: set[int] = set()
    for v in visits:
        user_ids.add(v.get("user"))
    for n in notes_by_visit.values():
        user_ids.add(n.get("user"))
    for invs in inv_by_visit.values():
        for inv in invs:
            try:
                user_ids.add(int(inv.get("performing_doctor")))
            except (TypeError, ValueError):
                pass
    users = fetch_by_ids(NS_USERS, [u for u in user_ids if u is not None])
    usernames = {uid: u.get("username") for uid, u in users.items()}

    rows = []
    for visit in visits:
        vid = visit["id"]
        rows.append(build_row(
            visit=visit,
            patient=patients.get(visit.get("patient")),
            vitals=vitals_by_visit.get(vid),
            note=notes_by_visit.get(vid),
            investigations=inv_by_visit.get(vid, []),
            invest_results=ir_by_visit.get(vid, []),
            prescriptions=rx_by_visit.get(vid, []),
            procedure_names=procedure_names,
            usernames=usernames,
        ))

    log.info("built %d row(s) since %s", len(rows), since.date())
    return rows


def load_v2_visits_to_snowflake(**context):
    """Delete+insert upsert the extracted rows into
    HOSPITALS.SIAYA_MEDICAL_CAMP.SIAYA_MEDICAL_CAMP_COMPLETE_DATA."""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_transform_v2_visits") or []
    upsert_rows(rows)


# ── DAG definition ───────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
    tags=["siaya", "v2", "visits", "snowflake", "ingest"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_transform_v2_visits",
        python_callable=extract_transform_v2_visits,
    )
    t_load = PythonOperator(
        task_id="load_v2_visits_to_snowflake",
        python_callable=load_v2_visits_to_snowflake,
    )

    t_extract >> t_load
