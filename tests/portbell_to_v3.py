#!/usr/bin/env python3
"""
prepare_opd_for_v3.py
=====================
Turns a hand-typed OPD register (Excel/CSV) into clean, V3-shaped CSV tables
plus a review report.  It does NOT post anything to V3.

INPUT COLUMNS (header names are matched loosely, order does not matter)
    DATE, SERIAL NO., OPD NO., NEW/REVISIT, PATIENT NAME, AGE, SEX, RESIDENCE,
    ID.NO, CONTACT, OCCUPATION, TEMP, WEIGHT, BP, DIAGNOSIS, TREATMENT,
    REMARKS, MODE OF PAYMENT

OUTPUT (in --out-dir)
    patients.csv             one row per unique patient (duplicates merged)
    visits.csv               one row per OPD visit, linked by patient_ref
    vitals.csv               temperature / weight / BP per visit
    doctor_notes.csv         diagnosis -> assessment, treatment -> plan
    prescriptions.csv        one row per drug item (with route when written)
    procedures.csv           one row per procedure (XLA, suturing, filling...)
    review.csv               every problem found, with original value + action
    possible_duplicates.csv  patient pairs that look alike but were NOT merged
    patient_merges.csv       patients merged from several rows (verify these)
    treatment_vocab.csv      every distinct drug/procedure string + count
    rejected_rows.csv        rows that could not be used at all
    (with --facility NAME)   .migration_state/NAME/*.json, v2_shaped_NAME.json,
                             facility_config_NAME.txt

USAGE
    python prepare_opd_for_v3.py register.xlsx --out-dir v3_ready \
        --organization-id 4 --facility-id 4
    python prepare_opd_for_v3.py register.csv  --sheet "Sheet1"

The V3 column names below are best guesses taken from the datapipelines repo
(v2_to_v3_api_migration.py).  Rename them in V3_COLUMN_MAP once you have the
real V3 field list; nothing else needs to change.
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Edit these if V3 uses different names.  {table: {our_column: v3_column}}
# --------------------------------------------------------------------------
V3_COLUMN_MAP: dict[str, dict[str, str]] = {
    "patients": {},       # e.g. {"phone": "mobile", "dob_estimated": "dob"}
    "visits": {},
    "vitals": {},
    "doctor_notes": {},
    "prescriptions": {},
    "procedures": {},
}

EXPECTED = {
    "DATE": "date", "SERIALNO": "serial", "OPDNO": "opd_no",
    "NEWREVISIT": "new_revisit", "PATIENTNAME": "name", "AGE": "age",
    "SEX": "sex", "RESIDENCE": "residence", "IDNO": "id_no",
    "CONTACT": "contact", "OCCUPATION": "occupation", "TEMP": "temp",
    "WEIGHT": "weight", "BP": "bp", "DIAGNOSIS": "diagnosis",
    "TREATMENT": "treatment", "REMARKS": "remarks",
    "MODEOFPAYMENT": "payment",
}

# --------------------------------------------------------------------------
# Spelling dictionaries (my reading of the register - review and extend)
# --------------------------------------------------------------------------
DX_FIXES = [
    (r"TONS[IO]LITIS", "TONSILLITIS"), (r"MIGRANE", "MIGRAINE"),
    (r"CARRIES", "CARIES"), (r"REASSUARANCE", "REASSURANCE"),
    (r"ATHRITI[SA]|ARTHRITIA", "ARTHRITIS"), (r"ALERGIC", "ALLERGIC"),
    (r"SEPTICE?AMIA", "SEPTICAEMIA"), (r"HAEMARRIODS|HEMORRIDS", "HAEMORRHOIDS"),
    (r"ASMATIC", "ASTHMATIC"), (r"\bPLURAL\b", "PLEURAL"),
    (r"PHARINGITIS", "PHARYNGITIS"), (r"PHARA?G[A]?EAL|PHARAGAEL", "PHARYNGEAL"),
    (r"MONITARING", "MONITORING"), (r"POISOINING", "POISONING"),
    (r"GIGIVITIES", "GINGIVITIS"), (r"RHEMATOID", "RHEUMATOID"),
    (r"UREATHRITIS", "URETHRITIS"), (r"GONN?ORRHOEA", "GONORRHOEA"),
    (r"RHNITIS", "RHINITIS"), (r"AXIETY", "ANXIETY"),
    (r"DIPPRESION", "DEPRESSION"), (r"COSTIPATION", "CONSTIPATION"),
    (r"SPONDOLYSIS", "SPONDYLOSIS"), (r"DERMITIA|DERMITIS", "DERMATITIS"),
    (r"TENDONITIS", "TENDINITIS"), (r"PYRONACHIA", "PARONYCHIA"),
    (r"PYRANOXIA", "PYREXIA"), (r"DISTOLICHYPOTENSION", "DIASTOLIC HYPOTENSION"),
    (r"BLEEDINGTHREATED", "BLEEDING, THREATENED"), (r"\bREPLACMENT\b", "REPLACEMENT"),
    (r"POSTURAL DIZNES", "POSTURAL DIZZINESS"), (r"SORE TONGUE", "SORE TONGUE"),
]
DX_EXPAND_PREFIX = [(r"^V\s*CANDIDIASIS", "VAGINAL CANDIDIASIS")]

DRUG_FIXES = {
    "PCM": "PARACETAMOL", "MTZ": "METRONIDAZOLE", "ABZ": "ALBENDAZOLE",
    "CIPLOFLOXIN": "CIPROFLOXACIN", "CIPLOFLAXIN": "CIPROFLOXACIN",
    "CIPLOFLOXICIN": "CIPROFLOXACIN", "CIPRO": "CIPROFLOXACIN",
    "CENTRIZIN": "CETIRIZINE", "CETRIZEN": "CETIRIZINE", "CENTRIZEN": "CETIRIZINE",
    "FLUCONAXOLE": "FLUCONAZOLE", "FLUCANAZOLE": "FLUCONAZOLE",
    "CLANTHROMYCIN": "CLARITHROMYCIN", "CLATHROMYCIN": "CLARITHROMYCIN",
    "TININDAZOLE": "TINIDAZOLE", "METROTIDAZOLE": "METRONIDAZOLE",
    "IBRUFEN": "BRUFEN", "BRUPHEN": "BRUFEN", "FLAGLY": "FLAGYL",
    "AMOXCLAV": "AMOXICLAV", "AMOXYCLAV": "AMOXICLAV", "AMOXICLAVE": "AMOXICLAV",
    "DECLOFENAC": "DICLOFENAC", "DICLO": "DICLOFENAC",
    "FLUCLOX": "FLUCLOXACILLIN", "FLUCHROX": "FLUCLOXACILLIN",
    "FLUXOXACCILIN": "FLUCLOXACILLIN", "FLUCOXACIN": "FLUCLOXACILLIN",
    "CEFRIAXONE": "CEFTRIAXONE", "CEFRIUXONE": "CEFTRIAXONE",
    "CEFRIOXONE": "CEFTRIAXONE", "CEFRIXOME": "CEFTRIAXONE",
    "CEFURIOAXONE": "CEFTRIAXONE", "CEFORAXONE": "CEFTRIAXONE",
    "CEFRUAXIONE": "CEFTRIAXONE", "CEFURIXONE": "CEFTRIAXONE",
    "CEFURIXONE": "CEFTRIAXONE", "CEFROXOME": "CEFUROXIME",
    "CEFLOROXIME": "CEFUROXIME", "GENTA": "GENTAMICIN", "GENT": "GENTAMICIN",
    "NIFELAT": "NIFEDIPINE", "NIFEDINE": "NIFEDIPINE", "NIREDIPINE": "NIFEDIPINE",
    "ATORASTATIN": "ATORVASTATIN", "ASPRING": "ASPIRIN", "LOSIX": "LASIX",
    "NEBILIZE": "NEBULIZATION", "TRAMEXAMIC": "TRANEXAMIC",
    "HYDROCOTISEN": "HYDROCORTISONE", "HYDROCARTSON": "HYDROCORTISONE",
    "HYDROCORTISEN": "HYDROCORTISONE", "HYDROCAT": "HYDROCORTISONE",
    "HYDROCORT": "HYDROCORTISONE", "H.CORT": "HYDROCORTISONE",
    "REASSUARANCE": "REASSURANCE",
}

PROC_KEYS = [
    "XLA", "FILLING", "SUTUR", "STINCH", "STITCH", "DRESSING", "DRAINED",
    "REMOVED", "RMOVED", "SYRINGING", "COD DONE", "NEBULI", "NEBILI", "U/S",
    "PSYCHOLOGY", "REASSUR", "MONITOR", "REPLACEMENT", "REFIED", "REFIX",
    "LIGATION",
]

OCC_MAP = {
    "B.MAN": "BUSINESSMAN", "B MAN": "BUSINESSMAN", "B.LADY": "BUSINESSWOMAN",
    "C.LABOUR": "CASUAL LABOURER", "C.LABOR": "CASUAL LABOURER",
    "C LABOURER": "CASUAL LABOURER", "C.LABOURER": "CASUAL LABOURER",
    "FARMING": "FARMER", "FARMER": "FARMER",
}
RES_FIX = {"KANYAGA": "KIANYAGA", "KINYAGA": "KIANYAGA", "KIANYAGA ": "KIANYAGA"}

FEMALE_ONLY = ["DEPO", "PREG", "PID", "ABORTION", "PG TEST", "MENORRH",
               "OVARIAN", "VAGINITIS", "VAGINAL", "FIBROID", "HG", "HYPEREMESIS"]
MALE_ONLY = ["BPH", "PROSTAT"]


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def s(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()


def norm_header(h) -> str:
    return re.sub(r"[^A-Z0-9]", "", s(h).upper())


def parse_date(raw: str):
    raw = s(raw)
    if not raw:
        return None, ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            return pd.Timestamp(int(m[1]), int(m[2]), int(m[3])), "iso"
        except ValueError:
            return None, ""
    # day-first; "23/82026" (missing slash) is handled by backtracking
    m = re.match(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-]?(\d{4}|\d{2})$", raw)
    if not m:
        return None, ""
    d, mo, y = int(m[1]), int(m[2]), int(m[3])
    y += 2000 if y < 100 else 0
    try:
        return pd.Timestamp(y, mo, d), ""
    except ValueError:
        return None, ""


def parse_age(raw: str):
    """'5.9' means 5 years 9 months in this register.  -> (years, months, issue)"""
    raw = s(raw)
    if not raw:
        return None, 0, "age missing"
    m = re.fullmatch(r"(\d{1,3})(?:\.(\d{1,2}))?", raw)
    if not m:
        return None, 0, f"age not understood: {raw!r}"
    years = int(m[1])
    months = int(m[2]) if m[2] else 0
    if months > 11:
        return years, 0, f"age months part >11 ({raw}); months ignored"
    if years > 110:
        return None, 0, f"implausible age {raw}"
    return years, months, ""


def clean_phone(raw: str):
    d = re.sub(r"\D", "", s(raw))
    if not d:
        return "", "", ""
    if d.startswith("254") and len(d) == 12:
        d = "0" + d[3:]
    elif len(d) == 9 and d[0] in "71":
        d = "0" + d          # leading zero lost in Excel
    if len(d) == 10 and d[0] == "0" and d[1] in "71":
        return d, "+254" + d[1:], ""
    return "", "", f"phone not valid ({raw})"


def clean_id(raw: str):
    v = re.sub(r"\s+", "", s(raw)).upper()
    if not v:
        return "", "", ""
    if re.fullmatch(r"[A-Z]{1,2}\d{6,9}", v):
        return v, "passport", ""
    if re.fullmatch(r"\d{6,8}", v):
        return v, "national_id", ""
    why = "looks like a phone number" if re.fullmatch(r"[71]\d{8}", v) else "wrong length"
    return "", "", f"ID not valid ({raw}): {why}"


def norm_name(raw: str) -> str:
    t = re.sub(r"[^A-Za-z' ]", " ", s(raw)).upper()
    return re.sub(r"\s+", " ", t).strip()


def split_name(full: str):
    toks = full.split()
    if len(toks) == 1:
        return toks[0].title(), "", True
    return toks[0].title(), " ".join(toks[1:]).title(), False


def name_sim(a: str, b: str) -> float:
    ta, tb = a.split(), b.split()
    sa, sb = " ".join(sorted(ta)), " ".join(sorted(tb))
    r = difflib.SequenceMatcher(None, sa, sb).ratio()
    short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if len(short) >= 2 and set(short) <= set(long_):
        r = max(r, 0.85)
    return r


def clean_diagnosis(raw: str):
    t = s(raw).upper()
    if not t:
        return [], ""
    uncertain = "?" in t
    t = t.replace("?", " ")
    t = re.sub(r"^#\s*", "FRACTURE ", t)
    t = re.sub(r"\b(?:[A-Z]\.){2,}[A-Z]\b", lambda m: m[0].replace(".", ""), t)   # U.T.I
    t = re.sub(r"\b([A-Z])\.([A-Z])\b", r"\1\2", t)                               # O.A
    t = re.sub(r"^A\.\s*", "ACUTE ", t)
    t = re.sub(r"(?<=[A-Z]{3})\.(?=[A-Z]{3})", ",", t)                            # X.Y -> X,Y
    for pat, rep in DX_FIXES:
        t = re.sub(pat, rep, t)
    t = re.sub(r"\s*\.{2,}\s*", ",", t)
    t = re.sub(r"\s*,+\s*\.*\s*", ",", t)
    parts = []
    for p in t.split(","):
        p = re.sub(r"\s+", " ", p.strip(" ."))
        for pat, rep in DX_EXPAND_PREFIX:
            p = re.sub(pat, rep, p)
        if p:
            parts.append(p)
    return parts, ("uncertain (?) in original" if uncertain else "")


ROUTE_RE = r"(?:I\.?V|I\.?M|PO)"


def parse_treatment(raw: str):
    """-> list of (kind, route, text)   kind in {'drug','procedure'}"""
    t = s(raw).upper()
    if not t:
        return []
    # split run-together items at route markers ("I.V XPEN I.V H.CORT")
    t = re.sub(rf"(?<=\S)\s+(?={ROUTE_RE}(?=[\s.,]|$))", ",", t)
    items = [p.strip(" .") for p in re.split(r"\s*,+\s*\.*\s*|\s*\.{2,}\s*", t)]
    out, pending = [], ""
    for it in items:
        if not it:
            continue
        if re.fullmatch(ROUTE_RE, it):
            pending = it.replace(".", "")
            continue
        route = pending
        m = re.match(rf"^({ROUTE_RE})\.?\s+(.*)$", it)
        if m:
            route, it = m[1].replace(".", ""), m[2]
        m = re.match(rf"^(.*?)\s+({ROUTE_RE})$", it)
        if m:
            it, route = m[1], m[2].replace(".", "")
        pending = ""
        it = re.sub(r"^V\s*\.?\s*PESSARIES", "VAGINAL PESSARIES", it)
        words = [DRUG_FIXES.get(w, w) for w in it.split()]
        it = " ".join(words).strip(" .")
        if not it:
            continue
        if re.search("|".join(re.escape(k) for k in PROC_KEYS), it):
            if it.startswith("XLA"):
                it = "TOOTH EXTRACTION (XLA)"
            out.append(("procedure", "", it))
        else:
            out.append(("drug", route, it))
    return out


def longest_nondecreasing(vals):
    """indices of one longest non-decreasing subsequence (O(n log n))."""
    tails, tails_idx, parent = [], [], [-1] * len(vals)
    for i, v in enumerate(vals):
        k = bisect_right(tails, v)
        if k == len(tails):
            tails.append(v)
            tails_idx.append(i)
        else:
            tails[k] = v
            tails_idx[k] = i
        parent[i] = tails_idx[k - 1] if k else -1
    keep, i = set(), tails_idx[-1] if tails_idx else -1
    while i != -1:
        keep.add(i)
        i = parent[i]
    return keep


class UF:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[max(ra, rb)] = min(ra, rb)


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------
def read_register(path: Path, sheet):
    if path.suffix.lower() in (".csv", ".tsv"):
        raw = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                          sep="\t" if path.suffix.lower() == ".tsv" else ",")
    else:
        raw = pd.read_excel(path, header=None, dtype=str, sheet_name=sheet or 0)
    hdr = None
    for i in range(min(15, len(raw))):
        if "PATIENTNAME" in [norm_header(c) for c in raw.iloc[i]]:
            hdr = i
            break
    if hdr is None:
        sys.exit("Could not find a header row containing 'PATIENT NAME'.")
    cols = [norm_header(c) for c in raw.iloc[hdr]]
    missing = [k for k in EXPECTED if k not in cols]
    if missing:
        sys.exit(f"Missing expected columns: {missing}")
    df = raw.iloc[hdr + 1:].copy()
    df.columns = cols
    df = df[list(EXPECTED)].rename(columns=EXPECTED)
    df["source_row"] = range(hdr + 2, hdr + 2 + len(df))   # Excel row number
    return df.reset_index(drop=True)



# --------------------------------------------------------------------------
# Migration state folder + V2-shaped records  (FORMATS ARE ASSUMED)
# --------------------------------------------------------------------------
# The datapipelines repo keeps per-facility checkpoint files in
# .migration_state/<facility>/.  I could not see how the repo reads them, so
# the structures below are my best guess.  Compare with a real kisumu file
# and adjust STATE_FILES / write_state() before pointing the repo at them.
V2_NS = {
    "patients": r"Ignite\Reception\Entities\Patients",
    "visits": r"Ignite\Evaluation\Entities\Visit",
    "doctor_notes": r"Ignite\Evaluation\Entities\DoctorNotes",
    "vitals": r"Ignite\Evaluation\Entities\Vitals",
}
STATE_FILES = {                     # file name -> initial content
    ".migration_done.json": [],             # finished jobs (none yet)
    ".migration_id_map.json": {},           # {namespace: {v2_id: v3_id}} filled while posting
    ".migration_id_to_uuid.json": {},
    ".migration_record_progress.json": {},
    ".migration_visit_admission.json": {},  # outpatient register: no admissions
}


def write_state(out: Path, facility: str, patients, visits, vitals, notes, org, fac):
    """Placeholder V2-style ids (1..n) + state folder + V2-shaped records."""
    pid = {p["patient_ref"]: i for i, p in enumerate(patients, 1)}
    vid = {v["visit_ref"]: i for i, v in enumerate(visits, 1)}
    state = out / ".migration_state" / facility
    state.mkdir(parents=True, exist_ok=True)
    for name, content in STATE_FILES.items():
        (state / name).write_text(json.dumps(content, indent=2), encoding="utf-8")
    visit_patient = {str(vid[v["visit_ref"]]): pid[v["patient_ref"]] for v in visits}
    (state / ".migration_visit_patient.json").write_text(
        json.dumps(visit_patient, indent=2), encoding="utf-8")

    def clean(d):
        return {k: v for k, v in d.items() if v not in ("", None)}
    v2 = {
        V2_NS["patients"]: [clean({
            "id": pid[p["patient_ref"]], "first_name": p["first_name"], "last_name": p["last_name"],
            "sex": {"male": "M", "female": "F"}.get(p["gender"], ""),
            "dob": p["dob_estimated"], "mobile": p["phone"], "id_number": p["national_id"],
            "occupation": p["occupation"], "residence": p["residence"],
            "created_at": p["first_visit_date"]}) for p in patients],
        V2_NS["visits"]: [clean({
            "id": vid[v["visit_ref"]], "patient": pid[v["patient_ref"]], "type": "outpatient",
            "visit_number": v["opd_no"], "created_at": v["visit_date"],
            "payment_mode": v["payment_mode"]}) for v in visits],
        V2_NS["doctor_notes"]: [clean({
            "visit": vid[n["visit_ref"]], "patient": pid[n["patient_ref"]],
            "diagnosis": n["assessment"], "treatment": n["plan"]}) for n in notes
            if n["assessment"] or n["plan"]],
        V2_NS["vitals"]: [clean({
            "visit": vid[x["visit_ref"]], "temperature": x["temperature"], "weight": x["weight"],
            "bp_systolic": x["systolic_bp"], "bp_diastolic": x["diastolic_bp"]}) for x in vitals],
    }
    (out / f"v2_shaped_{facility}.json").write_text(
        json.dumps(v2, indent=2, default=str), encoding="utf-8")
    (out / f"facility_config_{facility}.txt").write_text(
        "# Add to the repo's v2_to_v3_api_migration.py (ask the V3 admin for the ids)\n"
        f'FACILITY_V3_CONFIG["{facility}"] = {{"organization_id": {org or "None"}, '
        f'"facility_id": {fac or "None"}, "application_id": 1}}\n'
        "# .env (only if the facility has its own V3 URLs):\n"
        f"# AFYA_{facility.upper()}_ORGANIZATION_ID={org or ''}\n"
        f"# AFYA_{facility.upper()}_FACILITY_ID={fac or ''}\n", encoding="utf-8")
    return state

# --------------------------------------------------------------------------
# main pipeline
# --------------------------------------------------------------------------
def run(args):
    if args.mapping:
        spec = json.loads(Path(args.mapping).read_text(encoding="utf-8"))
        for table, renames in spec.get("column_map", {}).items():
            if isinstance(renames, dict) and table in V3_COLUMN_MAP:
                V3_COLUMN_MAP[table].update(renames)
    df = read_register(Path(args.input), args.sheet)
    review, rejected = [], []

    def flag(row, field, issue, original="", action="kept", level="warn"):
        review.append({"source_row": row["source_row"], "opd_no": row.get("opd_no", ""),
                       "patient": row.get("name", ""), "level": level,
                       "field": field, "issue": issue, "original": original,
                       "action": action})

    recs = []
    iso_dates = 0
    for _, r in df.iterrows():
        row = {k: s(v) for k, v in r.items()}
        row["source_row"] = r["source_row"]
        if not row["name"]:
            if any(row[k] for k in ("diagnosis", "treatment", "age", "contact")):
                rejected.append({**row, "reason": "no patient name but other data present"})
            elif row["serial"] or row["opd_no"]:
                rejected.append({**row, "reason": "empty slot (serial/OPD number only)"})
            continue
        dt, how = parse_date(row["date"])
        if dt is None:
            rejected.append({**row, "reason": f"date missing/unreadable ({row['date']!r})"})
            continue
        if how == "iso":
            iso_dates += 1
        if row["date"] and not how and not re.fullmatch(r"\d{1,2}/\d{1,2}/(\d{2}|\d{4})", row["date"]):
            flag(row, "date", "unusual date format repaired", row["date"], f"read as {dt.date()}")
        rec = dict(row)
        rec["date"] = dt
        recs.append(rec)

    if iso_dates:
        print(f"WARNING: {iso_dates} dates arrived as real Excel dates. If the sheet was "
              "typed as dd/mm, Excel may have swapped day and month on some of them. "
              "Check review.csv 'date out of sequence' rows.", file=sys.stderr)

    # ---- sequence checks -------------------------------------------------
    ords = [r["date"].toordinal() for r in recs]
    keep = longest_nondecreasing(ords)
    for i, r in enumerate(recs):
        if i not in keep:
            flag(r, "date", "date out of sequence with neighbouring rows",
                 r["date"].strftime("%d/%m/%Y"), "kept; please verify")

    serials = Counter()
    for r in recs:
        if re.fullmatch(r"\d+", r["serial"]):
            serials[int(r["serial"])] += 1
    for n, c in serials.items():
        if c > 1:
            for r in recs:
                if r["serial"] == str(n):
                    flag(r, "serial", f"serial number {n} used {c} times", r["serial"])
    if serials:
        present = set(serials) | {int(x["serial"]) for x in rejected if re.fullmatch(r"\d+", x.get("serial", ""))}
        gaps = sorted(set(range(min(present), max(present) + 1)) - present)
        runs, start = [], None
        for g in gaps:
            if start is None:
                start = prev = g
            elif g == prev + 1:
                prev = g
            else:
                runs.append((start, prev)); start = prev = g
        if start is not None:
            runs.append((start, prev))
        for a, b in runs:
            label = f"serial {a}" if a == b else f"serials {a}-{b}"
            review.append({"source_row": "", "opd_no": "", "patient": "", "level": "warn",
                           "field": "serial", "issue": f"{label} missing from register "
                           "(unused slot, lost page, or never typed)",
                           "original": "", "action": "none"})
    opd_count = Counter(r["opd_no"] for r in recs if r["opd_no"])

    # ---- per-row cleaning ------------------------------------------------
    for r in recs:
        r["name_norm"] = norm_name(r["name"])
        if r["serial"].isdigit() and r["opd_no"]:
            m = re.match(r"0*(\d+)/", r["opd_no"])
            if m and int(m[1]) != int(r["serial"]):
                flag(r, "opd_no", "serial and OPD number disagree",
                     f"serial {r['serial']} / OPD {r['opd_no']}")
        if r["opd_no"] and opd_count[r["opd_no"]] > 1:
            flag(r, "opd_no", "OPD number repeated", r["opd_no"])
        r["visit_ref"] = r["opd_no"] or f"ROW{r['source_row']}"

        sex = r["sex"].upper()
        r["gender"] = {"M": "male", "F": "female"}.get(sex, "")
        if not r["gender"]:
            flag(r, "sex", "sex missing or invalid", r["sex"], "left blank", "error")

        y, mth, issue = parse_age(r["age"])
        if issue:
            flag(r, "age", issue, r["age"], "dob left blank")
        r["years"], r["months"] = y, mth
        r["dob"] = (r["date"] - pd.DateOffset(years=y, months=mth)) if y is not None else None

        r["phone"], r["phone_e164"], issue = clean_phone(r["contact"])
        if issue:
            flag(r, "contact", issue, r["contact"], "left blank")
        r["national_id"], r["id_type"], issue = clean_id(r["id_no"])
        if issue:
            flag(r, "id_no", issue, r["id_no"], "left blank")

        occ = r["occupation"].upper()
        r["occupation"] = OCC_MAP.get(occ, occ) if occ else ""
        res = r["residence"].upper()
        r["residence"] = RES_FIX.get(res, res).title()

        # vitals
        r["temp_c"] = r["weight_kg"] = r["sys"] = r["dia"] = None
        if r["temp"]:
            try:
                t = float(r["temp"])
                if 32 <= t <= 43:
                    r["temp_c"] = t
                    if t < 35 or t > 40:
                        flag(r, "temp", "temperature unusual", r["temp"])
                else:
                    flag(r, "temp", "temperature impossible", r["temp"], "left blank", "error")
            except ValueError:
                flag(r, "temp", "temperature not a number", r["temp"], "left blank", "error")
        if r["weight"]:
            try:
                w = float(r["weight"])
                if 2 <= w <= 250:
                    r["weight_kg"] = w
                    if y is not None and ((y >= 18 and w < 30) or (y < 12 and w > 60) or w > 150):
                        flag(r, "weight", f"weight {w} kg odd for age {r['age']}", r["weight"])
                else:
                    flag(r, "weight", "weight impossible", r["weight"], "left blank", "error")
            except ValueError:
                flag(r, "weight", "weight not a number", r["weight"], "left blank", "error")
        if r["bp"]:
            m = re.fullmatch(r"(\d{2,3})\s*/\s*(\d{2,3})", r["bp"])
            if not m or not (50 <= int(m[1]) <= 280 and 20 <= int(m[2]) <= 170) or int(m[1]) <= int(m[2]):
                flag(r, "bp", "BP not valid", r["bp"], "left blank", "error")
            else:
                r["sys"], r["dia"] = int(m[1]), int(m[2])
                if r["sys"] - r["dia"] < 20:
                    flag(r, "bp", "BP pulse pressure very narrow - likely typo", r["bp"])

        # diagnosis / treatment
        r["dx_list"], note = clean_diagnosis(r["diagnosis"])
        if note:
            flag(r, "diagnosis", note, r["diagnosis"])
        if not r["dx_list"]:
            flag(r, "diagnosis", "diagnosis empty", "", "visit kept without diagnosis")
        dx_txt = " ".join(r["dx_list"])
        if r["gender"] == "male" and any(k in dx_txt for k in FEMALE_ONLY if len(k) > 2):
            flag(r, "sex", "female-only diagnosis on a male patient", f"{r['sex']} / {dx_txt}")
        if r["gender"] == "female" and any(k in dx_txt for k in MALE_ONLY):
            flag(r, "sex", "male-only diagnosis on a female patient", f"{r['sex']} / {dx_txt}")
        if r["occupation"].upper() == "STUDENT" and y is not None and y > 25:
            flag(r, "occupation", "student but age over 25", f"{r['age']} / STUDENT")
        r["tx_items"] = parse_treatment(r["treatment"])
        for kind, _rt, text in r["tx_items"]:
            if kind == "drug" and sum(len(w) >= 5 for w in text.split()) >= 2:
                flag(r, "treatment", "item may contain more than one drug", text,
                     "kept as one prescription line")
        if not r["payment"]:
            r["payment"] = ""

    # ---- patient matching ------------------------------------------------
    n = len(recs)
    uf = UF(n)
    by_tok, by_phone, by_id = defaultdict(list), defaultdict(list), defaultdict(list)
    for i, r in enumerate(recs):
        for t in set(r["name_norm"].split()):
            if len(t) >= 3:
                by_tok[t].append(i)
        if r["phone"]:
            by_phone[r["phone"]].append(i)
        if r["national_id"]:
            by_id[r["national_id"]].append(i)
    cand = set()
    for idx in list(by_tok.values()) + list(by_phone.values()) + list(by_id.values()):
        if len(idx) > 150:
            continue
        for a in range(len(idx)):
            for b in range(a + 1, len(idx)):
                cand.add((idx[a], idx[b]))

    def dob_gap(a, b):
        if a["dob"] is None or b["dob"] is None:
            return None
        return abs((a["dob"] - b["dob"]).days) / 365.25

    near_miss = []
    for i, j in sorted(cand):
        a, b = recs[i], recs[j]
        if a["gender"] and b["gender"] and a["gender"] != b["gender"]:
            continue
        sim = name_sim(a["name_norm"], b["name_norm"])
        gap = dob_gap(a, b)
        age_ok = gap is not None and gap <= 1.6
        same_id = a["national_id"] and a["national_id"] == b["national_id"]
        diff_id = a["national_id"] and b["national_id"] and a["national_id"] != b["national_id"]
        same_ph = a["phone"] and a["phone"] == b["phone"]
        merged = False
        if same_id and sim >= 0.6:
            merged = True
        elif not diff_id and same_ph and sim >= 0.75 and (gap is None or age_ok):
            merged = True
        elif not diff_id and sim >= 0.92 and (age_ok or (gap is None and sim >= 0.999)):
            merged = True
        if merged:
            uf.union(i, j)
        elif sim >= 0.9 or same_id:
            reason = ("same ID but different names" if same_id else
                      "same name, different ID" if diff_id else
                      "same/similar name, ages differ" if gap is not None and not age_ok else
                      "similar name, age unknown")
            near_miss.append((i, j, reason))

    groups = defaultdict(list)
    for i in range(n):
        groups[uf.find(i)].append(i)
    ordered = sorted(groups.values(), key=lambda g: min(g))
    pref = {}
    patients, merges = [], []
    for k, g in enumerate(ordered, 1):
        ref = f"P{k:05d}"
        rows = sorted((recs[i] for i in g), key=lambda x: (x["date"], x["source_row"]))
        for x in rows:
            x["patient_ref"] = ref
        for i in g:
            pref[i] = ref

        def latest(field):
            for x in reversed(rows):
                if x[field]:
                    return x[field]
            return ""
        full = max((x["name_norm"] for x in rows), key=lambda t: (len(t), t))
        first, last, single = split_name(full)
        if single:
            flag(rows[0], "name", "only one name given; last_name set to UNKNOWN", full)
            last = "UNKNOWN"
        ids = {x["national_id"] for x in rows if x["national_id"]}
        if len(ids) > 1:
            flag(rows[0], "id_no", "different IDs across visits of the same patient", ", ".join(sorted(ids)))
        sexes = Counter(x["gender"] for x in rows if x["gender"])
        dob = next((x["dob"] for x in rows if x["dob"] is not None), None)
        patients.append({
            "patient_ref": ref, "first_name": first, "last_name": last, "full_name": full.title(),
            "gender": sexes.most_common(1)[0][0] if sexes else "",
            "dob_estimated": dob.date().isoformat() if dob is not None else "",
            "age_years_at_first_visit": rows[0]["years"] if rows[0]["years"] is not None else "",
            "age_months_at_first_visit": rows[0]["months"] if rows[0]["years"] is not None else "",
            "residence": latest("residence"), "national_id": latest("national_id"),
            "id_type": latest("id_type"), "phone": latest("phone"),
            "phone_e164": latest("phone_e164"), "occupation": latest("occupation"),
            "first_visit_date": rows[0]["date"].date().isoformat(),
            "organization_id": args.organization_id or "", "facility_id": args.facility_id or "",
            "source_rows": ";".join(str(x["source_row"]) for x in rows),
        })
        if len(rows) > 1:
            merges.append({"patient_ref": ref, "visit_count": len(rows),
                           "name_variants": " | ".join(sorted({x["name"] for x in rows})),
                           "source_rows": ";".join(str(x["source_row"]) for x in rows)})
        for v, x in enumerate(rows):
            x["visit_type"] = "new" if v == 0 else "revisit"

    dups, seen_pairs = [], set()
    for i, j, reason in near_miss:
        pi, pj = pref[i], pref[j]
        if pi == pj or (pi, pj) in seen_pairs:
            continue
        seen_pairs.add((pi, pj))
        a, b = recs[i], recs[j]
        dups.append({"patient_ref_a": pi, "name_a": a["name"], "age_a": a["age"], "source_row_a": a["source_row"],
                     "patient_ref_b": pj, "name_b": b["name"], "age_b": b["age"], "source_row_b": b["source_row"],
                     "reason": reason})

    # ---- output tables ---------------------------------------------------
    visits, vitals, notes, rx, procs = [], [], [], [], []
    vocab = Counter()
    for x in sorted(recs, key=lambda x: (x["date"], x["source_row"])):
        dx_all = "; ".join(x["dx_list"])
        visits.append({
            "visit_ref": x["visit_ref"], "patient_ref": x["patient_ref"], "opd_no": x["opd_no"],
            "serial_no": x["serial"], "visit_date": x["date"].date().isoformat(),
            "visit_type": x["visit_type"],
            "register_said_new": x["new_revisit"],
            "diagnosis_primary": x["dx_list"][0] if x["dx_list"] else "",
            "diagnosis_all": dx_all, "payment_mode": x["payment"].upper(),
            "remarks": x["remarks"],
            "organization_id": args.organization_id or "", "facility_id": args.facility_id or "",
            "idempotency_key": f"opd-{args.facility_id or 'x'}-{x['visit_ref']}",
            "source_row": x["source_row"],
        })
        if any(v is not None for v in (x["temp_c"], x["weight_kg"], x["sys"])):
            vitals.append({"visit_ref": x["visit_ref"], "patient_ref": x["patient_ref"],
                           "temperature": x["temp_c"], "weight": x["weight_kg"],
                           "systolic_bp": x["sys"], "diastolic_bp": x["dia"]})
        plan = "; ".join((f"{rt} " if rt else "") + t for _, rt, t in x["tx_items"])
        notes.append({"visit_ref": x["visit_ref"], "patient_ref": x["patient_ref"],
                      "subjective": "", "objective": "", "assessment": dx_all, "plan": plan,
                      "assessment_raw": x["diagnosis"], "plan_raw": x["treatment"]})
        for kind, route, text in x["tx_items"]:
            vocab[(kind, text)] += 1
            if kind == "drug":
                rx.append({"visit_ref": x["visit_ref"], "patient_ref": x["patient_ref"],
                           "drug": text, "route": route})
            else:
                procs.append({"visit_ref": x["visit_ref"], "patient_ref": x["patient_ref"],
                              "procedure": text})

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows, cols=None):
        d = pd.DataFrame(rows, columns=cols) if rows or cols else pd.DataFrame()
        d = d.rename(columns=V3_COLUMN_MAP.get(name, {}))
        d.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")

    write("patients", patients)
    write("visits", visits)
    write("vitals", vitals)
    write("doctor_notes", notes)
    write("prescriptions", rx, ["visit_ref", "patient_ref", "drug", "route"])
    write("procedures", procs, ["visit_ref", "patient_ref", "procedure"])
    pd.DataFrame(review).to_csv(out / "review.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(dups).to_csv(out / "possible_duplicates.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(merges).to_csv(out / "patient_merges.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(rejected).to_csv(out / "rejected_rows.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([{"kind": k, "text": t, "count": c} for (k, t), c in vocab.most_common()]
                 ).to_csv(out / "treatment_vocab.csv", index=False, encoding="utf-8-sig")

    if args.facility:
        sd = write_state(out, args.facility, patients, visits, vitals, notes,
                         args.organization_id, args.facility_id)
        print(f"State folder: {sd}")
    lv = Counter(r["level"] for r in review)
    print(f"Read {len(df)} rows -> {len(recs)} usable visits, {len(rejected)} rejected")
    print(f"Patients: {len(patients)} unique ({len(recs) - len(patients)} repeat visits merged)")
    print(f"Review items: {len(review)}  (errors={lv['error']}, warnings={lv['warn']})")
    print(f"Possible duplicates NOT merged: {len(dups)}")
    print(f"Prescriptions: {len(rx)}   Procedures: {len(procs)}")
    print(f"Output written to {out.resolve()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", help=".xlsx / .csv register")
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--out-dir", default="v3_ready")
    ap.add_argument("--organization-id", default="")
    ap.add_argument("--facility-id", default="")
    ap.add_argument("--facility", default=None,
                    help="facility name (e.g. portbell): also writes .migration_state/<facility>/ "
                         "and v2_shaped_<facility>.json")
    ap.add_argument("--mapping", default=None,
                    help="v3_mapping.json; its 'column_map' section renames output columns")
    run(ap.parse_args())