"""
Load a HeidiSQL/MySQL dump (backup-2.sql, database `smarthealth`) into
Snowflake as HOSPITALS.SMARTHEALTH.

- Every CREATE TABLE is translated to a typed Snowflake table (empty tables included).
- INSERT rows are parsed in Python, written to CSV, PUT to the table stage and COPY'd in.
- Stored procedures in the dump are MySQL-specific and are skipped.

Usage: python load_smarthealth_backup_to_snowflake.py [path/to/backup-2.sql]
"""
import csv
import os
import re
import sys
import tempfile
from pathlib import Path

if sys.platform == "win32":
    import platform
    platform.libc_ver = lambda *a, **k: ("", "")  # Windows Store python alias can't be opened

import snowflake.connector

ROOT = Path(__file__).resolve().parent
DUMP = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "backup-2.sql"
DATABASE = "HOSPITALS"
SCHEMA = "SMARTHEALTH"


def load_env():
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip().removeprefix("export ").strip(), v.strip().strip('"').strip("'"))


def key_path(p):
    p = p.strip()
    if sys.platform == "win32" and p.startswith("/"):
        return r"\\wsl.localhost\Ubuntu" + p.replace("/", "\\")
    return p if os.path.isabs(p) else str(ROOT / p)


def connect():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
        role=(os.getenv("SNOWFLAKE_ROLE") or "").strip() or None,
        private_key_file=key_path(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
        database=DATABASE,
    )


# ---------------------------------------------------------------- DDL

def sf_type(mysql_type):
    t = mysql_type.lower()
    base = re.match(r"[a-z]+", t).group(0)
    if base in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year"):
        return "NUMBER(38,0)"
    if base in ("decimal", "numeric"):
        m = re.search(r"\((\d+)\s*,\s*(\d+)\)", t)
        return f"NUMBER({min(int(m.group(1)), 38)},{m.group(2)})" if m else "NUMBER(38,6)"
    if base in ("double", "float", "real"):
        return "FLOAT"
    if base == "bit":
        m = re.search(r"\((\d+)\)", t)
        return "BOOLEAN" if not m or m.group(1) == "1" else "NUMBER(38,0)"
    if base == "date":
        return "DATE"
    if base in ("datetime", "timestamp"):
        return "TIMESTAMP_NTZ"
    if base == "time":
        return "TIME"
    if base in ("binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"):
        return "BINARY"
    if base == "json":
        return "VARIANT"
    return "VARCHAR"


def parse_create(stmt):
    name = re.search(r"CREATE TABLE IF NOT EXISTS `([^`]+)`", stmt).group(1)
    cols = []
    for line in stmt.splitlines():
        m = re.match(r"\s+`([^`]+)`\s+([a-z]+(?:\([^)]*\))?(?:\s+unsigned)?)", line)
        if m:
            cols.append((m.group(1), m.group(2)))
    return name, cols


# ---------------------------------------------------------------- INSERT parsing

ESCAPES = {"0": "\0", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a", "b": "\b"}


def parse_values(s, i):
    """Parse `(v, v), (v, v);` starting at s[i]. Returns (rows, index after ';')."""
    rows, n = [], len(s)
    while i < n:
        c = s[i]
        if c == ";":
            return rows, i + 1
        if c != "(":
            i += 1
            continue
        i += 1
        row = []
        while True:
            while s[i] in " \t\r\n":
                i += 1
            c = s[i]
            if c == "'":
                i += 1
                buf = []
                while True:
                    j = i
                    while s[j] not in "'\\":
                        j += 1
                    buf.append(s[i:j])
                    if s[j] == "\\":
                        buf.append(ESCAPES.get(s[j + 1], s[j + 1]))
                        i = j + 2
                    elif s[j + 1] == "'":
                        buf.append("'")
                        i = j + 2
                    else:
                        i = j + 1
                        break
                row.append("".join(buf))
            elif s.startswith("b'", i):
                j = s.index("'", i + 2)
                row.append(("bits", s[i + 2:j]))
                i = j + 1
            elif s.startswith("0x", i):
                j = i + 2
                while s[j] in "0123456789abcdefABCDEF":
                    j += 1
                row.append(("hex", s[i + 2:j]))
                i = j
            elif s.startswith("_binary", i):
                i += len("_binary")
                continue
            else:
                j = i
                while s[j] not in ",)":
                    j += 1
                tok = s[i:j].strip()
                row.append(None if tok.upper() == "NULL" else tok)
                i = j
            while s[i] in " \t\r\n":
                i += 1
            if s[i] == ",":
                i += 1
                continue
            if s[i] == ")":
                i += 1
                break
        rows.append(row)
    return rows, i


NULL = "\\N"


def to_csv_value(v, sftype):
    if v is None:
        return NULL
    if isinstance(v, tuple):
        kind, raw = v
        if kind == "bits":
            n = int(raw, 2) if raw else 0
            return ("TRUE" if n else "FALSE") if sftype == "BOOLEAN" else str(n)
        return raw.upper()  # hex for BINARY
    if sftype in ("DATE", "TIMESTAMP_NTZ") and v.startswith("0000-00-00"):
        return NULL
    if sftype == "BINARY":
        return v.encode("utf-8", "surrogatepass").hex().upper()
    if sftype == "BOOLEAN":
        return "TRUE" if v not in ("0", "", "false", "FALSE") else "FALSE"
    return v


# ---------------------------------------------------------------- main

def main():
    load_env()
    text = DUMP.read_text(encoding="utf-8", errors="surrogateescape")

    tables = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS `[^`]+` \(.*?\n\)[^;]*;", text, re.S):
        name, cols = parse_create(m.group(0))
        tables[name] = cols
    print(f"{len(tables)} tables in dump", flush=True)

    data = {}
    for m in re.finditer(r"INSERT INTO `([^`]+)` \(([^)]*)\) VALUES", text):
        name = m.group(1)
        cols = re.findall(r"`([^`]+)`", m.group(2))
        rows, _ = parse_values(text, m.end())
        prev = data.setdefault(name, (cols, []))
        prev[1].extend(rows)
    print(f"{len(data)} tables with data, {sum(len(r) for _, r in data.values())} rows", flush=True)

    conn = connect()
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {DATABASE}."{SCHEMA}"')
    cur.execute(f'USE SCHEMA {DATABASE}."{SCHEMA}"')

    ok, failed = 0, []
    tmp = Path(tempfile.mkdtemp(prefix="smarthealth_"))
    for name, cols in tables.items():
        tname = name.upper()
        types = {c: sf_type(t) for c, t in cols}
        ddl = ", ".join(f'"{c.upper()}" {types[c]}' for c, _ in cols)
        try:
            cur.execute(f'CREATE OR REPLACE TABLE "{tname}" ({ddl})')
            nrows = 0
            if name in data:
                icols, rows = data[name]
                path = tmp / f"{tname}.csv"
                with open(path, "w", newline="", encoding="utf-8", errors="surrogateescape") as f:
                    w = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
                    for r in rows:
                        w.writerow([to_csv_value(v, types[c]) for c, v in zip(icols, r)])
                cur.execute(f"PUT 'file://{path.as_posix()}' @%\"{tname}\" OVERWRITE=TRUE AUTO_COMPRESS=TRUE")
                collist = ", ".join(f'"{c.upper()}"' for c in icols)
                cur.execute(f"""
                    COPY INTO "{tname}" ({collist}) FROM @%"{tname}"
                    FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('\\\\N')
                                   EMPTY_FIELD_AS_NULL=FALSE ESCAPE_UNENCLOSED_FIELD=NONE
                                   BINARY_FORMAT=HEX ENCODING='UTF8')
                    PURGE=TRUE ON_ERROR=ABORT_STATEMENT""")
                nrows = sum(r[3] for r in cur.fetchall() if len(r) > 3 and isinstance(r[3], int))
                if nrows != len(rows):
                    raise RuntimeError(f"loaded {nrows} of {len(rows)} rows")
            ok += 1
            print(f"OK   {tname:50s} {nrows:7d} rows", flush=True)
        except Exception as e:
            failed.append((tname, str(e).strip().splitlines()[-1]))
            print(f"FAIL {tname}: {str(e).strip()}", flush=True)

    print(f"\n{ok} tables created in {DATABASE}.{SCHEMA}, {len(failed)} failed")
    for t, e in failed:
        print(f"   {t}: {e}")


if __name__ == "__main__":
    main()
