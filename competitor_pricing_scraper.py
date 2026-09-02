#!/usr/bin/env python3
"""
competitor_pricing_scraper.py  —  standalone (no Airflow) pipeline runner.

  A "universal" scraper for competitive pricing: point it at ANY website
  (a product listing page, a category page, ...) and it will pull out
  structured product/price rows — no per-site CSS selectors required.

  1. Builds a list of (competitor, url) targets from a Google Sheet
     (recurring competitors), any --urls passed on the CLI (ad-hoc,
     one-off checks), and any --query passed on the CLI (discovers
     candidate URLs via Tavily search when you don't know the exact page).
  2. For each target, fetches the rendered page content via the Tavily
     Extract API (handles JS rendering / anti-bot server-side). If Tavily
     fails or returns too little content, falls back to a headless
     Selenium fetch.
  3. Feeds the page content to an OpenAI model with a fixed JSON schema
     (function calling) to pull out every product + its pricing — this is
     what makes it "universal": the LLM adapts to whatever layout the
     page has instead of hand-written selectors per competitor.
  4. Writes each target's extracted rows as gzipped JSONL to S3.
  5. COPYs each S3 file into RAW.COMPETITOR_PRICING_RAW.
  6. MERGEs RAW.COMPETITOR_PRICING_RAW → CLEAN.COMPETITOR_PRICING
     (one row per competitor/product/day — a daily price-history table).

USAGE
  # Recurring competitors from the Sheet
  python competitor_pricing_scraper.py

  # One-off check of a specific page, no Sheet involved
  python competitor_pricing_scraper.py --urls https://example.com/shop --competitor acme

  # Don't know the competitor's URL? Discover it via a Tavily search query first
  python competitor_pricing_scraper.py --query "goodlife pharmacy kenya prices" --competitor goodlife

  # Sheet + an extra ad-hoc URL in the same run
  python competitor_pricing_scraper.py --urls https://example.com/sale --competitor acme

  # Just see what would be extracted — no S3 / Snowflake / OpenAI cost beyond extraction
  python competitor_pricing_scraper.py --urls https://example.com/shop --competitor acme --dry-run

  # Only re-run specific competitors from the sheet
  python competitor_pricing_scraper.py --only-competitors goodlife,jumia

ENV VARS  (put them in a `.env` file next to this script — auto-loaded)
  # Snowflake (key-pair auth) — same as the facility pipeline
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # AWS  (or use the default boto3 credential chain — ~/.aws/credentials)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

  # Google service account for the competitor-target sheet
  GOOGLE_SA_JSON_PATH=/abs/path/to/service-account.json
  COMPETITOR_SHEET_ID=...
  COMPETITOR_SHEET_WORKSHEET=Sheet1
  # Sheet columns expected: competitor, url, category (optional), enabled (optional, TRUE/FALSE)

  # Tavily (page fetch + --query discovery)
  TAVILY_API_KEY=...
  DISCOVERY_EXCLUDE_DOMAINS=...     # optional, comma-separated, adds to the
                                    # built-in social/video skip-list for --query

  # OpenAI (structured extraction)
  OPENAI_API_KEY=...
  OPENAI_MODEL=gpt-4.1      # or a cheaper/faster model for high-volume runs

  # Tuning
  PIPELINE_WORKERS=6        # parallel targets — keep modest, Selenium fallback is heavy
  MAX_CONTENT_CHARS=20000   # page content is truncated to this before it goes to the model
  LOG_LEVEL=INFO

REQUIRED SNOWFLAKE OBJECTS  (create once, out of band — this script never runs DDL)
  CREATE TABLE HOSPITALS.SHARED.COMPETITOR_PRICING_RAW (
      competitor     STRING,
      source_url     STRING,
      category       STRING,
      fetch_method   STRING,
      scraped_at     TIMESTAMP_TZ,
      payload        VARIANT
  );

  CREATE TABLE HOSPITALS.SHARED.COMPETITOR_PRICING (
      competitor            STRING,
      product_name          STRING,
      sku                   STRING,
      brand                 STRING,
      current_price         FLOAT,
      original_price        FLOAT,
      currency              STRING,
      discount_percentage   FLOAT,
      in_stock              BOOLEAN,
      product_url           STRING,
      unit                  STRING,
      source_url            STRING,
      category              STRING,
      scraped_at            TIMESTAMP_TZ
  );

  This reuses the same external stage + file format the facility pipeline
  already has (HOSPITALS.SHARED.FACILITY_RAW_STAGE / JSON_FF) since both
  just COPY gzipped JSONL out of the same S3 bucket — confirm that stage
  is scoped at the bucket root before the first non-dry-run.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import gspread
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Optional: orjson is ~3x faster than stdlib json for encoding rows.
try:
    import orjson
    def _dumps_bytes(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────

log = logging.getLogger("competitor_pricing_scraper")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(name)s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────

PIPELINE_NAME = "competitor_pricing_scraper"

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/competitor_pricing"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"
RAW_TABLE        = f"{SF_DB}.{SF_SHARED_SCHEMA}.COMPETITOR_PRICING_RAW"
CLEAN_TABLE      = f"{SF_DB}.{SF_SHARED_SCHEMA}.COMPETITOR_PRICING"

PROGRESS_FILE = Path(__file__).resolve().parent / ".competitor_pricing_progress.json"

DEFAULT_PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "6"))
MAX_CONTENT_CHARS        = int(os.getenv("MAX_CONTENT_CHARS", "20000"))
MIN_TAVILY_CONTENT_CHARS = 200   # below this, treat Tavily's result as a failed fetch
DEFAULT_OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4.1")

# ─── PROGRESS / RESUME ───────────────────────────────────────────────────

_progress_lock = threading.Lock()

def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s — ignoring", PROGRESS_FILE, e)
    return {}

def _save_progress(data: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))

def _job_key(job: dict) -> str:
    return hashlib.md5(f"{job['competitor'].lower()}|{job['url']}".encode()).hexdigest()

def _mark_done(run_id: str, job: dict, s3_key: str | None) -> None:
    with _progress_lock:
        prog = _load_progress()
        bucket = prog.setdefault("default", {"run_id": run_id, "completed": {}})
        if bucket.get("run_id") != run_id:
            bucket["run_id"] = run_id
        bucket["completed"][_job_key(job)] = {
            "competitor": job["competitor"], "url": job["url"],
            "s3_key": s3_key, "at": datetime.now(timezone.utc).isoformat(),
        }
        _save_progress(prog)

def _clear_progress() -> None:
    with _progress_lock:
        _save_progress({})

def _completed_keys() -> set[str]:
    return set(_load_progress().get("default", {}).get("completed", {}).keys())

# ─── SNOWFLAKE CLIENT  (thread-safe via per-call lock) ───────────────────

class SnowflakeClient:
    """Read (`query`) + write (`execute`) with structured logging.
    Thread-safe across multiple workers as long as `execute` /
    `query` are called via the public methods (each one creates its
    own short-lived cursor under a lock)."""

    def __init__(self, schema_: str | None = None):
        with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(), "rb") as key:
            key.read()

        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )
        self._lock = threading.Lock()

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def query(self, sql: str, label: str | None = None) -> pd.DataFrame:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-26s SELECT    | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_pandas_all()
            elapsed = time.perf_counter() - t0
            log.info("✓ %-26s done      | %s rows · %d cols · %.2fs",
                     label, f"{len(df):,}", df.shape[1], elapsed)
            return df
        except Exception as e:
            log.exception("✗ %-26s SELECT failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-26s WRITE     | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                rowcount = cur.rowcount
                sfqid = cur.sfqid
            elapsed = time.perf_counter() - t0
            log.info("✓ %-26s done      | rowcount=%s · sfqid=%s · %.2fs",
                     label, rowcount, sfqid, elapsed)
            return {"rowcount": rowcount, "sfqid": sfqid, "elapsed_s": elapsed}
        except Exception as e:
            log.exception("✗ %-26s WRITE failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *a): self.close()

# ─── GOOGLE SHEET (competitor targets) ────────────────────────────────────

def get_gsheet_client():
    sa_path = os.getenv("GOOGLE_SA_JSON_PATH")
    if sa_path:
        return gspread.service_account(filename=sa_path)
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if sa_json:
        return gspread.service_account_from_dict(json.loads(sa_json))
    raise RuntimeError("Set GOOGLE_SA_JSON_PATH (file path) or GOOGLE_SA_JSON (raw JSON)")

def read_target_sheet(spreadsheet_id: str, worksheet_name: str) -> list[dict]:
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    return ws.get_all_records()

# ─── TARGET BUILDER ──────────────────────────────────────────────────────

def _normalize_url(url: str) -> str:
    """Chrome (and Tavily) reject a schemeless URL like 'goodlife.com' with
    an opaque 'invalid argument' error — default bare domains to https://."""
    url = url.strip()
    if url and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", url):
        url = f"https://{url}"
    return url

def build_targets(*, sheet_id: str | None, sheet_worksheet: str,
                  adhoc_urls: list[str], adhoc_queries: list[str],
                  adhoc_competitor: str, adhoc_category: str | None,
                  only_competitors: set[str] | None,
                  search_depth: str, max_search_results: int) -> list[dict]:
    seen, targets = set(), []

    if sheet_id:
        rows = read_target_sheet(sheet_id, sheet_worksheet)
        for r in rows:
            competitor = (r.get("competitor") or "").strip()
            url        = _normalize_url(r.get("url") or "")
            enabled    = str(r.get("enabled", "TRUE")).strip().upper()
            if not competitor or not url or enabled == "FALSE":
                continue
            if only_competitors and competitor.lower() not in only_competitors:
                continue
            key = (competitor.lower(), url)
            if key in seen:
                continue
            seen.add(key)
            targets.append({
                "competitor": competitor, "url": url,
                "category": (r.get("category") or "").strip() or None,
            })

    for url in adhoc_urls:
        url = _normalize_url(url)
        if not url:
            continue
        key = (adhoc_competitor.lower(), url)
        if key in seen:
            continue
        seen.add(key)
        targets.append({
            "competitor": adhoc_competitor, "url": url,
            "category": adhoc_category,
        })

    for query in adhoc_queries:
        query = query.strip()
        if not query:
            continue
        discovered = discover_urls_via_tavily(
            query, max_results=max_search_results, search_depth=search_depth,
        )
        for url in discovered:
            url = _normalize_url(url)
            key = (adhoc_competitor.lower(), url)
            if not url or key in seen:
                continue
            seen.add(key)
            targets.append({
                "competitor": adhoc_competitor, "url": url,
                "category": adhoc_category, "source_query": query,
            })

    log.info("Prepared %d scrape targets (sheet=%s, adhoc_urls=%d, adhoc_queries=%d)",
             len(targets), bool(sheet_id), len(adhoc_urls), len(adhoc_queries))
    return targets

# ─── FETCH  (Tavily primary, Selenium fallback) ──────────────────────────

_tavily_client_singleton = None
_tavily_lock = threading.Lock()

def _get_tavily_client():
    global _tavily_client_singleton
    if _tavily_client_singleton is None:
        with _tavily_lock:
            if _tavily_client_singleton is None:
                api_key = os.getenv("TAVILY_API_KEY")
                if not api_key:
                    return None
                from tavily import TavilyClient
                _tavily_client_singleton = TavilyClient(api_key=api_key)
    return _tavily_client_singleton

# Social/video platforms rarely carry parseable pricing text and commonly
# block headless fetches outright — skip them at discovery time rather than
# burning a Tavily-extract + Selenium-fallback attempt on a guaranteed dead end.
_DEFAULT_DISCOVERY_EXCLUDE_DOMAINS = {
    "tiktok.com", "instagram.com", "facebook.com", "twitter.com", "x.com",
    "youtube.com", "youtu.be", "pinterest.com", "linkedin.com", "reddit.com",
}

def _discovery_exclude_domains() -> set[str]:
    extra = os.getenv("DISCOVERY_EXCLUDE_DOMAINS", "")
    extra_domains = {d.strip().lower() for d in extra.split(",") if d.strip()}
    return _DEFAULT_DISCOVERY_EXCLUDE_DOMAINS | extra_domains

def _is_excluded_domain(url: str, excluded: set[str]) -> bool:
    from urllib.parse import urlparse
    host = (urlparse(url).netloc or "").lower()
    host = host.split("@")[-1].split(":")[0]  # strip userinfo/port if present
    return any(host == d or host.endswith(f".{d}") for d in excluded)

def discover_urls_via_tavily(query: str, *, max_results: int, search_depth: str) -> list[str]:
    """Query-based discovery — find candidate pages when you don't already
    know the competitor's URL. Returns only URLs; full content still comes
    from fetch_via_tavily()/extract() per URL, same as a curated target."""
    client = _get_tavily_client()
    if client is None:
        log.warning("TAVILY_API_KEY not set — cannot run discovery query %r", query)
        return []
    # Ask Tavily for extra results up front since some will be filtered out below.
    try:
        result = client.search(query=query, search_depth=search_depth,
                               max_results=max(max_results * 2, 10))
    except Exception as e:
        log.warning("Tavily search failed for query %r: %s", query, e)
        return []

    excluded = _discovery_exclude_domains()
    all_urls = [r["url"] for r in result.get("results", []) if r.get("url")]
    dropped  = [u for u in all_urls if _is_excluded_domain(u, excluded)]
    urls     = [u for u in all_urls if u not in dropped][:max_results]

    if dropped:
        log.info("Discovery query %r · skipped %d social/video URL(s): %s",
                 query, len(dropped), dropped)
    log.info("Discovery query %r → %d candidate URL(s)", query, len(urls))
    return urls

def fetch_via_tavily(url: str) -> str | None:
    client = _get_tavily_client()
    if client is None:
        log.warning("TAVILY_API_KEY not set — skipping Tavily fetch for %s", url)
        return None
    try:
        result = client.extract(urls=[url])
    except Exception as e:
        log.warning("Tavily extract failed for %s: %s", url, e)
        return None

    for r in result.get("results", []):
        if r.get("url") == url or True:  # single-url call — take the first hit
            content = (r.get("raw_content") or "").strip()
            if len(content) >= MIN_TAVILY_CONTENT_CHARS:
                return content
            break

    failed = result.get("failed_results") or []
    if failed:
        log.warning("Tavily could not fetch %s: %s", url, failed[0].get("error"))
    else:
        log.warning("Tavily returned too little content for %s — falling back", url)
    return None

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}};
"""

def fetch_via_selenium(url: str, timeout: int = 30) -> str | None:
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0")
    chrome_bin = os.getenv("CHROME_BINARY_LOCATION")
    if chrome_bin:
        options.binary_location = chrome_bin
    chromedriver = os.getenv("CHROMEDRIVER_PATH")

    driver = webdriver.Chrome(
        service=Service(chromedriver) if chromedriver else Service(),
        options=options,
    )
    try:
        driver.set_page_load_timeout(timeout)
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": STEALTH_JS})
        except Exception:
            pass
        driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text if len(text) >= MIN_TAVILY_CONTENT_CHARS else None
    finally:
        driver.quit()

def fetch_page_content(url: str, *, allow_selenium_fallback: bool) -> tuple[str, str]:
    """Returns (content, fetch_method). Raises if every method fails."""
    content = fetch_via_tavily(url)
    if content:
        return content, "tavily"

    if not allow_selenium_fallback:
        raise RuntimeError(f"Tavily fetch failed for {url} and Selenium fallback is disabled")

    log.info("Falling back to Selenium for %s", url)
    content = fetch_via_selenium(url)
    if content:
        return content, "selenium"

    raise RuntimeError(f"Both Tavily and Selenium fallback failed to fetch {url}")

# ─── STRUCTURED EXTRACTION  (OpenAI function-calling, fixed schema) ──────

_openai_client_singleton = None
_openai_lock = threading.Lock()

def _get_openai_client():
    global _openai_client_singleton
    if _openai_client_singleton is None:
        with _openai_lock:
            if _openai_client_singleton is None:
                import openai
                _openai_client_singleton = openai.OpenAI()  # reads OPENAI_API_KEY
    return _openai_client_singleton

PRODUCT_TOOL = {
    "type": "function",
    "function": {
        "name": "record_products",
        "description": "Record every distinct product and its pricing found in the page content.",
        "parameters": {
            "type": "object",
            "properties": {
                "products": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "product_name":        {"type": "string"},
                            "sku":                 {"type": ["string", "null"]},
                            "brand":               {"type": ["string", "null"]},
                            "current_price":       {"type": ["number", "null"]},
                            "original_price":      {"type": ["number", "null"], "description": "Pre-discount price, if shown"},
                            "currency":            {"type": ["string", "null"], "description": "ISO code or symbol as shown on the page, e.g. KES, USD, $"},
                            "discount_percentage": {"type": ["number", "null"]},
                            "in_stock":            {"type": ["boolean", "null"]},
                            "product_url":         {"type": ["string", "null"], "description": "Link to the product's own page, if present"},
                            "unit":                {"type": ["string", "null"], "description": "e.g. 500ml, per pack of 10"},
                        },
                        "required": ["product_name"],
                    },
                },
            },
            "required": ["products"],
        },
    },
}

def extract_products_with_openai(content: str, *, competitor: str, url: str,
                                 model: str) -> list[dict]:
    client = _get_openai_client()
    truncated = content[:MAX_CONTENT_CHARS]

    prompt = (
        f"This is the extracted content of a page from {competitor} ({url}).\n"
        "Find every distinct product listed with a price and call record_products "
        "with one entry per product. Skip navigation/footer/unrelated text. "
        "If a field isn't present on the page, leave it null — do not guess.\n\n"
        f"--- PAGE CONTENT ---\n{truncated}"
    )

    try:
        resp = client.chat.completions.create(
            model=model,
            tools=[PRODUCT_TOOL],
            tool_choice={"type": "function", "function": {"name": "record_products"}},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        log.error("OpenAI extraction failed for %s · %s: %s", competitor, url, e)
        return []

    tool_calls = resp.choices[0].message.tool_calls or []
    for call in tool_calls:
        if call.function.name == "record_products":
            try:
                return json.loads(call.function.arguments).get("products", []) or []
            except json.JSONDecodeError as e:
                log.error("Could not parse OpenAI tool arguments for %s · %s: %s", competitor, url, e)
                return []
    return []

# ─── PIPELINE STEPS ──────────────────────────────────────────────────────

def _safe_s3_token(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", s)

_s3_client_singleton = None
_s3_client_lock = threading.Lock()

def _s3_client():
    global _s3_client_singleton
    if _s3_client_singleton is None:
        with _s3_client_lock:
            if _s3_client_singleton is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                if not (ak and sk):
                    raise RuntimeError(
                        "AWS credentials missing — set AWS_ACCESS_KEY_ID + "
                        "AWS_SECRET_ACCESS_KEY in your .env (and AWS_REGION)."
                    )
                _s3_client_singleton = boto3.client(
                    "s3", aws_access_key_id=ak, aws_secret_access_key=sk,
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
    return _s3_client_singleton

def scrape_one_target(job: dict, run_id: str, *, dry_run: bool,
                      allow_selenium_fallback: bool, model: str) -> dict | None:
    competitor = job["competitor"]
    url        = job["url"]

    content, fetch_method = fetch_page_content(url, allow_selenium_fallback=allow_selenium_fallback)
    log.info("    %s · %s — fetched %d chars via %s · preview: %r",
             competitor, url, len(content), fetch_method, content[:160].replace("\n", " "))

    products = extract_products_with_openai(content, competitor=competitor, url=url, model=model)

    if not products:
        log.info("    %s · %s — 0 products extracted (method=%s, content_chars=%d), skipping S3",
                 competitor, url, fetch_method, len(content))
        return None

    scraped_at = datetime.now(timezone.utc)
    rows = [
        {
            **p,
            "competitor": competitor,
            "source_url": url,
            "category": job.get("category"),
            "fetch_method": fetch_method,
            "scraped_at": scraped_at.isoformat(),
        }
        for p in products
    ]

    if dry_run:
        log.info("DRY-RUN ✓ %-20s %s products from %s (method=%s)",
                 competitor, len(products), url, fetch_method)
        for p in products[:5]:
            log.info("    · %s — %s", p.get("product_name"), p.get("current_price"))
        return None

    dt = scraped_at.date().isoformat()
    comp_safe = _safe_s3_token(competitor)
    url_hash  = hashlib.md5(url.encode()).hexdigest()[:10]
    key = (
        f"{S3_PREFIX}/"
        f"competitor={comp_safe or 'unknown'}/"
        f"dt={dt}/"
        f"{url_hash}__{run_id}.jsonl.gz"
    )

    parts = [_dumps_bytes(row) for row in rows]
    jsonl_bytes = b"\n".join(parts) + b"\n"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)
    _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Uploaded to s3://%s/%s products=%s (method=%s)",
             S3_BUCKET, key, len(products), fetch_method)

    return {
        "competitor": competitor, "source_url": url,
        "category": job.get("category"), "fetch_method": fetch_method,
        "scraped_at": scraped_at.isoformat(), "s3_key": key,
        "row_count": len(products),
    }

def copy_into_snowflake(job_result: dict, sf: SnowflakeClient) -> None:
    competitor   = job_result["competitor"].replace("'", "''")
    source_url   = job_result["source_url"].replace("'", "''")
    category     = (job_result.get("category") or "").replace("'", "''")
    fetch_method = job_result["fetch_method"]
    scraped_at   = job_result["scraped_at"]
    s3_key       = job_result["s3_key"]

    sql = f"""
    COPY INTO {RAW_TABLE} (competitor, source_url, category, fetch_method, scraped_at, payload)
    FROM (
      SELECT
        '{competitor}'::STRING        AS competitor,
        '{source_url}'::STRING        AS source_url,
        NULLIF('{category}', '')::STRING AS category,
        '{fetch_method}'::STRING      AS fetch_method,
        '{scraped_at}'::TIMESTAMP_TZ  AS scraped_at,
        PARSE_JSON($1)                AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    sf.execute(sql, label=f"copy:{job_result['competitor']}")

def merge_clean(sf: SnowflakeClient) -> None:
    sql = f"""
    MERGE INTO {CLEAN_TABLE} AS t
    USING (
        SELECT
            competitor,
            payload:product_name::STRING          AS product_name,
            payload:sku::STRING                    AS sku,
            payload:brand::STRING                  AS brand,
            payload:current_price::FLOAT           AS current_price,
            payload:original_price::FLOAT          AS original_price,
            payload:currency::STRING               AS currency,
            payload:discount_percentage::FLOAT     AS discount_percentage,
            payload:in_stock::BOOLEAN              AS in_stock,
            payload:product_url::STRING            AS product_url,
            payload:unit::STRING                   AS unit,
            source_url,
            category,
            scraped_at
        FROM {RAW_TABLE}
        WHERE payload:product_name IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY competitor, source_url, payload:product_name::STRING,
                         DATE_TRUNC('day', scraped_at)
            ORDER BY scraped_at DESC
        ) = 1
    ) AS s
    ON  t.competitor  = s.competitor
    AND t.source_url  = s.source_url
    AND t.product_name = s.product_name
    AND DATE_TRUNC('day', t.scraped_at) = DATE_TRUNC('day', s.scraped_at)
    WHEN MATCHED THEN UPDATE SET
        sku                  = s.sku,
        brand                = s.brand,
        current_price        = s.current_price,
        original_price       = s.original_price,
        currency             = s.currency,
        discount_percentage  = s.discount_percentage,
        in_stock             = s.in_stock,
        product_url          = s.product_url,
        unit                 = s.unit,
        category             = s.category,
        scraped_at           = s.scraped_at
    WHEN NOT MATCHED THEN INSERT (
        competitor, product_name, sku, brand, current_price, original_price,
        currency, discount_percentage, in_stock, product_url, unit,
        source_url, category, scraped_at
    ) VALUES (
        s.competitor, s.product_name, s.sku, s.brand, s.current_price, s.original_price,
        s.currency, s.discount_percentage, s.in_stock, s.product_url, s.unit,
        s.source_url, s.category, s.scraped_at
    );
    """
    sf.execute(sql, label="merge_clean:competitor_pricing")

# ─── ORCHESTRATOR ────────────────────────────────────────────────────────

def run_pipeline(*, sheet_id: str | None, sheet_worksheet: str,
                 adhoc_urls: list[str], adhoc_queries: list[str],
                 adhoc_competitor: str, adhoc_category: str | None,
                 only_competitors: set[str] | None,
                 search_depth: str = "advanced", max_search_results: int = 5,
                 skip_merge: bool = False, dry_run: bool = False,
                 workers: int = DEFAULT_PIPELINE_WORKERS,
                 model: str = DEFAULT_OPENAI_MODEL,
                 allow_selenium_fallback: bool = True,
                 resume: bool = True) -> None:

    run_id = datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H-%M-%SZ")

    all_targets = build_targets(
        sheet_id=sheet_id, sheet_worksheet=sheet_worksheet,
        adhoc_urls=adhoc_urls, adhoc_queries=adhoc_queries,
        adhoc_competitor=adhoc_competitor, adhoc_category=adhoc_category,
        only_competitors=only_competitors,
        search_depth=search_depth, max_search_results=max_search_results,
    )
    if not all_targets:
        log.warning("No scrape targets — pass --urls / --query, or set COMPETITOR_SHEET_ID.")
        return

    if not resume and not dry_run:
        _clear_progress()
        log.info("⟲ Resume disabled · cleared previous progress")
        targets = all_targets
    elif resume and not dry_run:
        done = _completed_keys()
        skipped = [t for t in all_targets if _job_key(t) in done]
        targets = [t for t in all_targets if _job_key(t) not in done]
        if skipped:
            log.info("⟲ Resume mode · skipping %d already-completed targets "
                     "(use --no-resume to redo)", len(skipped))
    else:
        targets = all_targets

    log.info("══════ START %s · run_id=%s · %d/%d targets · workers=%d ══════",
             PIPELINE_NAME, run_id, len(targets), len(all_targets), workers)

    successes: list[dict] = []
    failures:  list[dict] = []

    sf_client = None
    try:
        if not dry_run:
            sf_client = SnowflakeClient(schema_=SF_SHARED_SCHEMA)

        def _do_one(idx_and_job):
            idx, job = idx_and_job
            log.info("──[%d/%d] start · %s · %s", idx, len(targets), job["competitor"], job["url"])
            try:
                result = scrape_one_target(
                    job, run_id=run_id, dry_run=dry_run,
                    allow_selenium_fallback=allow_selenium_fallback, model=model,
                )
                if result is None:
                    if not dry_run:
                        _mark_done(run_id, job, s3_key=None)
                    return ("skip", None, job)
                copy_into_snowflake(result, sf_client)
                _mark_done(run_id, job, s3_key=result["s3_key"])
                return ("ok", result, job)
            except Exception as e:
                log.error("✗ %s · %s · failed: %s", job["competitor"], job["url"], e, exc_info=True)
                return ("err", str(e), job)

        if targets:
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                for status, payload, job in pool.map(_do_one, list(enumerate(targets, start=1))):
                    if status == "ok":
                        successes.append(payload)
                    elif status == "err":
                        failures.append({"job": job, "error": payload})

        if not dry_run and not skip_merge and successes:
            try:
                merge_clean(sf_client)
            except Exception as e:
                log.error("merge_clean failed: %s", e, exc_info=True)
                failures.append({"job": "merge_clean", "error": str(e)})

    finally:
        if sf_client is not None:
            sf_client.close()

    if not dry_run and not failures:
        _clear_progress()

    log.info("══════ END   ✓ %d ok · ✗ %d failed · %s ══════",
             len(successes), len(failures), PIPELINE_NAME)
    if failures:
        log.warning("Failures (truncated):")
        for f in failures[:10]:
            log.warning("  · %s",
                        {k: (v if k != "job" or isinstance(v, str)
                             else f"{v.get('competitor')} · {v.get('url')}")
                         for k, v in f.items()})
        sys.exit(1)

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sheet-id", default=os.getenv("COMPETITOR_SHEET_ID"),
                    help="Google Sheet ID with recurring competitor targets.")
    ap.add_argument("--sheet-worksheet", default=os.getenv("COMPETITOR_SHEET_WORKSHEET", "Sheet1"))
    ap.add_argument("--urls", help="Comma-separated list of ad-hoc URLs to scrape this run.")
    ap.add_argument("--query",
                    help="Comma-separated list of search queries — discovers candidate URLs "
                         "via Tavily search when you don't already know the competitor's page "
                         "(e.g. 'goodlife pharmacy kenya prices').")
    ap.add_argument("--search-depth", choices=["basic", "advanced"], default="advanced",
                    help="Tavily search depth for --query discovery (default advanced).")
    ap.add_argument("--max-search-results", type=int, default=5,
                    help="Max candidate URLs to pull in per --query (default 5).")
    ap.add_argument("--competitor", default="adhoc",
                    help="Competitor label applied to --urls / --query (default: adhoc).")
    ap.add_argument("--category", default=None, help="Category label applied to --urls / --query.")
    ap.add_argument("--only-competitors",
                    help="Comma-separated list to limit the Sheet run to these competitors.")
    ap.add_argument("--skip-merge", action="store_true", help="Skip the CLEAN MERGE step.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + extract only — no S3 / Snowflake writes.")
    ap.add_argument("--no-resume", action="store_true",
                    help="Ignore .competitor_pricing_progress.json and redo every target.")
    ap.add_argument("--no-selenium-fallback", action="store_true",
                    help="Fail a target outright if Tavily can't fetch it, instead of falling back.")
    ap.add_argument("--workers", type=int, default=DEFAULT_PIPELINE_WORKERS)
    ap.add_argument("--model", default=DEFAULT_OPENAI_MODEL,
                    help=f"OpenAI model for extraction (default {DEFAULT_OPENAI_MODEL}).")
    args = ap.parse_args()

    adhoc_urls = [u for u in (args.urls.split(",") if args.urls else []) if u.strip()]
    adhoc_queries = [q for q in (args.query.split(",") if args.query else []) if q.strip()]
    only_competitors = None
    if args.only_competitors:
        only_competitors = {c.strip().lower() for c in args.only_competitors.split(",") if c.strip()}

    if not args.sheet_id and not adhoc_urls and not adhoc_queries:
        ap.error("Nothing to do — pass --urls / --query, or set COMPETITOR_SHEET_ID / --sheet-id.")

    run_pipeline(
        sheet_id=args.sheet_id, sheet_worksheet=args.sheet_worksheet,
        adhoc_urls=adhoc_urls, adhoc_queries=adhoc_queries,
        adhoc_competitor=args.competitor, adhoc_category=args.category,
        only_competitors=only_competitors,
        search_depth=args.search_depth, max_search_results=args.max_search_results,
        skip_merge=args.skip_merge, dry_run=args.dry_run,
        workers=args.workers, model=args.model,
        allow_selenium_fallback=not args.no_selenium_fallback,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
