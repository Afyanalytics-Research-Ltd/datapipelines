# dags/competitor_pricing_pipeline.py
"""
Google Sheet (competitor targets) → Tavily/Selenium fetch → OpenAI structured
extraction → S3 → Snowflake HOSPITALS.SHARED.COMPETITOR_PRICING_RAW
  → MERGE → HOSPITALS.SHARED.COMPETITOR_PRICING  (daily price-history table)

Scheduled replacement for the standalone competitor_pricing_scraper.py CLI
script. Same business logic — a "universal" scraper: point it at ANY
competitor URL from the target Sheet and an LLM (function-calling, fixed
JSON schema) extracts every product + price from the fetched page content,
no per-site CSS selectors required.

Deliberately DOES NOT use the script's local `.competitor_pricing_progress.json`
resume-cache. Each scheduled run reads the full current target list from the
Sheet and reprocesses every enabled row; idempotency instead comes from:
  - the S3 key being unique per (competitor, url, day, dag run) so re-running
    a run_id never collides with a previous upload,
  - COPY INTO being scoped to that exact FILES=(...) key,
  - the CLEAN MERGE upserting on (competitor, source_url, product_name, day),
    so a retried/duplicated COPY never produces duplicate CLEAN rows.
A per-target fetch/extract failure is caught and reported as a row status
(EXTRACTED / EMPTY / FAILED) rather than raised, so one bad competitor page
cannot abort the other targets in the same run; report_failures reddens the
DAG run afterwards if anything failed, once the healthy targets have already
committed.

Not carried over from the CLI script (out of scope for a scheduled, Sheet-
driven pipeline — see report for details): --urls / --query ad-hoc targets,
Tavily-search discovery, --only-competitors, --dry-run, --no-resume.

Airflow Variables required:
  COMPETITOR_SHEET_ID          Google Sheet key with recurring competitor
                                targets (columns: competitor, url, category
                                [optional], enabled [optional, TRUE/FALSE])
  COMPETITOR_SHEET_WORKSHEET   Worksheet tab name (default: Sheet1)
  GOOGLE_SA_JSON                Google service-account credentials JSON

Airflow Connections required:
  aws_default   S3 (bucket: collabmedbucket) — used via S3Hook

Env vars (from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE  SNOWFLAKE_PRIVATE_KEY_PATH
  TAVILY_API_KEY                 page fetch (Tavily Extract API)
  OPENAI_API_KEY                 structured price extraction
  OPENAI_MODEL                   optional, default gpt-4.1
  MAX_CONTENT_CHARS              optional, default 20000
  COMPETITOR_ALLOW_SELENIUM_FALLBACK   optional, default "true" — falls back
                                  to a headless Selenium fetch when Tavily
                                  fails/returns too little content. Requires
                                  Chrome + chromedriver on the worker image;
                                  set to "false" to disable if not present.
  CHROME_BINARY_LOCATION / CHROMEDRIVER_PATH   optional, for the Selenium
                                  fallback above.

Required Snowflake objects (create once, out of band — this DAG never runs
DDL):
  CREATE TABLE HOSPITALS.SHARED.COMPETITOR_PRICING_RAW (
      competitor STRING, source_url STRING, category STRING,
      fetch_method STRING, scraped_at TIMESTAMP_TZ, payload VARIANT
  );
  CREATE TABLE HOSPITALS.SHARED.COMPETITOR_PRICING (
      competitor STRING, product_name STRING, sku STRING, brand STRING,
      current_price FLOAT, original_price FLOAT, currency STRING,
      discount_percentage FLOAT, in_stock BOOLEAN, product_url STRING,
      unit STRING, source_url STRING, category STRING, scraped_at TIMESTAMP_TZ
  );
Reuses the same external stage + file format as the facility pipelines
(HOSPITALS.SHARED.FACILITY_RAW_STAGE / JSON_FF) — both just COPY gzipped
JSONL out of the same S3 bucket.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import gspread
import snowflake.connector
from dotenv import load_dotenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "competitor_pricing_pipeline"

S3_CONN_ID = "aws_default"
S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/competitor_pricing"

SF_DB = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"
RAW_TABLE = f"{SF_DB}.{SF_SHARED_SCHEMA}.COMPETITOR_PRICING_RAW"
CLEAN_TABLE = f"{SF_DB}.{SF_SHARED_SCHEMA}.COMPETITOR_PRICING"

MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", "20000"))
MIN_TAVILY_CONTENT_CHARS = 200  # below this, treat Tavily's result as a failed fetch
DEFAULT_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")
ALLOW_SELENIUM_FALLBACK = os.getenv(
    "COMPETITOR_ALLOW_SELENIUM_FALLBACK", "true"
).strip().lower() != "false"

# Cap on mapped scrape tasks running at once — Selenium fallback is heavy,
# keep this modest (mirrors PIPELINE_WORKERS default in the CLI script).
MAX_ACTIVE_SCRAPE_TASKS = int(os.getenv("PIPELINE_WORKERS", "6"))


# ── Snowflake client (key-pair auth required for COPY INTO / MERGE) ──────
class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )

    def close(self):
        if self._conn:
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

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid}
        log.info("✓ %-28s | rowcount=%s · %.2fs", label, result["rowcount"],
                 time.perf_counter() - t0)
        return result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


# ── Google Sheet (competitor targets) ─────────────────────────────────────
def _gsheet_client():
    return gspread.service_account_from_dict(json.loads(Variable.get("GOOGLE_SA_JSON")))


def _read_sheet(spreadsheet_id: str, worksheet: str) -> list[dict]:
    ws = _gsheet_client().open_by_key(spreadsheet_id).worksheet(worksheet)
    return ws.get_all_records()


def _normalize_url(url: str) -> str:
    """Chrome (and Tavily) reject a schemeless URL like 'goodlife.com' with an
    opaque 'invalid argument' error — default bare domains to https://."""
    url = (url or "").strip()
    if url and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", url):
        url = f"https://{url}"
    return url


def _safe_s3_token(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())


# ── Fetch (Tavily primary, Selenium fallback) ─────────────────────────────
_BOT_BLOCK_SIGNATURES = (
    "attention required", "you have been blocked", "please enable cookies",
    "checking your browser", "just a moment", "verify you are human",
    "are you a robot", "unusual traffic", "access denied", "captcha",
    "cf-error", "ray id",
)


def _looks_like_bot_block(content: str) -> bool:
    head = content[:2000].lower()
    return any(sig in head for sig in _BOT_BLOCK_SIGNATURES)


def _fetch_via_tavily(url: str) -> str | None:
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        log.warning("TAVILY_API_KEY not set — skipping Tavily fetch for %s", url)
        return None
    from tavily import TavilyClient
    client = TavilyClient(api_key=api_key)
    try:
        result = client.extract(urls=[url])
    except Exception as e:
        log.warning("Tavily extract failed for %s: %s", url, e)
        return None

    for r in result.get("results", []):
        content = (r.get("raw_content") or "").strip()
        if len(content) >= MIN_TAVILY_CONTENT_CHARS:
            if _looks_like_bot_block(content):
                log.warning("Tavily fetch for %s looks like a bot-block page — falling back", url)
                return None
            return content
        break

    failed = result.get("failed_results") or []
    if failed:
        log.warning("Tavily could not fetch %s: %s", url, failed[0].get("error"))
    else:
        log.warning("Tavily returned too little content for %s — falling back", url)
    return None


_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}};
"""


def _fetch_via_selenium(url: str, timeout: int = 30) -> str | None:
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
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _STEALTH_JS})
        except Exception:
            pass
        driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        if len(text) < MIN_TAVILY_CONTENT_CHARS:
            return None
        if _looks_like_bot_block(text):
            log.warning("Selenium fetch for %s looks like a bot-block page", url)
            return None
        return text
    finally:
        driver.quit()


def _fetch_page_content(url: str) -> tuple[str, str]:
    """Returns (content, fetch_method). Raises if every method fails."""
    content = _fetch_via_tavily(url)
    if content:
        return content, "tavily"

    if not ALLOW_SELENIUM_FALLBACK:
        raise RuntimeError(f"Tavily fetch failed for {url} and Selenium fallback is disabled")

    log.info("Falling back to Selenium for %s", url)
    content = _fetch_via_selenium(url)
    if content:
        return content, "selenium"

    raise RuntimeError(f"Both Tavily and Selenium fallback failed to fetch {url}")


# ── Structured extraction (OpenAI function-calling, fixed schema) ────────
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


def _extract_products_with_openai(content: str, *, competitor: str, url: str, model: str) -> list[dict]:
    import openai
    client = openai.OpenAI()  # reads OPENAI_API_KEY
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


# ── DAG task callables ────────────────────────────────────────────────────
def prepare_targets(**context) -> list[dict]:
    """Build one scrape job per enabled (competitor, url) row in the target
    Sheet, deduped by (competitor.lower(), url). Returns a list of
    {"job": {...}} dicts for dynamic task mapping."""
    sheet_id = Variable.get("COMPETITOR_SHEET_ID")
    sheet_tab = Variable.get("COMPETITOR_SHEET_WORKSHEET", default_var="Sheet1")
    rows = _read_sheet(sheet_id, sheet_tab)

    seen: set[tuple] = set()
    jobs: list[dict] = []
    for r in rows:
        competitor = (r.get("competitor") or "").strip()
        url = _normalize_url(r.get("url") or "")
        enabled = str(r.get("enabled", "TRUE")).strip().upper()
        if not competitor or not url or enabled == "FALSE":
            continue
        key = (competitor.lower(), url)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({
            "job": {
                "competitor": competitor,
                "url": url,
                "category": (r.get("category") or "").strip() or None,
            }
        })

    log.info("Prepared %d competitor-pricing scrape targets from sheet=%s/%s",
             len(jobs), sheet_id, sheet_tab)
    return jobs


def scrape_and_upload(job: dict, **context) -> dict:
    """Fetch one target's page, extract products via OpenAI, upload the rows
    as gzipped JSONL to S3. Never raises — failures are reported via the
    returned status so one bad target cannot abort the rest of the run."""
    competitor = job["competitor"]
    url = job["url"]
    category = job.get("category")
    run_id = context["run_id"]
    base = {"competitor": competitor, "source_url": url, "category": category}

    try:
        content, fetch_method = _fetch_page_content(url)
        log.info("%s · %s — fetched %d chars via %s · preview: %r",
                 competitor, url, len(content), fetch_method, content[:160].replace("\n", " "))

        products = _extract_products_with_openai(
            content, competitor=competitor, url=url, model=DEFAULT_OPENAI_MODEL,
        )

        if not products:
            log.info("%s · %s — 0 products extracted (method=%s, content_chars=%d)",
                     competitor, url, fetch_method, len(content))
            return {**base, "status": "EMPTY", "fetch_method": fetch_method,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "s3_key": None, "row_count": 0}

        scraped_at = datetime.now(timezone.utc)
        rows = [
            {
                **p,
                "competitor": competitor,
                "source_url": url,
                "category": category,
                "fetch_method": fetch_method,
                "scraped_at": scraped_at.isoformat(),
            }
            for p in products
        ]

        dt = scraped_at.date().isoformat()
        comp_safe = _safe_s3_token(competitor)
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        key = (
            f"{S3_PREFIX}/"
            f"competitor={comp_safe or 'unknown'}/"
            f"dt={dt}/"
            f"{url_hash}__{run_id}.jsonl.gz"
        )

        jsonl_bytes = b"\n".join(
            json.dumps(row, separators=(",", ":"), default=str).encode("utf-8") for row in rows
        ) + b"\n"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(jsonl_bytes)

        S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
            bytes_data=buf.getvalue(), key=key, bucket_name=S3_BUCKET, replace=True,
        )
        log.info("Uploaded s3://%s/%s products=%d (method=%s)",
                 S3_BUCKET, key, len(products), fetch_method)

        return {**base, "status": "EXTRACTED", "fetch_method": fetch_method,
                "scraped_at": scraped_at.isoformat(), "s3_key": key,
                "row_count": len(products)}

    except Exception as exc:
        log.error("scrape failed %s · %s: %s", competitor, url, exc, exc_info=True)
        return {**base, "status": "FAILED", "fetch_method": None,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "s3_key": None, "row_count": 0, "error": str(exc)[:2000]}


def copy_into_raw(**job_result) -> dict:
    """COPY one S3 file into COMPETITOR_PRICING_RAW. Skipped for targets that
    produced no rows (EMPTY) or failed to scrape (FAILED)."""
    status = job_result.get("status")
    competitor = job_result["competitor"]
    source_key = f"{competitor}::{job_result['source_url']}"

    if status != "EXTRACTED":
        log.info("skip copy for %s (status=%s)", source_key, status)
        return {"competitor": competitor, "status": status}

    competitor_sql = competitor.replace("'", "''")
    source_url = job_result["source_url"].replace("'", "''")
    category = (job_result.get("category") or "").replace("'", "''")
    fetch_method = job_result["fetch_method"]
    scraped_at = job_result["scraped_at"]
    s3_key = job_result["s3_key"]

    sql = f"""
    COPY INTO {RAW_TABLE} (competitor, source_url, category, fetch_method, scraped_at, payload)
    FROM (
      SELECT
        '{competitor_sql}'::STRING    AS competitor,
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
    try:
        with SnowflakeClient(schema_=SF_SHARED_SCHEMA) as sf:
            sf.execute(sql, label=f"copy:{competitor}")
        return {"competitor": competitor, "status": "COPIED"}
    except Exception as exc:
        log.error("copy failed for %s: %s", source_key, exc, exc_info=True)
        return {"competitor": competitor, "status": "FAILED", "error": str(exc)[:2000]}


def merge_clean(**context) -> None:
    """MERGE RAW → CLEAN, one row per (competitor, source_url, product_name,
    day). Runs unconditionally (ALL_DONE) — a day with no new successful
    copies is simply a no-op MERGE."""
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
    with SnowflakeClient(schema_=SF_SHARED_SCHEMA) as sf:
        sf.execute(sql, label="merge_clean:competitor_pricing")


def report_failures(**context) -> None:
    """Fail the DAG run if any target failed to scrape or copy, AFTER the
    healthy targets have already committed via merge_clean. The repo has no
    dedicated alerting for this pipeline, so a red run is the signal."""
    ti = context["ti"]
    scrape_results = ti.xcom_pull(task_ids="scrape_and_upload") or []
    copy_results = ti.xcom_pull(task_ids="copy_into_raw") or []

    failed_scrapes = [r for r in scrape_results if r.get("status") == "FAILED"]
    failed_copies = [r for r in copy_results if r.get("status") == "FAILED"]

    if failed_scrapes:
        for r in failed_scrapes[:20]:
            log.error("FAILED scrape %s · %s :: %s",
                      r.get("competitor"), r.get("source_url"), (r.get("error") or "")[:300])
    if failed_copies:
        for r in failed_copies[:20]:
            log.error("FAILED copy %s :: %s", r.get("competitor"), (r.get("error") or "")[:300])

    if failed_scrapes or failed_copies:
        raise RuntimeError(
            f"{len(failed_scrapes)} scrape failure(s), {len(failed_copies)} copy failure(s) "
            f"this run — see task logs above."
        )
    log.info("All %d competitor-pricing targets scraped/copied successfully", len(scrape_results))


# ── DAG definition ─────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    tags=["competitor_pricing", "scraper", "llm", "snowflake", "s3"],
) as dag:

    t_prepare = PythonOperator(
        task_id="prepare_targets",
        python_callable=prepare_targets,
    )
    t_scrape = PythonOperator.partial(
        task_id="scrape_and_upload",
        python_callable=scrape_and_upload,
        trigger_rule=TriggerRule.ALL_DONE,
        max_active_tis_per_dag=MAX_ACTIVE_SCRAPE_TASKS,
    ).expand(op_kwargs=t_prepare.output)

    t_copy = PythonOperator.partial(
        task_id="copy_into_raw",
        python_callable=copy_into_raw,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_scrape.output)

    t_merge = PythonOperator(
        task_id="merge_clean",
        python_callable=merge_clean,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    t_report = PythonOperator(
        task_id="report_failures",
        python_callable=report_failures,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_prepare >> t_scrape >> t_copy >> t_merge >> t_report
