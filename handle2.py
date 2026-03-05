import os
import re
import hmac
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Markdown -> HTML -> PDF
import markdown as md
from weasyprint import HTML


# -----------------------------
# Config
# -----------------------------
load_dotenv()

VA_API_KEY = os.getenv("VA_API_KEY")
if not VA_API_KEY:
    raise RuntimeError("Missing VA_API_KEY in environment/.env")

VA_AUTH_SCHEME = os.getenv("VA_AUTH_SCHEME", "Basic").strip()  # "Basic" or "Bearer"
LANDING_BASE_URL = os.getenv("LANDING_BASE_URL", "https://api.va.landing.ai").rstrip("/")

ANON_SALT = os.getenv("ANON_SALT", "CHANGE_ME_TO_A_LONG_RANDOM_SECRET")

TZ = ZoneInfo("Africa/Nairobi")
TODAY_LOCAL = datetime.now(TZ).date()

OUT_DIR = Path("/mnt/c/Users/luthe/Documents/landingai_today_exports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 120


# -----------------------------
# Helpers: time parsing
# -----------------------------
def epoch_to_datetime_local(epoch: int) -> datetime:
    """
    LandingAI docs show received_at as an integer. In practice it can be seconds or ms.
    We'll detect by magnitude.
    """
    if epoch > 10_000_000_000:  # likely ms
        dt_utc = datetime.fromtimestamp(epoch / 1000, tz=timezone.utc)
    else:  # likely seconds
        dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt_utc.astimezone(TZ)


# -----------------------------
# Helpers: crypto anonymization
# -----------------------------
def hmac_token(value: str, prefix: str, length: int = 10) -> str:
    msg = value.strip().encode("utf-8")
    key = ANON_SALT.encode("utf-8")
    digest = hmac.new(key, msg, hashlib.sha256).hexdigest().upper()
    return f"{prefix}-{digest[:length]}"


def anonymize_markdown(markdown_text: str) -> str:
    """
    Replaces:
      IP NO: <value>  -> IP NO: IP-XXXXXXXXXX
      NAME: <value>   -> NAME: NM-XXXXXXXXXX

    This is conservative: it only replaces "IP NO:" and "NAME:" lines/segments.
    """
    ip_pattern = re.compile(r"(?im)\bIP\s*NO\s*:\s*([^\n\r]+)")
    name_pattern = re.compile(r"(?im)\bNAME\s*:\s*([^\n\r]+)")

    def ip_repl(m):
        original = m.group(1).strip()
        return f"IP NO: {hmac_token(original, 'IP', 10)}"

    def name_repl(m):
        original = m.group(1).strip()
        return f"NAME: {hmac_token(original, 'NM', 10)}"

    out = ip_pattern.sub(ip_repl, markdown_text)
    out = name_pattern.sub(name_repl, out)
    return out


# -----------------------------
# LandingAI API calls
# -----------------------------
def auth_headers() -> dict:
    return {"Authorization": f"{VA_AUTH_SCHEME} {VA_API_KEY}"}


def list_parse_jobs(status: str = "completed", page_size: int = 100):
    """
    GET /v1/ade/parse/jobs?page=0&pageSize=100&status=completed
    """
    jobs = []
    page = 0

    while True:
        url = f"{LANDING_BASE_URL}/v1/ade/parse/jobs"
        params = {"page": page, "pageSize": page_size}
        if status:
            params["status"] = status

        r = requests.get(url, headers=auth_headers(), params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()

        batch = data.get("jobs", []) or []
        jobs.extend(batch)

        has_more = bool(data.get("has_more", False))
        if not has_more or not batch:
            break

        page += 1

    return jobs


def get_job(job_id: str) -> dict:
    """
    GET /v1/ade/parse/jobs/{job_id}
    """
    url = f"{LANDING_BASE_URL}/v1/ade/parse/jobs/{job_id}"
    r = requests.get(url, headers=auth_headers(), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_markdown_from_job(job_json: dict) -> str:
    """
    Prefer job_json["data"]["markdown"].
    If output_url exists (large result), fetch it and then read markdown.
    """
    data = job_json.get("data") or {}
    markdown_text = data.get("markdown")
    if isinstance(markdown_text, str) and markdown_text.strip():
        return markdown_text

    output_url = job_json.get("output_url")
    if output_url:
        # output_url is a presigned URL that can expire; each GET job regenerates it (per docs).
        rr = requests.get(output_url, timeout=TIMEOUT)
        rr.raise_for_status()
        big = rr.json()
        # big should be a parse response-like JSON containing markdown
        md2 = (big.get("markdown")
               or (big.get("data") or {}).get("markdown"))
        if isinstance(md2, str) and md2.strip():
            return md2

    raise RuntimeError("No markdown found in job response (data.markdown missing and output_url empty).")


# -----------------------------
# Markdown -> PDF
# -----------------------------
def markdown_to_pdf(md_text: str, pdf_path: Path, title: str = "Anonymized Export"):
    """
    Convert markdown to HTML then to PDF using WeasyPrint.
    """
    html_body = md.markdown(md_text, extensions=["tables", "fenced_code", "toc"])
    html_full = f"""
    <html>
      <head>
        <meta charset="utf-8"/>
        <style>
          body {{ font-family: Arial, sans-serif; font-size: 12px; line-height: 1.4; }}
          h1,h2,h3 {{ margin: 0.6em 0 0.3em; }}
          code, pre {{ font-family: monospace; font-size: 11px; }}
          pre {{ white-space: pre-wrap; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #ddd; padding: 6px; vertical-align: top; }}
          th {{ background: #f3f3f3; }}
          .title {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; }}
          .meta {{ color: #666; margin-bottom: 16px; }}
        </style>
      </head>
      <body>
        <div class="title">{title}</div>
        <div class="meta">Generated: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}</div>
        {html_body}
      </body>
    </html>
    """
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_full).write_pdf(str(pdf_path))


# -----------------------------
# Main
# -----------------------------
def main():
    print(f"Timezone: {TZ}")
    print(f"Filtering jobs for date (local): {TODAY_LOCAL}")
    print(f"Output folder: {OUT_DIR.resolve()}")

    jobs = list_parse_jobs(status="completed", page_size=100)
    print(f"Found {len(jobs)} completed jobs in account.")

    # Filter "received today"
    todays = []
    for j in jobs:
        received_at = j.get("received_at")
        if received_at is None:
            continue
        try:
            dt_local = epoch_to_datetime_local(int(received_at))
        except Exception:
            continue

        if dt_local.date() == TODAY_LOCAL:
            todays.append((j.get("job_id"), dt_local))

    todays.sort(key=lambda x: x[1])

    print(f"Jobs received today: {len(todays)}")
    if not todays:
        return

    for idx, (job_id, dt_local) in enumerate(todays, start=1):
        # if idx == 2: break
        if not job_id:
            continue

        try:
            print(f"\n[{idx}/{len(todays)}] Downloading job_id={job_id} received={dt_local.isoformat()}")

            job = get_job(job_id)

            # Extract filename if available
            filename = None
            md_title = f"LandingAI Job {job_id}"
            meta = job.get("metadata") or (job.get("data") or {}).get("metadata") or {}
            if isinstance(meta, dict):
                filename = meta.get("filename")
                if filename:
                    md_title = f"{filename} (job {job_id})"

            markdown_text = get_markdown_from_job(job)

            anonymized_md = anonymize_markdown(markdown_text)

            # Save .md
            safe_name = (filename or f"job_{job_id}").replace("/", "_").replace("\\", "_")
            stem = f"{dt_local.strftime('%Y%m%d_%H%M%S')}_{safe_name}"
            md_path = OUT_DIR / f"{stem}.md"
            pdf_path = OUT_DIR / f"{stem}.pdf"

            md_path.write_text(anonymized_md, encoding="utf-8")
            print(f"✅ Saved MD: {md_path.name}")

            # Convert to PDF
            markdown_to_pdf(anonymized_md, pdf_path, title=f"Anonymized: {md_title}")
            print(f"✅ Saved PDF: {pdf_path.name}")

        except Exception as e:
            print(f"❌ Failed job {job_id}: {e}")


if __name__ == "__main__":
    main()