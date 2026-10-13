"""
v3_to_v3_client.py — a gateway client bound to ONE V3 instance + tenant, so a
V3 → V3 copy can hold two at once (the v2_to_v3 module keeps a single global
login). Settings per side come from env, keyed like the other V3 DAGs:

  AFYA_<KEY>_USERNAME / _PASSWORD          the account (its org IS the tenant)
  AFYA_<KEY>_URL_TEMPLATE                   e.g. https://{service}.afyaanalytics.ai/api/
  AFYA_<KEY>_<SERVICE>_URL                  per-service override
  AFYA_<KEY>_ORGANIZATION_ID / _FACILITY_ID expected tenant (login must agree)
  AFYA_<KEY>_MIGRATION_KEY                  X-Migration-Key for theatre / dialysis
                                            (falls back to MODEL_GATEWAY_MIGRATION_KEY)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time

import requests

log = logging.getLogger(__name__)

SERVICES = ("core", "reception", "evaluation", "inventory", "finance", "theatre", "inpatient", "dialysis")
DEFAULT_URL_TEMPLATE = "https://{service}migrate.afyaanalytics.ai/api/"
MIGRATION_KEY_SERVICES = {"theatre", "dialysis"}
TOKEN_TTL = 50 * 60


class GatewayError(RuntimeError):
    pass


class V3Client:
    def __init__(self, key: str, *, urls: dict[str, str] | None = None, username: str | None = None,
                 password: str | None = None, organization_id: int | None = None,
                 facility_id: int | None = None, migration_key: str | None = None,
                 session: requests.Session | None = None):
        self.key = key
        up = key.upper()
        env = lambda name: (os.getenv(f"AFYA_{up}_{name}") or "").strip()
        tmpl = env("URL_TEMPLATE") or DEFAULT_URL_TEMPLATE
        self.urls = dict(urls or {s: env(f"{s.upper()}_URL") or tmpl.format(service=s) for s in SERVICES})
        self.username = username or env("USERNAME")
        self.password = password or env("PASSWORD")
        org, fac = env("ORGANIZATION_ID"), env("FACILITY_ID")
        self.organization_id = organization_id if organization_id is not None else (int(org) if org.isdigit() else None)
        self.facility_id = facility_id if facility_id is not None else (int(fac) if fac.isdigit() else None)
        self.migration_key = migration_key or env("MIGRATION_KEY") or (os.getenv("MODEL_GATEWAY_MIGRATION_KEY") or "").strip()
        self.user_id: int | None = None
        self._session = session or requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._token: tuple[str, float] | None = None
        self._lock = threading.Lock()

    def __repr__(self):
        return f"V3Client({self.key}: org {self.organization_id} / facility {self.facility_id} @ {self.urls['core']})"

    # ── auth ──────────────────────────────────────────────────────────────
    def login(self) -> str:
        if not self.username or not self.password:
            raise GatewayError(f"{self.key}: set AFYA_{self.key.upper()}_USERNAME / _PASSWORD")
        url = f"{self.urls['core'].rstrip('/')}/v1/login"
        body = {"username": self.username, "password": self.password}
        r = self._session.post(url, json=body, timeout=30)
        if r.status_code != 200:
            raise GatewayError(f"{self.key}: login failed {r.status_code} {r.text[:200]}")
        res = r.json()
        if res.get("step") == 2:                      # several facilities: name ours
            allowed = res.get("allowed_facility_ids") or []
            if self.facility_id is not None and self.facility_id not in allowed:
                raise GatewayError(f"{self.key}: account can't log into facility {self.facility_id} ({allowed})")
            body["facility_id"] = self.facility_id if self.facility_id is not None else allowed[0]
            r = self._session.post(url, json=body, timeout=30)
            if r.status_code != 200:
                raise GatewayError(f"{self.key}: login (step 2) failed {r.status_code} {r.text[:200]}")
            res = r.json()
        token = res.get("access_token")
        if not token:
            raise GatewayError(f"{self.key}: no access_token in login response")
        user = res.get("user") or res.get("data") or {}
        org = (res.get("tenant") or {}).get("id") or user.get("organization_id")
        fac = ((res.get("facility") or {}).get("id") or (res.get("allowed_facility_ids") or [None])[0]
               or user.get("facility_id"))
        # the tenant comes from the account: a wrong account would silently
        # read or write another organisation — refuse instead
        if self.organization_id is not None and org != self.organization_id:
            raise GatewayError(f"{self.key}: account belongs to organization {org}, expected {self.organization_id}")
        self.organization_id = org
        if self.facility_id is None:
            self.facility_id = fac
        self.user_id = user.get("id")
        log.info("%s: login ok — org %s facility %s", self.key, org, self.facility_id)
        return token

    def token(self) -> str:
        with self._lock:
            if not self._token or time.time() - self._token[1] > TOKEN_TTL:
                self._token = (self.login(), time.time())
            return self._token[0]

    def _headers(self, service: str) -> dict:
        h = {"Content-Type": "application/json", "Accept": "application/json",
             "Authorization": f"Bearer {self.token()}"}
        if service in MIGRATION_KEY_SERVICES and self.migration_key:
            h["X-Migration-Key"] = self.migration_key
        return h

    # ── gateway ───────────────────────────────────────────────────────────
    def post(self, service: str, body: dict, *, timeout: int = 120, retries: int = 5) -> requests.Response:
        url = f"{self.urls[service].rstrip('/')}/v1/gateway"
        data = json.dumps(body, default=str).encode()
        wait = 5
        for attempt in range(1, retries + 1):
            try:
                r = self._session.post(url, headers=self._headers(service), data=data, timeout=timeout)
            except requests.RequestException as e:
                if attempt == retries:
                    raise
                log.warning("%s %s: %s — retry in %ss", self.key, service, e, wait)
                time.sleep(wait)
                wait = min(wait * 2, 60)
                continue
            if r.status_code == 401 and attempt < retries:
                with self._lock:
                    self._token = None
                continue
            if r.status_code in (429, 502, 503, 504) and attempt < retries:
                time.sleep(wait)
                wait = min(wait * 2, 60)
                continue
            return r
        return r

    def models(self, service: str) -> list[dict]:
        r = self.post(service, {"action": "list"}, timeout=60)
        if not r.ok:
            raise GatewayError(f"{self.key} {service} list: {r.status_code} {r.text[:200]}")
        return [e for e in (r.json().get("data") or []) if e.get("alias")]

    def describe(self, service: str, alias: str) -> dict:
        r = self.post(service, {"action": "describe", "model": alias}, timeout=60)
        if not r.ok:
            raise GatewayError(f"{self.key} {service}/{alias} describe: {r.status_code} {r.text[:200]}")
        return r.json().get("data") or {}

    def read_page(self, service: str, alias: str, page: int, per_page: int = 500) -> tuple[list[dict], dict]:
        self.token()                                  # the tenant id comes from the login
        body = {"action": "read", "model": alias, "source_tenant_id": self.organization_id,
                "per_page": per_page, "page": page}
        r = self.post(service, body, timeout=120)
        if not r.ok:
            raise GatewayError(f"{self.key} {service}/{alias} read p{page}: {r.status_code} {r.text[:200]}")
        j = r.json()
        rows = j.get("data") or []
        if isinstance(rows, dict):
            rows = rows.get("data") or list(rows.values())
        return rows, (j.get("meta") or {})

    def count(self, service: str, alias: str) -> int | None:
        _, meta = self.read_page(service, alias, 1, per_page=1)
        return meta.get("total")

    def read_all(self, service: str, alias: str, per_page: int = 500):
        """Every row this tenant has, oldest id first, page by page (generator)."""
        page = 1
        while True:
            rows, meta = self.read_page(service, alias, page, per_page)
            yield from rows
            last = meta.get("last_page")
            if not rows or len(rows) < per_page or (last and page >= last):
                return
            page += 1

    def insert(self, service: str, alias: str, data: dict, *, match_on: str | None = "uuid") -> requests.Response:
        self.token()
        body = {"action": "insert", "model": alias, "destination_tenant_id": self.organization_id, "data": data}
        if match_on and data.get(match_on):
            body["match_on"] = match_on
        return self.post(service, body, timeout=120)
