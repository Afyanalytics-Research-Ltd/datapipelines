#!/usr/bin/env python3
"""
competitor_pricing_agent.py  —  conversational scraping agent (CLI).

  Modeled on techwithtim/BDAIScraperAgent's pattern (a chat agent that
  decides what to search/scrape and answers conversationally) but applied
  to competitor pricing instead of travel, and run as a terminal chat loop
  instead of a Streamlit app.

  Unlike competitor_pricing_scraper.py (a batch pipeline: fixed target
  list → S3 → Snowflake), this is a tool-calling LLM agent you talk to.
  You ask a question in plain language; the model decides for itself
  whether to check its memory, search the web, scrape a page, and/or
  save what it found — there's no fixed pipeline of steps.

  Tools available to the model:
    - recall_products : semantic search over previously saved results
                        (ChromaDB, persisted locally — this is the
                        "memory" the model should check before re-scraping)
    - search_web       : Bright Data SERP search → candidate URLs
    - scrape_page       : Bright Data Web Unlocker → cleaned page text
    - save_products    : the model extracts pricing itself from whatever
                        page text it just read, and calls this to persist
                        it into ChromaDB

USAGE
  python competitor_pricing_agent.py
  > what's the cheapest paracetamol at goodlife pharmacy right now?
  > has jumia dropped iphone prices this week?
  > (type 'exit' or 'quit' to end the session)

ENV VARS  (put them in a `.env` file next to this script — auto-loaded)
  # LLM — pick one via LLM_PROVIDER
  LLM_PROVIDER=ollama                 # or "anthropic"
  OLLAMA_MODEL=llama3.1                # must be a tool-calling-capable model
  OLLAMA_HOST=http://localhost:11434   # default if unset
  ANTHROPIC_API_KEY=...                # only needed if LLM_PROVIDER=anthropic
  ANTHROPIC_MODEL=claude-sonnet-5
  ANTHROPIC_WORKSPACE_ID=...           # only needed if your API key is
                                        # "identity-linked" (tied to your
                                        # claude.ai login) rather than a
                                        # classic standalone key — find it
                                        # under Settings > Workspaces in the
                                        # Anthropic console; the request
                                        # will otherwise 400 asking for it

  # Bright Data — two zones: a Web Unlocker zone (renders/unblocks a given
  # URL) and a SERP zone (search-engine results). Get the proxy connection
  # string for each from your Bright Data dashboard (Proxies & Scraping
  # Infrastructure → your zone → "Access parameters").
  BRIGHTDATA_UNLOCKER_PROXY=http://brd-customer-XXXX-zone-unlocker:PASS@brd.superproxy.io:33335
  BRIGHTDATA_SERP_PROXY=http://brd-customer-XXXX-zone-serp:PASS@brd.superproxy.io:33335
  # If you only have one zone, set BRIGHTDATA_SERP_PROXY to the same value.

  # Memory
  CHROMA_PERSIST_DIR=.competitor_pricing_memory   # local dir, created on first run
  MAX_CONTENT_CHARS=20000

KNOWN ROUGH EDGES (read before relying on this)
  - Bright Data's Web Unlocker proxy MITMs the TLS connection to unblock
    pages, which is why requests here use verify=False — you're trusting
    Bright Data's proxy with plaintext traffic to whatever site you scrape.
    That's the standard/documented way to use this product, but it's worth
    knowing rather than assuming.
  - The SERP JSON response is parsed defensively (`organic`/`organic_results`,
    `link`/`url`, `description`/`snippet`) because the exact field names can
    vary by Bright Data API version/zone config — if search_web returns
    nothing useful, run with LOG_LEVEL=DEBUG and check the raw response.
  - Ollama tool-calling requires a model that actually supports it (e.g.
    llama3.1, qwen2.5, mistral-nemo). Older/smaller models will just ignore
    the tools and reply in plain text.
  - First run downloads ChromaDB's small default embedding model (~80MB).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── LOGGING ─────────────────────────────────────────────────────────────

log = logging.getLogger("competitor_pricing_agent")
if not log.handlers:
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(name)s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").strip().lower()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5")

BRIGHTDATA_UNLOCKER_PROXY = os.getenv("BRIGHTDATA_UNLOCKER_PROXY")
BRIGHTDATA_SERP_PROXY     = os.getenv("BRIGHTDATA_SERP_PROXY") or BRIGHTDATA_UNLOCKER_PROXY

CHROMA_PERSIST_DIR  = os.getenv("CHROMA_PERSIST_DIR", str(Path(__file__).resolve().parent / ".competitor_pricing_memory"))
MAX_CONTENT_CHARS   = int(os.getenv("MAX_CONTENT_CHARS", "20000"))
MIN_CONTENT_CHARS   = 200

_BOT_BLOCK_SIGNATURES = (
    "attention required", "you have been blocked", "please enable cookies",
    "checking your browser", "just a moment", "verify you are human",
    "are you a robot", "unusual traffic", "access denied", "captcha",
)

# ─── MEMORY  (ChromaDB) ───────────────────────────────────────────────────

_chroma_collection = None

def _get_collection():
    global _chroma_collection
    if _chroma_collection is None:
        import chromadb
        client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
        _chroma_collection = client.get_or_create_collection("competitor_pricing")
    return _chroma_collection

def save_products(competitor: str, source_url: str, products: list[dict]) -> str:
    """Persist extracted product/price rows into the local vector memory."""
    if not products:
        return "No products given — nothing saved."

    coll = _get_collection()
    scraped_at = datetime.now(timezone.utc).isoformat()
    ids, docs, metas = [], [], []

    for p in products:
        name = (p.get("product_name") or "").strip()
        if not name:
            continue
        record_id = hashlib.md5(f"{competitor}|{source_url}|{name}".encode()).hexdigest()
        price = p.get("current_price")
        currency = p.get("currency") or ""
        doc = f"{competitor} — {name} — {price} {currency}".strip()
        meta = {
            "competitor": competitor, "source_url": source_url,
            "product_name": name, "sku": p.get("sku") or "",
            "brand": p.get("brand") or "",
            "current_price": price if isinstance(price, (int, float)) else 0.0,
            "original_price": p.get("original_price") if isinstance(p.get("original_price"), (int, float)) else 0.0,
            "currency": currency,
            "discount_percentage": p.get("discount_percentage") if isinstance(p.get("discount_percentage"), (int, float)) else 0.0,
            "in_stock": bool(p.get("in_stock")) if p.get("in_stock") is not None else True,
            "product_url": p.get("product_url") or "",
            "unit": p.get("unit") or "",
            "scraped_at": scraped_at,
        }
        ids.append(record_id)
        docs.append(doc)
        metas.append(meta)

    if not ids:
        return "No valid products given — nothing saved."

    coll.upsert(ids=ids, documents=docs, metadatas=metas)
    log.info("Saved %d product(s) for %s → memory", len(ids), competitor)
    return f"Saved {len(ids)} product(s) for {competitor} to memory."

def recall_products(query: str, competitor: str | None = None, n_results: int = 5) -> list[dict]:
    """Semantic search over previously saved products — check this before
    scraping again; it may already have a recent enough answer."""
    coll = _get_collection()
    where = {"competitor": competitor} if competitor else None
    try:
        result = coll.query(query_texts=[query], n_results=n_results, where=where)
    except Exception as e:
        log.warning("Chroma query failed: %s", e)
        return []
    metas = (result.get("metadatas") or [[]])[0]
    return metas

# ─── FETCH  (Bright Data) ─────────────────────────────────────────────────

def _brightdata_get(url: str, proxy_url: str, *, timeout: int = 60) -> str:
    proxies = {"http": proxy_url, "https": proxy_url}
    resp = requests.get(url, proxies=proxies, timeout=timeout, verify=False)
    resp.raise_for_status()
    return resp.text

def _looks_like_bot_block(content: str) -> bool:
    head = content[:2000].lower()
    return any(sig in head for sig in _BOT_BLOCK_SIGNATURES)

def search_web(query: str, max_results: int = 5) -> list[dict]:
    """Bright Data SERP zone — Google search results for `query`."""
    if not BRIGHTDATA_SERP_PROXY:
        return [{"error": "BRIGHTDATA_SERP_PROXY is not set — cannot search."}]

    from urllib.parse import quote
    google_url = f"https://www.google.com/search?q={quote(query)}&brd_json=1"
    try:
        raw = _brightdata_get(google_url, BRIGHTDATA_SERP_PROXY)
    except Exception as e:
        log.warning("Bright Data SERP request failed for %r: %s", query, e)
        return [{"error": f"Search request failed: {e}"}]

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("SERP response wasn't JSON (zone may not have brd_json enabled) — "
                    "set LOG_LEVEL=DEBUG to inspect the raw response.")
        log.debug("Raw SERP response: %.2000s", raw)
        return [{"error": "Search response wasn't structured JSON — check zone config."}]

    organic = data.get("organic") or data.get("organic_results") or []
    results = []
    for item in organic[:max_results]:
        url = item.get("link") or item.get("url")
        if not url:
            continue
        results.append({
            "title": item.get("title"),
            "url": url,
            "snippet": item.get("description") or item.get("snippet"),
        })
    log.info("search_web(%r) → %d result(s)", query, len(results))
    return results

def scrape_page(url: str) -> str:
    """Bright Data Web Unlocker — fetch a URL's rendered content as clean text."""
    if not BRIGHTDATA_UNLOCKER_PROXY:
        return "ERROR: BRIGHTDATA_UNLOCKER_PROXY is not set — cannot scrape."

    url = url.strip()
    if url and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", url):
        url = f"https://{url}"

    try:
        html = _brightdata_get(url, BRIGHTDATA_UNLOCKER_PROXY)
    except Exception as e:
        log.warning("Bright Data unlocker request failed for %s: %s", url, e)
        return f"ERROR: failed to fetch {url}: {e}"

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)

    if len(text) < MIN_CONTENT_CHARS:
        return f"ERROR: fetched {url} but got almost no content ({len(text)} chars)."
    if _looks_like_bot_block(text):
        return f"ERROR: {url} returned what looks like a bot-block/challenge page, not real content."

    log.info("scrape_page(%s) → %d chars", url, len(text))
    return text[:MAX_CONTENT_CHARS]

# ─── TOOL SCHEMA  (provider-agnostic; adapted per backend below) ─────────

TOOLS = [
    {
        "name": "recall_products",
        "description": "Search previously saved competitor pricing data before scraping again. "
                       "Always try this first — it may already have a recent enough answer.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What you're looking for, e.g. 'paracetamol goodlife'"},
                "competitor": {"type": "string", "description": "Filter to one competitor. Omit if unknown."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_web",
        "description": "Search the web for candidate pages (e.g. a competitor's shop/product page) "
                       "when you don't already have a URL.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "max_results": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "scrape_page",
        "description": "Fetch a specific URL's page content so you can read prices/products off it.",
        "parameters": {
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        },
    },
    {
        "name": "save_products",
        "description": "Save the product/price rows you just read off a scraped page, so future "
                       "questions can be answered from memory instead of scraping again. Extract "
                       "every distinct priced product yourself from the page text you were given. "
                       "Omit any field you don't know — don't guess.",
        "parameters": {
            "type": "object",
            "properties": {
                "competitor": {"type": "string"},
                "source_url": {"type": "string"},
                "products": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "product_name":        {"type": "string"},
                            "sku":                 {"type": "string"},
                            "brand":               {"type": "string"},
                            "current_price":       {"type": "number"},
                            "original_price":      {"type": "number"},
                            "currency":            {"type": "string"},
                            "discount_percentage": {"type": "number"},
                            "in_stock":            {"type": "boolean"},
                            "product_url":         {"type": "string"},
                            "unit":                {"type": "string"},
                        },
                        "required": ["product_name"],
                    },
                },
            },
            "required": ["competitor", "source_url", "products"],
        },
    },
]

_TOOL_IMPLS = {
    "recall_products": lambda args: recall_products(
        args["query"], args.get("competitor"), args.get("n_results", 5)),
    "search_web": lambda args: search_web(args["query"], args.get("max_results", 5)),
    "scrape_page": lambda args: scrape_page(args["url"]),
    "save_products": lambda args: save_products(
        args["competitor"], args["source_url"], args.get("products", [])),
}

def _run_tool(name: str, args: dict) -> str:
    impl = _TOOL_IMPLS.get(name)
    if impl is None:
        return f"ERROR: unknown tool {name!r}"
    try:
        result = impl(args)
    except Exception as e:
        log.exception("Tool %s failed", name)
        return f"ERROR: tool {name} raised: {e}"
    return json.dumps(result) if not isinstance(result, str) else result

SYSTEM_PROMPT = (
    "You are a competitive-pricing research assistant. You have tools to check your own "
    "memory of previously scraped prices, search the web, scrape a specific page, and save "
    "new pricing data you find. When asked about a competitor's prices:\n"
    "1. Call recall_products first — if it has a good-enough recent answer, use it.\n"
    "2. Otherwise, call search_web to find the competitor's actual product/shop page "
    "(prefer the retailer's own site over marketplaces or social media).\n"
    "3. Call scrape_page on the most promising result.\n"
    "4. Read the page content yourself and identify every distinct product with a price.\n"
    "5. Call save_products with what you found, then answer the user's question in plain "
    "language, citing the actual numbers.\n"
    "If a tool errors or a page has no useful pricing content, say so plainly instead of "
    "guessing at numbers."
)

# ─── LLM BACKENDS ─────────────────────────────────────────────────────────
# Both return a normalized dict: {"content": str | None,
#                                 "tool_calls": [{"id", "name", "arguments"}]}

def _chat_ollama(messages: list[dict]) -> dict:
    import ollama
    ollama_tools = [{"type": "function", "function": {
        "name": t["name"], "description": t["description"], "parameters": t["parameters"],
    }} for t in TOOLS]

    resp = ollama.chat(model=OLLAMA_MODEL, messages=messages, tools=ollama_tools)
    msg = resp["message"]
    tool_calls = []
    for tc in (msg.get("tool_calls") or []):
        fn = tc.get("function", {})
        args = fn.get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}
        tool_calls.append({"id": str(uuid.uuid4()), "name": fn.get("name"), "arguments": args})
    return {"content": msg.get("content") or None, "tool_calls": tool_calls}

def _chat_anthropic(messages: list[dict]) -> dict:
    import anthropic
    # An "identity-linked" API key (tied to your claude.ai login rather than
    # a standalone key) requires the target Workspace on every request.
    default_headers = {}
    workspace_id = os.getenv("ANTHROPIC_WORKSPACE_ID")
    if workspace_id:
        default_headers["anthropic-workspace-id"] = workspace_id
    client = anthropic.Anthropic(default_headers=default_headers or None)
    anthropic_tools = [{
        "name": t["name"], "description": t["description"], "input_schema": t["parameters"],
    } for t in TOOLS]

    system = None
    convo = []
    for m in messages:
        if m["role"] == "system":
            system = m["content"]
        else:
            convo.append(m)

    resp = client.messages.create(
        model=ANTHROPIC_MODEL, max_tokens=4096, system=system,
        tools=anthropic_tools, messages=convo,
    )
    content_text = None
    tool_calls = []
    for block in resp.content:
        if block.type == "text":
            content_text = (content_text or "") + block.text
        elif block.type == "tool_use":
            tool_calls.append({"id": block.id, "name": block.name, "arguments": block.input})
    return {"content": content_text, "tool_calls": tool_calls}

def chat(messages: list[dict]) -> dict:
    if LLM_PROVIDER == "anthropic":
        return _chat_anthropic(messages)
    return _chat_ollama(messages)

# ─── AGENT LOOP ───────────────────────────────────────────────────────────

def run_agent_turn(history: list[dict], user_input: str, *, max_tool_rounds: int = 6) -> str:
    history.append({"role": "user", "content": user_input})

    for _ in range(max_tool_rounds):
        result = chat(history)

        if not result["tool_calls"]:
            reply = result["content"] or "(no response)"
            history.append({"role": "assistant", "content": reply})
            return reply

        # Record the assistant's tool-call turn, then execute each tool and
        # feed results back — provider-specific message shapes handled here
        # so run_agent_turn / chat() stay symmetric per backend.
        if LLM_PROVIDER == "anthropic":
            content_blocks = []
            if result["content"]:
                content_blocks.append({"type": "text", "text": result["content"]})
            for tc in result["tool_calls"]:
                content_blocks.append({
                    "type": "tool_use", "id": tc["id"], "name": tc["name"], "input": tc["arguments"],
                })
            history.append({"role": "assistant", "content": content_blocks})

            tool_results = []
            for tc in result["tool_calls"]:
                output = _run_tool(tc["name"], tc["arguments"])
                log.info("  ↳ %s(%s) → %.200s", tc["name"], tc["arguments"], output)
                tool_results.append({
                    "type": "tool_result", "tool_use_id": tc["id"], "content": output,
                })
            history.append({"role": "user", "content": tool_results})
        else:
            history.append({
                "role": "assistant", "content": result["content"] or "",
                "tool_calls": [
                    {"function": {"name": tc["name"], "arguments": tc["arguments"]}}
                    for tc in result["tool_calls"]
                ],
            })
            for tc in result["tool_calls"]:
                output = _run_tool(tc["name"], tc["arguments"])
                log.info("  ↳ %s(%s) → %.200s", tc["name"], tc["arguments"], output)
                history.append({"role": "tool", "content": output, "name": tc["name"]})

    fallback = "I made several tool calls but couldn't reach a final answer — try rephrasing?"
    history.append({"role": "assistant", "content": fallback})
    return fallback

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    print(f"competitor_pricing_agent · provider={LLM_PROVIDER} · "
          f"model={OLLAMA_MODEL if LLM_PROVIDER != 'anthropic' else ANTHROPIC_MODEL}")
    print("Ask about a competitor's prices. Type 'exit' or 'quit' to end.\n")

    history = [{"role": "system", "content": SYSTEM_PROMPT}]

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not user_input:
            continue
        if user_input.lower() in {"exit", "quit"}:
            break

        reply = run_agent_turn(history, user_input)
        print(f"\nAgent: {reply}\n")


if __name__ == "__main__":
    main()
