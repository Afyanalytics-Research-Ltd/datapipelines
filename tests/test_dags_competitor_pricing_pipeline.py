"""
Deep coverage for dags/competitor_pricing_pipeline.py — the scheduled
replacement for the standalone competitor_pricing_scraper.py CLI.

Covers: DAG task-graph shape, Sheet-target parsing/dedup, the Tavily/
Selenium fetch + bot-block detection, the OpenAI function-calling
extraction wrapper, the S3 key template, the COPY INTO / MERGE SQL
builders (incl. the QUALIFY ROW_NUMBER() day-partition dedup and SQL
string-interpolation escaping), per-target failure isolation, and
report_failures' end-of-run gate.

All external I/O (gspread/Google Sheets, Tavily, Selenium/Chrome, OpenAI,
S3Hook, Snowflake) is mocked — no real network calls are made.
"""
from __future__ import annotations

import gzip
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest import mock

import pytest

from tests.airflow_stub import TriggerRule
from tests.helpers import load_dag_module

MODULE_NAME = "competitor_pricing_pipeline"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


# ─────────────────────────────────────────────────────────────────────────
# DAG structural shape
# ─────────────────────────────────────────────────────────────────────────
class TestDagStructure:
    def test_dag_id_schedule_catchup_tags(self, module):
        dag = module.dag
        assert dag.dag_id == "competitor_pricing_pipeline"
        assert dag.schedule == "@daily"
        assert dag.catchup is False
        assert dag.tags == ["competitor_pricing", "scraper", "llm", "snowflake", "s3"]
        assert dag.max_active_tasks == 8

    def test_default_args_retries(self, module):
        dag = module.dag
        assert dag.default_args.get("retries") == 3
        assert dag.default_args.get("retry_delay") == timedelta(minutes=2)

    def test_task_ids(self, module):
        dag = module.dag
        assert set(dag.task_ids) == {
            "prepare_targets", "scrape_and_upload", "copy_into_raw",
            "merge_clean", "report_failures",
        }

    def test_linear_dependency_chain(self, module):
        dag = module.dag
        t = dag.task_dict
        assert t["prepare_targets"].downstream_task_ids == {"scrape_and_upload"}
        assert t["scrape_and_upload"].upstream_task_ids == {"prepare_targets"}
        assert t["scrape_and_upload"].downstream_task_ids == {"copy_into_raw"}
        assert t["copy_into_raw"].upstream_task_ids == {"scrape_and_upload"}
        assert t["copy_into_raw"].downstream_task_ids == {"merge_clean"}
        assert t["merge_clean"].upstream_task_ids == {"copy_into_raw"}
        assert t["merge_clean"].downstream_task_ids == {"report_failures"}
        assert t["report_failures"].upstream_task_ids == {"merge_clean"}
        assert t["report_failures"].downstream_task_ids == set()

    def test_scrape_and_upload_is_mapped_over_prepare_targets_output(self, module):
        dag = module.dag
        t_scrape = dag.task_dict["scrape_and_upload"]
        assert t_scrape.is_mapped is True
        assert t_scrape.mapped_kwargs is not None
        assert t_scrape.mapped_kwargs["op_kwargs"].task is dag.task_dict["prepare_targets"]

    def test_copy_into_raw_is_mapped_over_scrape_and_upload_output(self, module):
        dag = module.dag
        t_copy = dag.task_dict["copy_into_raw"]
        assert t_copy.is_mapped is True
        assert t_copy.mapped_kwargs is not None
        assert t_copy.mapped_kwargs["op_kwargs"].task is dag.task_dict["scrape_and_upload"]

    def test_trigger_rules(self, module):
        dag = module.dag
        t = dag.task_dict
        # prepare_targets keeps the (stub) default trigger rule.
        assert t["prepare_targets"].trigger_rule == TriggerRule.ALL_SUCCESS
        assert t["scrape_and_upload"].trigger_rule == TriggerRule.ALL_DONE
        assert t["copy_into_raw"].trigger_rule == TriggerRule.ALL_DONE
        assert t["merge_clean"].trigger_rule == TriggerRule.ALL_DONE
        assert t["report_failures"].trigger_rule == TriggerRule.ALL_DONE

    def test_scrape_task_concurrency_cap(self, module):
        dag = module.dag
        t_scrape = dag.task_dict["scrape_and_upload"]
        assert t_scrape.extra_kwargs.get("max_active_tis_per_dag") == module.MAX_ACTIVE_SCRAPE_TASKS


# ─────────────────────────────────────────────────────────────────────────
# Small pure helpers
# ─────────────────────────────────────────────────────────────────────────
class TestNormalizeUrl:
    def test_adds_https_to_bare_domain(self, module):
        assert module._normalize_url("goodlife.com") == "https://goodlife.com"

    def test_keeps_existing_scheme(self, module):
        assert module._normalize_url("http://goodlife.com") == "http://goodlife.com"
        assert module._normalize_url("https://goodlife.com/shop") == "https://goodlife.com/shop"

    def test_strips_whitespace(self, module):
        assert module._normalize_url("  goodlife.com  ") == "https://goodlife.com"

    def test_empty_or_none(self, module):
        assert module._normalize_url("") == ""
        assert module._normalize_url(None) == ""


class TestSafeS3Token:
    def test_replaces_unsafe_chars(self, module):
        assert module._safe_s3_token("Acme / Pharmacy!") == "Acme_Pharmacy_"

    def test_collapses_runs_of_unsafe_chars(self, module):
        assert module._safe_s3_token("a///b") == "a_b"

    def test_empty_and_none(self, module):
        assert module._safe_s3_token("") == ""
        assert module._safe_s3_token(None) == ""

    def test_keeps_allowed_chars(self, module):
        assert module._safe_s3_token("abc-DEF_123.45+6=7") == "abc-DEF_123.45+6=7"


class TestBotBlockDetection:
    @pytest.mark.parametrize("signature", [
        "Attention Required! | Cloudflare",
        "You have been blocked from accessing this site",
        "Please enable cookies to continue",
        "Checking your browser before accessing",
        "Just a moment...",
        "Verify you are human by completing the action below",
        "Are you a robot?",
        "Unusual traffic from your computer network",
        "Access Denied",
        "Complete the CAPTCHA to continue",
        "cf-error-details",
        "Ray ID: 8badf00d",
    ])
    def test_detects_known_block_signatures_case_insensitively(self, module, signature):
        page = f"<html><body>{signature}</body></html>"
        assert module._looks_like_bot_block(page) is True

    def test_normal_content_is_not_flagged(self, module):
        content = "Paracetamol 500mg — KES 120. Ibuprofen 200mg — KES 150."
        assert module._looks_like_bot_block(content) is False

    def test_only_checks_first_2000_chars(self, module):
        # signature appears only after the 2000-char head window
        content = ("x" * 2000) + "you have been blocked"
        assert module._looks_like_bot_block(content) is False


# ─────────────────────────────────────────────────────────────────────────
# prepare_targets — Sheet parsing / dedup / enabled filtering
# ─────────────────────────────────────────────────────────────────────────
class TestPrepareTargets:
    def _sheet(self, module, rows, monkeypatch, sheet_id="SHEET123", worksheet="Sheet1"):
        monkeypatch.setattr(module, "_read_sheet", lambda sid, ws: rows)

    def test_builds_one_job_per_enabled_row(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [
            {"competitor": "Goodlife", "url": "goodlife.com", "category": "pharmacy"},
            {"competitor": "Jumia", "url": "https://jumia.co.ke/shop"},
        ], monkeypatch)

        jobs = module.prepare_targets()
        assert len(jobs) == 2
        assert jobs[0] == {"job": {"competitor": "Goodlife", "url": "https://goodlife.com", "category": "pharmacy"}}
        assert jobs[1] == {"job": {"competitor": "Jumia", "url": "https://jumia.co.ke/shop", "category": None}}

    def test_filters_disabled_rows(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [
            {"competitor": "Goodlife", "url": "goodlife.com", "enabled": "FALSE"},
            {"competitor": "Jumia", "url": "jumia.co.ke", "enabled": "TRUE"},
            {"competitor": "Naivas", "url": "naivas.co.ke", "enabled": "false"},
        ], monkeypatch)

        jobs = module.prepare_targets()
        assert [j["job"]["competitor"] for j in jobs] == ["Jumia"]

    def test_default_enabled_is_true_when_column_missing(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [{"competitor": "Goodlife", "url": "goodlife.com"}], monkeypatch)
        jobs = module.prepare_targets()
        assert len(jobs) == 1

    def test_filters_missing_competitor_or_url(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [
            {"competitor": "", "url": "goodlife.com"},
            {"competitor": "Jumia", "url": ""},
            {"competitor": "   ", "url": "naivas.co.ke"},
            {"competitor": "Valid", "url": "valid.com"},
        ], monkeypatch)
        jobs = module.prepare_targets()
        assert [j["job"]["competitor"] for j in jobs] == ["Valid"]

    def test_dedup_by_lowercased_competitor_and_normalized_url(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [
            {"competitor": "Goodlife", "url": "https://goodlife.com"},
            {"competitor": "goodlife", "url": "goodlife.com"},  # same after normalize+lower
            {"competitor": "Goodlife", "url": "https://goodlife.com/other"},  # different url, kept
        ], monkeypatch)
        jobs = module.prepare_targets()
        assert len(jobs) == 2
        urls = {j["job"]["url"] for j in jobs}
        assert urls == {"https://goodlife.com", "https://goodlife.com/other"}

    def test_empty_sheet_returns_empty_list(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [], monkeypatch)
        assert module.prepare_targets() == []

    def test_uses_default_worksheet_when_variable_missing(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        captured = {}

        def fake_read_sheet(sid, ws):
            captured["sheet_id"] = sid
            captured["worksheet"] = ws
            return []

        monkeypatch.setattr(module, "_read_sheet", fake_read_sheet)
        module.prepare_targets()
        assert captured == {"sheet_id": "SHEET123", "worksheet": "Sheet1"}

    def test_category_blank_string_normalized_to_none(self, module, monkeypatch, set_variables):
        set_variables(COMPETITOR_SHEET_ID="SHEET123")
        self._sheet(module, [{"competitor": "Goodlife", "url": "goodlife.com", "category": "   "}], monkeypatch)
        jobs = module.prepare_targets()
        assert jobs[0]["job"]["category"] is None


# ─────────────────────────────────────────────────────────────────────────
# Fetch layer: Tavily
# ─────────────────────────────────────────────────────────────────────────
class TestFetchViaTavily:
    def test_no_api_key_returns_none(self, module, monkeypatch):
        monkeypatch.delenv("TAVILY_API_KEY", raising=False)
        assert module._fetch_via_tavily("https://example.com") is None

    def test_success_returns_content(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        good_content = "Product A KES 100. " * 20  # > MIN_TAVILY_CONTENT_CHARS
        fake_client = mock.Mock()
        fake_client.extract.return_value = {"results": [{"raw_content": good_content}]}
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            content = module._fetch_via_tavily("https://example.com")
        assert content == good_content.strip()
        fake_client.extract.assert_called_once_with(urls=["https://example.com"])

    def test_extract_exception_returns_none(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        fake_client = mock.Mock()
        fake_client.extract.side_effect = RuntimeError("429 Too Many Requests")
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            assert module._fetch_via_tavily("https://example.com") is None
        # exactly one attempt — no internal retry/backoff loop
        fake_client.extract.assert_called_once()

    def test_too_little_content_returns_none(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        fake_client = mock.Mock()
        fake_client.extract.return_value = {"results": [{"raw_content": "short"}]}
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            assert module._fetch_via_tavily("https://example.com") is None

    def test_bot_block_content_returns_none(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        blocked = "Attention Required! " + ("filler " * 50)
        fake_client = mock.Mock()
        fake_client.extract.return_value = {"results": [{"raw_content": blocked}]}
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            assert module._fetch_via_tavily("https://example.com") is None

    def test_no_results_but_failed_results_present(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        fake_client = mock.Mock()
        fake_client.extract.return_value = {
            "results": [], "failed_results": [{"error": "unreachable"}],
        }
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            assert module._fetch_via_tavily("https://example.com") is None

    def test_no_results_and_no_failed_results(self, module, monkeypatch):
        monkeypatch.setenv("TAVILY_API_KEY", "test-key")
        fake_client = mock.Mock()
        fake_client.extract.return_value = {"results": []}
        with mock.patch("tavily.TavilyClient", return_value=fake_client):
            assert module._fetch_via_tavily("https://example.com") is None


# ─────────────────────────────────────────────────────────────────────────
# Fetch layer: Selenium fallback
# ─────────────────────────────────────────────────────────────────────────
class TestFetchViaSelenium:
    def _fake_driver(self, html: str):
        driver = mock.Mock()
        driver.page_source = html
        return driver

    def test_success_extracts_text(self, module):
        html = "<html><body><script>ignored()</script><p>Paracetamol KES 120</p></body></html>" \
               + ("<p>filler content padding text</p>" * 20)
        driver = self._fake_driver(html)
        with mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("time.sleep"):
            content = module._fetch_via_selenium("https://example.com")
        assert content is not None
        assert "Paracetamol KES 120" in content
        assert "ignored()" not in content  # <script> stripped
        driver.get.assert_called_once_with("https://example.com")
        driver.quit.assert_called_once()

    def test_too_short_text_returns_none(self, module):
        driver = self._fake_driver("<html><body><p>hi</p></body></html>")
        with mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("time.sleep"):
            assert module._fetch_via_selenium("https://example.com") is None
        driver.quit.assert_called_once()

    def test_bot_block_text_returns_none(self, module):
        html = "<html><body>" + "Are you a robot? " * 40 + "</body></html>"
        driver = self._fake_driver(html)
        with mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("time.sleep"):
            assert module._fetch_via_selenium("https://example.com") is None

    def test_driver_quit_called_even_on_exception(self, module):
        driver = mock.Mock()
        driver.get.side_effect = RuntimeError("page load timeout")
        with mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("time.sleep"):
            with pytest.raises(RuntimeError):
                module._fetch_via_selenium("https://example.com")
        driver.quit.assert_called_once()


class TestFetchPageContent:
    def test_tavily_success_skips_selenium(self, module, monkeypatch):
        monkeypatch.setattr(module, "_fetch_via_tavily", lambda url: "tavily content")
        selenium_mock = mock.Mock(side_effect=AssertionError("selenium should not be called"))
        monkeypatch.setattr(module, "_fetch_via_selenium", selenium_mock)
        content, method = module._fetch_page_content("https://example.com")
        assert (content, method) == ("tavily content", "tavily")
        selenium_mock.assert_not_called()

    def test_tavily_fails_falls_back_to_selenium_success(self, module, monkeypatch):
        monkeypatch.setattr(module, "_fetch_via_tavily", lambda url: None)
        monkeypatch.setattr(module, "_fetch_via_selenium", lambda url: "selenium content")
        monkeypatch.setattr(module, "ALLOW_SELENIUM_FALLBACK", True)
        content, method = module._fetch_page_content("https://example.com")
        assert (content, method) == ("selenium content", "selenium")

    def test_tavily_fails_selenium_disabled_raises(self, module, monkeypatch):
        monkeypatch.setattr(module, "_fetch_via_tavily", lambda url: None)
        monkeypatch.setattr(module, "ALLOW_SELENIUM_FALLBACK", False)
        selenium_mock = mock.Mock(side_effect=AssertionError("selenium should not be called"))
        monkeypatch.setattr(module, "_fetch_via_selenium", selenium_mock)
        with pytest.raises(RuntimeError, match="Selenium fallback is disabled"):
            module._fetch_page_content("https://example.com")
        selenium_mock.assert_not_called()

    def test_both_fail_raises(self, module, monkeypatch):
        monkeypatch.setattr(module, "_fetch_via_tavily", lambda url: None)
        monkeypatch.setattr(module, "_fetch_via_selenium", lambda url: None)
        monkeypatch.setattr(module, "ALLOW_SELENIUM_FALLBACK", True)
        with pytest.raises(RuntimeError, match="Both Tavily and Selenium fallback failed"):
            module._fetch_page_content("https://example.com")


# ─────────────────────────────────────────────────────────────────────────
# OpenAI structured extraction
# ─────────────────────────────────────────────────────────────────────────
def _fake_openai_response(*, name="record_products", arguments=None, no_tool_calls=False):
    if no_tool_calls:
        message = SimpleNamespace(tool_calls=None)
    else:
        call = SimpleNamespace(function=SimpleNamespace(name=name, arguments=arguments))
        message = SimpleNamespace(tool_calls=[call])
    choice = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice])


class TestExtractProductsWithOpenai:
    def test_product_tool_schema_shape(self, module):
        tool = module.PRODUCT_TOOL
        assert tool["type"] == "function"
        fn = tool["function"]
        assert fn["name"] == "record_products"
        props = fn["parameters"]["properties"]["products"]["items"]["properties"]
        assert set(props) == {
            "product_name", "sku", "brand", "current_price", "original_price",
            "currency", "discount_percentage", "in_stock", "product_url", "unit",
        }
        assert fn["parameters"]["properties"]["products"]["items"]["required"] == ["product_name"]
        assert fn["parameters"]["required"] == ["products"]

    def test_success_returns_products(self, module):
        args = json.dumps({"products": [{"product_name": "Paracetamol", "current_price": 120}]})
        resp = _fake_openai_response(arguments=args)
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "page content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == [{"product_name": "Paracetamol", "current_price": 120}]

    def test_truncates_content_to_max_chars(self, module):
        long_content = "x" * (module.MAX_CONTENT_CHARS + 5000)
        args = json.dumps({"products": []})
        resp = _fake_openai_response(arguments=args)
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            module._extract_products_with_openai(
                long_content, competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        sent_prompt = fake_client.chat.completions.create.call_args.kwargs["messages"][0]["content"]
        # truncated page content should not contain the full untruncated length
        assert len(sent_prompt) < len(long_content) + 500

    def test_api_exception_returns_empty_list(self, module):
        fake_client = mock.Mock()
        fake_client.chat.completions.create.side_effect = RuntimeError("503 Service Unavailable")
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []
        # exactly one attempt — no internal retry/backoff loop
        fake_client.chat.completions.create.assert_called_once()

    def test_no_tool_calls_returns_empty_list(self, module):
        resp = _fake_openai_response(no_tool_calls=True)
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []

    def test_wrong_function_name_returns_empty_list(self, module):
        resp = _fake_openai_response(name="some_other_function", arguments="{}")
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []

    def test_malformed_json_arguments_returns_empty_list(self, module):
        resp = _fake_openai_response(arguments="{not valid json")
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []

    def test_missing_products_key_returns_empty_list(self, module):
        resp = _fake_openai_response(arguments=json.dumps({"unexpected": "shape"}))
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []

    def test_null_products_value_returns_empty_list(self, module):
        resp = _fake_openai_response(arguments=json.dumps({"products": None}))
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == []

    def test_schema_violating_product_missing_required_field_passes_through_unvalidated(self, module):
        """The code never validates the LLM's tool-call arguments against
        PRODUCT_TOOL's own JSON schema (e.g. the required `product_name`) —
        it trusts the API to have honored tool_choice-forced schema and just
        json.loads()s whatever comes back. A response that violates its own
        declared schema (missing the required product_name) is returned
        as-is rather than being filtered out here."""
        bad_products = [{"current_price": 50}]  # missing required product_name
        resp = _fake_openai_response(arguments=json.dumps({"products": bad_products}))
        fake_client = mock.Mock()
        fake_client.chat.completions.create.return_value = resp
        with mock.patch("openai.OpenAI", return_value=fake_client):
            products = module._extract_products_with_openai(
                "content", competitor="Goodlife", url="https://goodlife.com", model="gpt-4.1",
            )
        assert products == bad_products


# ─────────────────────────────────────────────────────────────────────────
# scrape_and_upload
# ─────────────────────────────────────────────────────────────────────────
class TestScrapeAndUpload:
    def _job(self, competitor="Goodlife", url="https://goodlife.com", category="pharmacy"):
        return {"competitor": competitor, "url": url, "category": category}

    def test_success_uploads_to_s3_and_returns_extracted(self, module):
        products = [{"product_name": "Paracetamol", "current_price": 120}]
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai", return_value=products), \
             mock.patch.object(module, "S3Hook") as s3_hook_cls:
            s3_instance = s3_hook_cls.return_value
            result = module.scrape_and_upload(self._job(), run_id="run123")

        assert result["status"] == "EXTRACTED"
        assert result["fetch_method"] == "tavily"
        assert result["row_count"] == 1
        assert result["competitor"] == "Goodlife"
        assert result["source_url"] == "https://goodlife.com"
        assert result["category"] == "pharmacy"
        assert result["s3_key"].startswith("raw/competitor_pricing/competitor=Goodlife/dt=")
        assert result["s3_key"].endswith("__run123.jsonl.gz")

        s3_hook_cls.assert_called_once_with(aws_conn_id=module.S3_CONN_ID)
        assert s3_instance.load_bytes.call_count == 1
        call_kwargs = s3_instance.load_bytes.call_args.kwargs
        assert call_kwargs["bucket_name"] == module.S3_BUCKET
        assert call_kwargs["key"] == result["s3_key"]
        assert call_kwargs["replace"] is True

        # verify the uploaded payload really is gzipped JSONL with the
        # product + metadata fields merged in
        decompressed = gzip.decompress(call_kwargs["bytes_data"]).decode("utf-8")
        lines = [json.loads(l) for l in decompressed.strip().split("\n")]
        assert len(lines) == 1
        assert lines[0]["product_name"] == "Paracetamol"
        assert lines[0]["current_price"] == 120
        assert lines[0]["competitor"] == "Goodlife"
        assert lines[0]["source_url"] == "https://goodlife.com"
        assert lines[0]["category"] == "pharmacy"
        assert lines[0]["fetch_method"] == "tavily"
        assert "scraped_at" in lines[0]

    def test_empty_products_returns_empty_status_no_upload(self, module):
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai", return_value=[]), \
             mock.patch.object(module, "S3Hook") as s3_hook_cls:
            result = module.scrape_and_upload(self._job(), run_id="run123")

        assert result["status"] == "EMPTY"
        assert result["s3_key"] is None
        assert result["row_count"] == 0
        s3_hook_cls.assert_not_called()

    def test_fetch_failure_returns_failed_status_without_raising(self, module):
        with mock.patch.object(module, "_fetch_page_content",
                                side_effect=RuntimeError("Both Tavily and Selenium fallback failed")), \
             mock.patch.object(module, "S3Hook") as s3_hook_cls:
            result = module.scrape_and_upload(self._job(), run_id="run123")

        assert result["status"] == "FAILED"
        assert result["s3_key"] is None
        assert result["row_count"] == 0
        assert "Tavily and Selenium" in result["error"]
        s3_hook_cls.assert_not_called()

    def test_extraction_exception_is_isolated_as_failed(self, module):
        """A schema-violating / malformed products payload that blows up the
        row-building list comprehension (e.g. openai returning a dict
        instead of a list for "products") must not propagate — it's caught
        by the same broad except and reported as FAILED, same as a fetch
        error."""
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai",
                                return_value={"unexpected": "not-a-list"}):
            result = module.scrape_and_upload(self._job(), run_id="run123")
        assert result["status"] == "FAILED"
        assert result["error"]

    def test_s3_upload_exception_is_isolated_as_failed(self, module):
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai",
                                return_value=[{"product_name": "X"}]), \
             mock.patch.object(module, "S3Hook") as s3_hook_cls:
            s3_hook_cls.return_value.load_bytes.side_effect = RuntimeError("S3 unreachable")
            result = module.scrape_and_upload(self._job(), run_id="run123")
        assert result["status"] == "FAILED"
        assert "S3 unreachable" in result["error"]

    def test_error_message_truncated_to_2000_chars(self, module):
        huge_error = "x" * 5000
        with mock.patch.object(module, "_fetch_page_content", side_effect=RuntimeError(huge_error)):
            result = module.scrape_and_upload(self._job(), run_id="run123")
        assert len(result["error"]) == 2000

    def test_multiple_targets_one_failure_does_not_affect_others(self, module):
        """Simulates the mapped-task fan-out: calling scrape_and_upload once
        per target. One bad target raising internally must not prevent the
        others (called independently, as Airflow would) from succeeding."""
        good_job = self._job(competitor="Goodlife", url="https://goodlife.com")
        bad_job = self._job(competitor="BadSite", url="https://bad.example.com")

        def fetch_side_effect(url):
            if "bad.example.com" in url:
                raise RuntimeError("bot-blocked")
            return "page text", "tavily"

        with mock.patch.object(module, "_fetch_page_content", side_effect=fetch_side_effect), \
             mock.patch.object(module, "_extract_products_with_openai",
                                return_value=[{"product_name": "X", "current_price": 1}]), \
             mock.patch.object(module, "S3Hook"):
            good_result = module.scrape_and_upload(good_job, run_id="run123")
            bad_result = module.scrape_and_upload(bad_job, run_id="run123")

        assert good_result["status"] == "EXTRACTED"
        assert bad_result["status"] == "FAILED"

    def test_unsafe_competitor_name_sanitized_in_s3_key(self, module):
        job = self._job(competitor="Acme/Pharmacy!!", url="https://acme.example.com")
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai",
                                return_value=[{"product_name": "X"}]), \
             mock.patch.object(module, "S3Hook"):
            result = module.scrape_and_upload(job, run_id="run123")
        assert "competitor=Acme_Pharmacy_/" in result["s3_key"]
        assert "/" not in result["s3_key"].split("competitor=")[1].split("/")[0].replace("_", "")

    def test_blank_competitor_falls_back_to_unknown_token(self, module):
        job = self._job(competitor="", url="https://acme.example.com")
        with mock.patch.object(module, "_fetch_page_content", return_value=("page text", "tavily")), \
             mock.patch.object(module, "_extract_products_with_openai",
                                return_value=[{"product_name": "X"}]), \
             mock.patch.object(module, "S3Hook"):
            result = module.scrape_and_upload(job, run_id="run123")
        assert "competitor=unknown/" in result["s3_key"]


# ─────────────────────────────────────────────────────────────────────────
# copy_into_raw — COPY INTO SQL builder
# ─────────────────────────────────────────────────────────────────────────
def _extracted_result(**overrides):
    base = {
        "competitor": "Goodlife", "source_url": "https://goodlife.com",
        "category": "pharmacy", "status": "EXTRACTED", "fetch_method": "tavily",
        "scraped_at": "2026-09-22T10:00:00+00:00",
        "s3_key": "raw/competitor_pricing/competitor=Goodlife/dt=2026-09-22/abc123__run1.jsonl.gz",
        "row_count": 3,
    }
    base.update(overrides)
    return base


class TestCopyIntoRaw:
    def test_extracted_status_runs_copy_and_returns_copied(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            result = module.copy_into_raw(**_extracted_result())

        assert result == {"competitor": "Goodlife", "status": "COPIED"}
        sf_cls.assert_called_once_with(schema_=module.SF_SHARED_SCHEMA)
        assert sf_instance.execute.call_count == 1
        sql = sf_instance.execute.call_args.args[0]
        assert f"COPY INTO {module.RAW_TABLE}" in sql
        assert "(competitor, source_url, category, fetch_method, scraped_at, payload)" in sql
        assert "'Goodlife'::STRING" in sql
        assert "'https://goodlife.com'::STRING" in sql
        assert "NULLIF('pharmacy', '')::STRING" in sql
        assert "'tavily'::STRING" in sql
        assert "'2026-09-22T10:00:00+00:00'::TIMESTAMP_TZ" in sql
        assert f"FROM @{module.SF_STAGE}" in sql
        assert "FILES = ('raw/competitor_pricing/competitor=Goodlife/dt=2026-09-22/abc123__run1.jsonl.gz')" in sql
        assert f"FILE_FORMAT = (FORMAT_NAME = {module.SF_FILE_FORMAT})" in sql
        assert "ON_ERROR = 'CONTINUE'" in sql

    @pytest.mark.parametrize("status", ["EMPTY", "FAILED"])
    def test_non_extracted_status_skips_copy(self, module, status):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            result = module.copy_into_raw(**_extracted_result(status=status, s3_key=None,
                                                                fetch_method=None, row_count=0))
        assert result == {"competitor": "Goodlife", "status": status}
        sf_cls.assert_not_called()

    def test_snowflake_exception_is_isolated_as_failed(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.execute.side_effect = RuntimeError("warehouse suspended")
            result = module.copy_into_raw(**_extracted_result())
        assert result["status"] == "FAILED"
        assert "warehouse suspended" in result["error"]

    def test_sql_injection_shaped_competitor_name_is_escaped(self, module):
        """competitor/source_url/category are interpolated into the SQL
        string directly (no bind params) but the code does its own
        single-quote escaping via .replace("'", "''") before interpolating
        — this is the standard SQL-92 way to escape a single-quoted string
        literal. Verify a name containing a quote produces syntactically
        safe, correctly-doubled quotes rather than breaking out of the
        string literal."""
        payload = "O'Brien'; DROP TABLE COMPETITOR_PRICING_RAW; --"
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.copy_into_raw(**_extracted_result(competitor=payload))
        sql = sf_instance.execute.call_args.args[0]
        # every single-quote from the payload must have been doubled
        assert "'O''Brien''; DROP TABLE COMPETITOR_PRICING_RAW; --'::STRING" in sql

    def test_sql_injection_shaped_source_url_and_category_are_escaped(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.copy_into_raw(**_extracted_result(
                source_url="https://evil.example.com/'; DROP TABLE x; --",
                category="pharma' OR '1'='1",
            ))
        sql = sf_instance.execute.call_args.args[0]
        assert "https://evil.example.com/''; DROP TABLE x; --" in sql
        assert "pharma'' OR ''1''=''1" in sql

    def test_category_none_becomes_nullif_empty_string(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.copy_into_raw(**_extracted_result(category=None))
        sql = sf_instance.execute.call_args.args[0]
        assert "NULLIF('', '')::STRING" in sql


# ─────────────────────────────────────────────────────────────────────────
# merge_clean — MERGE SQL builder / QUALIFY dedup
# ─────────────────────────────────────────────────────────────────────────
class TestMergeClean:
    def test_merge_sql_shape(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.merge_clean()

        sf_cls.assert_called_once_with(schema_=module.SF_SHARED_SCHEMA)
        sql = sf_instance.execute.call_args.args[0]

        assert f"MERGE INTO {module.CLEAN_TABLE} AS t" in sql
        assert f"FROM {module.RAW_TABLE}" in sql
        assert "WHERE payload:product_name IS NOT NULL" in sql

    def test_qualify_row_number_day_partition_dedup(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.merge_clean()
        sql = sf_instance.execute.call_args.args[0]

        assert "QUALIFY ROW_NUMBER() OVER (" in sql
        assert "PARTITION BY competitor, source_url, payload:product_name::STRING," in sql
        assert "DATE_TRUNC('day', scraped_at)" in sql
        assert "ORDER BY scraped_at DESC" in sql
        assert ") = 1" in sql

    def test_merge_match_and_insert_columns(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.merge_clean()
        sql = sf_instance.execute.call_args.args[0]

        assert "ON  t.competitor  = s.competitor" in sql
        assert "AND t.source_url  = s.source_url" in sql
        assert "AND t.product_name = s.product_name" in sql
        assert "AND DATE_TRUNC('day', t.scraped_at) = DATE_TRUNC('day', s.scraped_at)" in sql
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        assert "WHEN NOT MATCHED THEN INSERT (" in sql
        for col in ["sku", "brand", "current_price", "original_price", "currency",
                    "discount_percentage", "in_stock", "product_url", "unit", "category", "scraped_at"]:
            assert f"{col}\n" in sql or f"{col} " in sql or f"{col}=" in sql or f"{col} " in sql

    def test_merge_clean_takes_no_job_specific_params_no_injection_surface(self, module):
        """Unlike copy_into_raw, merge_clean's SQL is fully static (only
        constant table names, no per-row string interpolation) — there is
        no user-controlled input reaching this SQL string at all."""
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_instance = sf_cls.return_value.__enter__.return_value
            module.merge_clean()
            sql_first = sf_instance.execute.call_args.args[0]
            module.merge_clean()
            sql_second = sf_instance.execute.call_args.args[0]
        assert sql_first == sql_second

    def test_snowflake_exception_propagates(self, module):
        """Unlike scrape_and_upload/copy_into_raw, merge_clean does not
        catch exceptions itself — a MERGE failure is expected to fail the
        task (and, transitively, be caught by report_failures's downstream
        visibility only insofar as the task run itself goes red)."""
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.execute.side_effect = RuntimeError("boom")
            with pytest.raises(RuntimeError, match="boom"):
                module.merge_clean()


# ─────────────────────────────────────────────────────────────────────────
# report_failures
# ─────────────────────────────────────────────────────────────────────────
class TestReportFailures:
    def _ti(self, scrape_results=None, copy_results=None):
        def xcom_pull(task_ids=None, **kwargs):
            if task_ids == "scrape_and_upload":
                return scrape_results
            if task_ids == "copy_into_raw":
                return copy_results
            return None
        ti = mock.Mock()
        ti.xcom_pull.side_effect = xcom_pull
        return ti

    def test_all_succeeded_no_exception(self, module):
        ti = self._ti(
            scrape_results=[{"status": "EXTRACTED"}, {"status": "EMPTY"}],
            copy_results=[{"status": "COPIED"}, {"status": "EMPTY"}],
        )
        module.report_failures(ti=ti)  # should not raise

    def test_scrape_failures_raise(self, module):
        ti = self._ti(
            scrape_results=[{"status": "EXTRACTED"}, {"status": "FAILED", "competitor": "Bad",
                                                        "source_url": "u", "error": "boom"}],
            copy_results=[{"status": "COPIED"}],
        )
        with pytest.raises(RuntimeError, match=r"1 scrape failure\(s\), 0 copy failure\(s\)"):
            module.report_failures(ti=ti)

    def test_copy_failures_raise(self, module):
        ti = self._ti(
            scrape_results=[{"status": "EXTRACTED"}],
            copy_results=[{"status": "FAILED", "competitor": "Bad", "error": "boom"}],
        )
        with pytest.raises(RuntimeError, match=r"0 scrape failure\(s\), 1 copy failure\(s\)"):
            module.report_failures(ti=ti)

    def test_both_scrape_and_copy_failures_counted(self, module):
        ti = self._ti(
            scrape_results=[{"status": "FAILED", "competitor": "A", "source_url": "u", "error": "e1"},
                             {"status": "FAILED", "competitor": "B", "source_url": "u2", "error": "e2"}],
            copy_results=[{"status": "FAILED", "competitor": "C", "error": "e3"}],
        )
        with pytest.raises(RuntimeError, match=r"2 scrape failure\(s\), 1 copy failure\(s\)"):
            module.report_failures(ti=ti)

    def test_none_xcom_results_treated_as_empty(self, module):
        ti = self._ti(scrape_results=None, copy_results=None)
        module.report_failures(ti=ti)  # should not raise
