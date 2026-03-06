#!/usr/bin/env python3
"""Stage 2.6 gold fee enrichment for fee-complete strict recommendations."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

from etf_app import ishares_enrich as s24
from etf_app.kid_ingest import parse_ongoing_charges

PARSER_VERSION = "stage2_6_gold_fee_enrich_v1"
SOURCE_NAME = "gold_fee_enrich"

WISDOMTREE_TEMPLATE = "https://dataspanapi.wisdomtree.com/pdr/documents/PRIIP_KID/MSL/GB/EN-GB/{ISIN}/"
WISDOMTREE_TICKER_TEMPLATE = "https://dataspanapi.wisdomtree.com/pdr/documents/PRIIP_KID/{TICKER}/GB/EN-GB/{ISIN}/"
WISDOMTREE_HMSL_TEMPLATE = "https://dataspanapi.wisdomtree.com/pdr/documents/PRIIP_KID/HMSL/IE/EN-IE/{ISIN}/"
INVESCO_TEMPLATE = (
    "https://www.invesco.com/content/dam/invesco/emea/en/product-documents/etf/share-class/kid/{ISIN}_kid_en.pdf"
)
LSE_SEARCH_TEMPLATE = "https://api.londonstockexchange.com/api/gw/lse/search?worlds=quotes&q={QUERY}"
LSE_INSTRUMENT_TEMPLATE = "https://api.londonstockexchange.com/api/gw/lse/instruments/alldata/{TIDM}"

DEFAULT_ACCEPTED_FLAGS = (
    "ok",
    "partial",
    "issuer_page_ok",
    "amundi_factsheet_ok",
    "wisdomtree_kid_ok",
    "invesco_kid_ok",
    "lse_ter_ok",
)


@dataclass
class DownloadResult:
    success: bool
    pdf_bytes: Optional[bytes]
    final_url: Optional[str]
    from_cache: bool
    error: Optional[str]
    cache_path: Optional[Path]
    http_status: Optional[int]
    content_type: Optional[str]


@dataclass
class AttemptResult:
    source_key: str
    success: bool
    ongoing_charges: Optional[float]
    source_url: Optional[str]
    quality_flag: Optional[str]
    use_of_income: Optional[str]
    ucits_compliant: Optional[int]
    debug: dict[str, object]


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2.6 gold fee enrichment")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--cache-dir", default="kid_cache", help="Cache directory for PDFs")
    parser.add_argument("--limit", type=int, default=50, help="Max gold instruments to process")
    parser.add_argument("--rate-limit", type=float, default=0.2, help="HTTP delay in seconds")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--max-retries", type=int, default=1, help="HTTP retry count")
    parser.add_argument(
        "--enable-lse-fallback",
        action="store_true",
        help="Enable optional LSE fallback source (non-issuer-hosted).",
    )
    return parser.parse_args(argv)


class PdfHttpClient:
    def __init__(self, rate_limit: float, timeout: int, max_retries: int) -> None:
        self.rate_limit = max(0.0, rate_limit)
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.last_request_at = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            }
        )

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

    def get(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        request_timeout = timeout or self.timeout
        attempt = 0
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                resp = self.session.get(url, headers=headers, timeout=request_timeout, allow_redirects=True)
                self.last_request_at = time.monotonic()
                if resp.status_code in {429, 500, 502, 503, 504} and attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                return resp
            except requests.RequestException:
                if attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise
        raise RuntimeError("GET failed unexpectedly")


def normalize_space(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def ensure_tables_and_view(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_metadata_snapshot(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            asof_date TEXT,
            source TEXT,
            source_url TEXT,
            ter REAL NULL,
            use_of_income TEXT NULL,
            ucits_compliant INTEGER NULL,
            quality_flag TEXT,
            raw_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instrument_url_map(
            instrument_id INTEGER,
            url_type TEXT,
            url TEXT,
            UNIQUE(instrument_id, url_type)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_instrument_url_map_instrument ON instrument_url_map(instrument_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_issuer_metadata_snapshot_instrument ON issuer_metadata_snapshot(instrument_id)")
    conn.execute("DROP VIEW IF EXISTS instrument_cost_current")

    flag_sql = ", ".join(f"'{flag}'" for flag in DEFAULT_ACCEPTED_FLAGS)
    conn.execute(
        f"""
        CREATE VIEW instrument_cost_current AS
        WITH ranked AS (
            SELECT
                instrument_id,
                asof_date,
                ongoing_charges,
                doc_id,
                quality_flag,
                cost_id,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument_id
                    ORDER BY asof_date DESC, cost_id DESC
                ) AS rn
            FROM cost_snapshot
            WHERE ongoing_charges IS NOT NULL
              AND quality_flag IN ({flag_sql})
        )
        SELECT
            instrument_id,
            asof_date,
            ongoing_charges,
            doc_id,
            quality_flag
        FROM ranked
        WHERE rn = 1
        """
    )


def load_gold_targets(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    sql = """
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            COALESCE(iss.normalized_name, iss.issuer_name, '') AS issuer_normalized,
            l.venue_mic,
            l.ticker
        FROM instrument i
        JOIN instrument_classification ic
          ON ic.instrument_id = i.instrument_id
         AND COALESCE(ic.gold_flag, 0) = 1
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        LEFT JOIN issuer iss
          ON iss.issuer_id = i.issuer_id
        LEFT JOIN instrument_cost_current c
          ON c.instrument_id = i.instrument_id
        WHERE c.ongoing_charges IS NULL
        ORDER BY i.isin
        LIMIT ?
    """
    return conn.execute(sql, (limit,)).fetchall()


def upsert_url_map(conn: sqlite3.Connection, instrument_id: int, url_type: str, url: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_url_map(instrument_id, url_type, url)
        VALUES (?, ?, ?)
        ON CONFLICT(instrument_id, url_type) DO UPDATE SET
            url = excluded.url
        """,
        (instrument_id, url_type, url),
    )


def insert_issuer_metadata_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    source_url: Optional[str],
    ter: Optional[float],
    use_of_income: Optional[str],
    ucits_compliant: Optional[int],
    quality_flag: str,
    raw_json: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(
            instrument_id,
            asof_date,
            source,
            source_url,
            ter,
            use_of_income,
            ucits_compliant,
            quality_flag,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instrument_id,
            asof_date,
            SOURCE_NAME,
            source_url,
            ter,
            use_of_income,
            ucits_compliant,
            quality_flag,
            json.dumps(raw_json, ensure_ascii=True),
        ),
    )


def insert_cost_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    ongoing_charges: float,
    quality_flag: str,
    raw_json: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO cost_snapshot(
            instrument_id,
            asof_date,
            ongoing_charges,
            entry_costs,
            exit_costs,
            transaction_costs,
            doc_id,
            quality_flag,
            raw_json
        ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
        """,
        (instrument_id, asof_date, ongoing_charges, quality_flag, json.dumps(raw_json, ensure_ascii=True)),
    )


def is_pdf_content(content_type: Optional[str], final_url: Optional[str], content: bytes) -> bool:
    ctype = (content_type or "").lower()
    if "pdf" in ctype:
        return True
    if final_url and final_url.lower().endswith(".pdf"):
        return True
    if content.startswith(b"%PDF"):
        return True
    return False


def download_pdf_with_cache(
    client: PdfHttpClient,
    url: str,
    cache_dir: Path,
    *,
    headers: Optional[dict[str, str]] = None,
) -> DownloadResult:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = hashlib.sha256(url.encode("utf-8")).hexdigest()
    cache_path = cache_dir / f"{cache_key}.pdf"
    if cache_path.exists():
        return DownloadResult(
            success=True,
            pdf_bytes=cache_path.read_bytes(),
            final_url=url,
            from_cache=True,
            error=None,
            cache_path=cache_path,
            http_status=200,
            content_type="application/pdf",
        )

    try:
        response = client.get(url, headers=headers)
    except requests.RequestException as exc:
        return DownloadResult(
            success=False,
            pdf_bytes=None,
            final_url=None,
            from_cache=False,
            error=f"request_error:{exc.__class__.__name__}",
            cache_path=cache_path,
            http_status=None,
            content_type=None,
        )

    content = response.content or b""
    status = int(response.status_code)
    final_url = response.url or url
    ctype = response.headers.get("content-type")
    if status != 200:
        return DownloadResult(
            success=False,
            pdf_bytes=None,
            final_url=final_url,
            from_cache=False,
            error=f"http_{status}",
            cache_path=cache_path,
            http_status=status,
            content_type=ctype,
        )

    if not is_pdf_content(ctype, final_url, content):
        marker = content.find(b"%PDF")
        if marker == -1:
            return DownloadResult(
                success=False,
                pdf_bytes=None,
                final_url=final_url,
                from_cache=False,
                error="not_pdf_content",
                cache_path=cache_path,
                http_status=status,
                content_type=ctype,
            )
        content = content[marker:]

    cache_path.write_bytes(content)
    return DownloadResult(
        success=True,
        pdf_bytes=content,
        final_url=final_url,
        from_cache=False,
        error=None,
        cache_path=cache_path,
        http_status=status,
        content_type=ctype,
    )


def parse_pdf_ongoing_charges(pdf_bytes: bytes) -> tuple[Optional[float], dict[str, object]]:
    parsed = parse_ongoing_charges(pdf_bytes)
    ongoing = parsed.get("ongoing_charges")
    value = float(ongoing) if isinstance(ongoing, (int, float)) else None
    debug = {
        "ongoing_charges": value,
        "entry_costs": parsed.get("entry_costs"),
        "exit_costs": parsed.get("exit_costs"),
        "transaction_costs": parsed.get("transaction_costs"),
        "effective_date": parsed.get("effective_date"),
        "language": parsed.get("language"),
        "snippet": parsed.get("snippet"),
        "extractor": parsed.get("extractor"),
        "fallback_used": parsed.get("fallback_used"),
        "page_count": parsed.get("page_count"),
        "errors": parsed.get("errors"),
    }
    return value, debug


def is_ishares_candidate(issuer_normalized: str, instrument_name: str) -> bool:
    hay = f"{issuer_normalized} {instrument_name}".upper()
    return ("ISHARES" in hay) or ("BLACKROCK" in hay)


def is_wisdomtree_candidate(issuer_normalized: str, instrument_name: str) -> bool:
    hay = f"{issuer_normalized} {instrument_name}".upper()
    return ("WISDOMTREE" in hay) or ("GOLD BULLION SECURITIES" in hay)


def is_invesco_candidate(issuer_normalized: str, instrument_name: str) -> bool:
    hay = f"{issuer_normalized} {instrument_name}".upper()
    return "INVESCO" in hay


def attempt_ishares(
    conn: sqlite3.Connection,
    ishares_client: s24.HttpClient,
    instrument_id: int,
    isin: str,
) -> AttemptResult:
    debug: dict[str, object] = {"method": "ishares_product_page", "isin": isin}
    product_url = s24.find_existing_ishares_product_url(conn, instrument_id)
    if product_url:
        debug["url_source"] = "existing_map_or_logs"

    if not product_url:
        candidates, discovery_debug = s24.discover_candidates_by_isin(ishares_client, isin)
        debug["discovery"] = discovery_debug
        debug["candidate_count"] = len(candidates)
        for candidate in candidates:
            ok, accepted_url, html, verify_debug = s24.verify_candidate_for_isin(ishares_client, candidate, isin)
            checks = debug.setdefault("candidate_checks", [])
            if isinstance(checks, list):
                checks.append(verify_debug)
            if ok and accepted_url:
                product_url = accepted_url
                if html:
                    debug["verified_html"] = html
                break

    if not product_url:
        debug["error"] = "no_url"
        return AttemptResult("ishares", False, None, None, None, None, None, debug)

    s24.upsert_instrument_url_map(conn, instrument_id, product_url)

    html = debug.get("verified_html") if isinstance(debug.get("verified_html"), str) else None
    fetch_result: Optional[s24.HttpResult] = None
    if not html:
        fetch_result = ishares_client.get(s24.add_site_entry_params(product_url))
        debug["page_fetch_status"] = fetch_result.status_code
        debug["page_fetch_content_type"] = fetch_result.content_type
        debug["page_fetch_error"] = fetch_result.error
        if not fetch_result.ok or not fetch_result.text:
            debug["error"] = "download_fail"
            return AttemptResult("ishares", False, None, product_url, None, None, None, debug)
        html = fetch_result.text
        product_url = s24.canonical_product_url(fetch_result.final_url or product_url)

    parsed = s24.parse_ishares_product_page(html or "")
    ter = parsed.get("ter")
    use_of_income = parsed.get("use_of_income")
    ucits = parsed.get("ucits_compliant")
    debug["parsed"] = {
        "ter": ter,
        "use_of_income": use_of_income,
        "ucits_compliant": ucits,
        "facts_keys": sorted(list((parsed.get("facts") or {}).keys()))[:30],
    }
    if not isinstance(ter, (int, float)):
        debug["error"] = "parse_fail"
        return AttemptResult("ishares", False, None, product_url, None, None, None, debug)

    return AttemptResult(
        source_key="ishares",
        success=True,
        ongoing_charges=float(ter),
        source_url=product_url,
        quality_flag="issuer_page_ok",
        use_of_income=str(use_of_income) if use_of_income is not None else None,
        ucits_compliant=int(ucits) if ucits is not None else None,
        debug=debug,
    )


def attempt_wisdomtree(
    conn: sqlite3.Connection,
    pdf_client: PdfHttpClient,
    cache_dir: Path,
    instrument_id: int,
    isin: str,
    ticker: str,
) -> AttemptResult:
    ticker_clean = (ticker or "").strip().upper()
    candidates = [WISDOMTREE_TEMPLATE.format(ISIN=isin.upper())]
    if ticker_clean:
        candidates.append(WISDOMTREE_TICKER_TEMPLATE.format(TICKER=ticker_clean, ISIN=isin.upper()))
    candidates.append(WISDOMTREE_HMSL_TEMPLATE.format(ISIN=isin.upper()))

    debug: dict[str, object] = {"method": "wisdomtree_kid_template", "candidates": candidates, "attempts": []}
    dl: Optional[DownloadResult] = None
    selected_url: Optional[str] = None
    parse_debug: dict[str, object] = {}
    ongoing: Optional[float] = None

    for url in candidates:
        dl = download_pdf_with_cache(pdf_client, url, cache_dir)
        attempt = {
            "url": url,
            "download": {
                "success": dl.success,
                "error": dl.error,
                "final_url": dl.final_url,
                "status": dl.http_status,
                "content_type": dl.content_type,
                "from_cache": dl.from_cache,
            },
        }
        if dl.success and dl.pdf_bytes:
            parsed_value, parsed_debug = parse_pdf_ongoing_charges(dl.pdf_bytes)
            attempt["parse"] = parsed_debug
            if parsed_value is not None:
                selected_url = dl.final_url or url
                ongoing = parsed_value
                parse_debug = parsed_debug
                debug["attempts"].append(attempt)
                break
        debug["attempts"].append(attempt)

    if ongoing is None:
        fallback_url = selected_url or (candidates[0] if candidates else None)
        return AttemptResult("wisdomtree", False, None, fallback_url, None, None, None, debug)

    upsert_url_map(conn, instrument_id, "wisdomtree_kid_pdf", selected_url or candidates[0])
    debug["parse"] = parse_debug
    return AttemptResult(
        source_key="wisdomtree",
        success=True,
        ongoing_charges=ongoing,
        source_url=selected_url or candidates[0],
        quality_flag="wisdomtree_kid_ok",
        use_of_income=None,
        ucits_compliant=None,
        debug=debug,
    )


def attempt_invesco(
    conn: sqlite3.Connection,
    pdf_client: PdfHttpClient,
    cache_dir: Path,
    instrument_id: int,
    isin: str,
) -> AttemptResult:
    url = INVESCO_TEMPLATE.format(ISIN=isin.upper())
    debug: dict[str, object] = {"method": "invesco_kid_template", "url": url}

    dl = download_pdf_with_cache(pdf_client, url, cache_dir)
    debug["download_first"] = {
        "success": dl.success,
        "error": dl.error,
        "status": dl.http_status,
        "content_type": dl.content_type,
        "final_url": dl.final_url,
        "from_cache": dl.from_cache,
    }
    if (not dl.success) and dl.http_status == 406:
        browser_headers = {
            "Accept": "application/pdf,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }
        dl = download_pdf_with_cache(pdf_client, url, cache_dir, headers=browser_headers)
        debug["download_retry_406"] = {
            "success": dl.success,
            "error": dl.error,
            "status": dl.http_status,
            "content_type": dl.content_type,
            "final_url": dl.final_url,
            "from_cache": dl.from_cache,
        }

    if not dl.success or not dl.pdf_bytes:
        return AttemptResult("invesco", False, None, url, None, None, None, debug)

    upsert_url_map(conn, instrument_id, "invesco_kid_pdf", dl.final_url or url)
    ongoing, parse_debug = parse_pdf_ongoing_charges(dl.pdf_bytes)
    debug["parse"] = parse_debug
    if ongoing is None:
        return AttemptResult("invesco", False, None, dl.final_url or url, None, None, None, debug)
    return AttemptResult(
        source_key="invesco",
        success=True,
        ongoing_charges=ongoing,
        source_url=dl.final_url or url,
        quality_flag="invesco_kid_ok",
        use_of_income=None,
        ucits_compliant=None,
        debug=debug,
    )


def parse_lse_fee_from_html(html: str) -> Optional[float]:
    normalized = normalize_space(html)
    patterns = [
        r"\bTER\b.{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*%",
        r"\bOngoing(?:\s+charges?)?\b.{0,120}?([0-9]+(?:[.,][0-9]+)?)\s*%",
        r"\bTotal expense ratio\b.{0,120}?([0-9]+(?:[.,][0-9]+)?)\s*%",
    ]
    for pattern in patterns:
        m = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not m:
            continue
        token = m.group(1).replace(",", ".")
        try:
            value = float(token)
        except ValueError:
            continue
        if 0 <= value <= 100:
            return value
    return None


def resolve_lse_tidm(ticker: str, isin: str) -> Optional[str]:
    query = ticker.strip() or isin.strip()
    if not query:
        return None
    url = LSE_SEARCH_TEMPLATE.format(QUERY=requests.utils.quote(query))
    try:
        response = requests.get(url, timeout=20)
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    instruments = payload.get("instruments") if isinstance(payload, dict) else None
    if not isinstance(instruments, list):
        return None

    isin_upper = isin.upper()
    ticker_upper = ticker.upper()
    for item in instruments:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").upper()
        url_text = str(item.get("url") or "")
        if key == isin_upper:
            m = re.search(r"/stock/([^/]+)/", url_text, flags=re.IGNORECASE)
            if m:
                return m.group(1).upper()
    for item in instruments:
        if not isinstance(item, dict):
            continue
        code = str(item.get("key") or "").upper()
        if code == ticker_upper:
            return code
    return None


def attempt_lse_fallback(
    instrument_name: str,
    isin: str,
    ticker: str,
) -> AttemptResult:
    debug: dict[str, object] = {
        "method": "lse_fallback",
        "isin": isin,
        "ticker": ticker,
        "instrument_name": instrument_name,
    }
    tidm = resolve_lse_tidm(ticker, isin)
    debug["resolved_tidm"] = tidm
    if not tidm:
        return AttemptResult("lse_fallback", False, None, None, None, None, None, debug)

    instrument_url = LSE_INSTRUMENT_TEMPLATE.format(TIDM=tidm)
    try:
        response = requests.get(instrument_url, timeout=20)
    except requests.RequestException as exc:
        debug["error"] = f"request_error:{exc.__class__.__name__}"
        return AttemptResult("lse_fallback", False, None, instrument_url, None, None, None, debug)
    debug["instrument_api_status"] = response.status_code
    debug["instrument_api_content_type"] = response.headers.get("content-type")
    if response.status_code != 200:
        return AttemptResult("lse_fallback", False, None, instrument_url, None, None, None, debug)

    api_payload: dict[str, object] = {}
    try:
        api_payload = response.json()
    except ValueError:
        api_payload = {}
    debug["instrument_api_keys"] = sorted(api_payload.keys())[:30] if isinstance(api_payload, dict) else []

    page_url = f"https://www.londonstockexchange.com/stock/{tidm}/wisdomtree"
    try:
        page_resp = requests.get(page_url, timeout=20)
    except requests.RequestException as exc:
        debug["page_error"] = f"request_error:{exc.__class__.__name__}"
        return AttemptResult("lse_fallback", False, None, page_url, None, None, None, debug)
    debug["page_status"] = page_resp.status_code
    debug["page_content_type"] = page_resp.headers.get("content-type")
    fee = parse_lse_fee_from_html(page_resp.text if page_resp.status_code == 200 else "")
    debug["parsed_fee"] = fee
    if fee is None:
        return AttemptResult("lse_fallback", False, None, page_url, None, None, None, debug)
    return AttemptResult(
        source_key="lse_fallback",
        success=True,
        ongoing_charges=fee,
        source_url=page_url,
        quality_flag="lse_ter_ok",
        use_of_income=None,
        ucits_compliant=None,
        debug=debug,
    )


def backfill_primary_currency_from_lse(
    conn: sqlite3.Connection,
    instrument_id: int,
    ticker: str,
) -> dict[str, object]:
    debug: dict[str, object] = {"ticker": ticker}
    row = conn.execute(
        """
        SELECT listing_id, venue_mic, trading_currency
        FROM listing
        WHERE instrument_id = ?
          AND COALESCE(primary_flag, 0) = 1
        LIMIT 1
        """,
        (instrument_id,),
    ).fetchone()
    if not row:
        debug["status"] = "no_primary_listing"
        return debug
    current = normalize_space(str(row["trading_currency"] or ""))
    if current:
        debug["status"] = "already_present"
        debug["current_currency"] = current
        return debug
    tidm = normalize_space(ticker).upper()
    if not tidm:
        debug["status"] = "missing_ticker"
        return debug

    url = LSE_INSTRUMENT_TEMPLATE.format(TIDM=requests.utils.quote(tidm))
    debug["lookup_url"] = url
    try:
        response = requests.get(url, timeout=20)
    except requests.RequestException as exc:
        debug["status"] = f"request_error:{exc.__class__.__name__}"
        return debug

    debug["api_status"] = response.status_code
    if response.status_code != 200:
        debug["status"] = "api_non_200"
        return debug
    try:
        payload = response.json()
    except ValueError:
        debug["status"] = "api_invalid_json"
        return debug
    currency = normalize_space(str(payload.get("currency") or ""))
    if not currency:
        debug["status"] = "currency_missing_in_api"
        return debug

    conn.execute(
        """
        UPDATE listing
        SET trading_currency = ?
        WHERE listing_id = ?
        """,
        (currency, int(row["listing_id"])),
    )
    debug["status"] = "updated"
    debug["currency"] = currency
    return debug


def print_target_list(rows: list[sqlite3.Row]) -> None:
    print("\n=== Gold Missing-Fee Targets ===")
    print("ISIN | instrument_name | issuer_normalized | venue | ticker")
    for row in rows:
        instrument_name = normalize_space(str(row["instrument_name"] or ""))
        issuer = normalize_space(str(row["issuer_normalized"] or ""))
        print(
            f"{row['isin']} | {instrument_name} | {issuer} | "
            f"{row['venue_mic'] or ''} | {row['ticker'] or ''}"
        )


def print_kpis(
    *,
    attempted: int,
    filled: int,
    source_counts: dict[str, int],
    samples: list[tuple[str, str, str, str, float]],
) -> None:
    print("\n=== Stage 2.6 Gold KPIs ===")
    print(f"attempted_gold: {attempted}")
    print(f"fees_filled_gold: {filled}")
    print(f"filled_by_ishares: {source_counts.get('ishares', 0)}")
    print(f"filled_by_wisdomtree: {source_counts.get('wisdomtree', 0)}")
    print(f"filled_by_invesco: {source_counts.get('invesco', 0)}")
    print(f"filled_by_lse_fallback: {source_counts.get('lse_fallback', 0)}")

    print("\n=== Sample Successes (up to 10) ===")
    if not samples:
        print("No successful fee fills.")
        return
    for isin, ticker, source_key, source_url, ongoing in samples[:10]:
        print(f"{isin} | {ticker} | {source_key} | {ongoing:.4f} | {source_url}")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    pdf_client = PdfHttpClient(
        rate_limit=args.rate_limit,
        timeout=args.timeout,
        max_retries=args.max_retries,
    )
    ishares_client = s24.HttpClient(
        rate_limit=args.rate_limit,
        timeout=args.timeout,
        max_retries=args.max_retries,
    )
    cache_dir = Path(args.cache_dir)
    asof_date = dt.date.today().isoformat()

    attempted = 0
    filled = 0
    source_counts = {"ishares": 0, "wisdomtree": 0, "invesco": 0, "lse_fallback": 0}
    sample_successes: list[tuple[str, str, str, str, float]] = []

    try:
        conn.execute("BEGIN")
        ensure_tables_and_view(conn)

        targets = load_gold_targets(conn, args.limit)
        attempted = len(targets)
        print_target_list(targets)
        log(f"Loaded gold missing-fee subset: {attempted} rows (limit={args.limit})")
        if not targets:
            conn.commit()
            print_kpis(attempted=0, filled=0, source_counts=source_counts, samples=sample_successes)
            return 0

        for idx, row in enumerate(targets, start=1):
            instrument_id = int(row["instrument_id"])
            isin = str(row["isin"])
            instrument_name = str(row["instrument_name"] or "")
            issuer_normalized = str(row["issuer_normalized"] or "")
            ticker = str(row["ticker"] or "")

            debug: dict[str, object] = {
                "parser_version": PARSER_VERSION,
                "instrument_id": instrument_id,
                "isin": isin,
                "instrument_name": instrument_name,
                "issuer_normalized": issuer_normalized,
                "attempted_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "attempt_chain": [],
            }
            chosen_result: Optional[AttemptResult] = None

            currency_backfill = backfill_primary_currency_from_lse(conn, instrument_id, ticker)
            debug["currency_backfill"] = currency_backfill

            if is_ishares_candidate(issuer_normalized, instrument_name):
                result = attempt_ishares(conn, ishares_client, instrument_id, isin)
                debug["attempt_chain"].append({"source": "ishares", "success": result.success, "debug": result.debug})
                if result.success:
                    chosen_result = result

            if chosen_result is None and is_wisdomtree_candidate(issuer_normalized, instrument_name):
                result = attempt_wisdomtree(conn, pdf_client, cache_dir, instrument_id, isin, ticker)
                debug["attempt_chain"].append({"source": "wisdomtree", "success": result.success, "debug": result.debug})
                if result.success:
                    chosen_result = result

            if chosen_result is None and is_invesco_candidate(issuer_normalized, instrument_name):
                result = attempt_invesco(conn, pdf_client, cache_dir, instrument_id, isin)
                debug["attempt_chain"].append({"source": "invesco", "success": result.success, "debug": result.debug})
                if result.success:
                    chosen_result = result

            if chosen_result is None and args.enable_lse_fallback:
                result = attempt_lse_fallback(instrument_name, isin, ticker)
                debug["attempt_chain"].append({"source": "lse_fallback", "success": result.success, "debug": result.debug})
                if result.success:
                    chosen_result = result

            if chosen_result and chosen_result.success and chosen_result.ongoing_charges is not None:
                insert_cost_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    ongoing_charges=float(chosen_result.ongoing_charges),
                    quality_flag=str(chosen_result.quality_flag),
                    raw_json={
                        "source": SOURCE_NAME,
                        "selected_source": chosen_result.source_key,
                        "source_url": chosen_result.source_url,
                        "parser_version": PARSER_VERSION,
                        "attempt_chain": debug["attempt_chain"],
                    },
                )
                insert_issuer_metadata_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    source_url=chosen_result.source_url,
                    ter=float(chosen_result.ongoing_charges),
                    use_of_income=chosen_result.use_of_income,
                    ucits_compliant=chosen_result.ucits_compliant,
                    quality_flag="ok",
                    raw_json={
                        "source": SOURCE_NAME,
                        "selected_source": chosen_result.source_key,
                        "attempt_chain": debug["attempt_chain"],
                    },
                )
                filled += 1
                source_counts[chosen_result.source_key] = source_counts.get(chosen_result.source_key, 0) + 1
                if len(sample_successes) < 10:
                    sample_successes.append(
                        (
                            isin,
                            ticker,
                            chosen_result.source_key,
                            str(chosen_result.source_url or ""),
                            float(chosen_result.ongoing_charges),
                        )
                    )
            else:
                final_quality = "parse_fail"
                if not debug["attempt_chain"]:
                    final_quality = "no_source"
                elif any(
                    any(
                        token in str(item.get("debug", {}).get("error", "")).lower()
                        for token in ("download", "http_", "request_error", "not_pdf")
                    )
                    for item in debug["attempt_chain"]
                    if isinstance(item, dict)
                ):
                    final_quality = "download_fail"

                insert_issuer_metadata_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    source_url=None,
                    ter=None,
                    use_of_income=None,
                    ucits_compliant=None,
                    quality_flag=final_quality,
                    raw_json=debug,
                )

            if idx % 10 == 0:
                log(f"Processed {idx}/{attempted}: fees_filled={filled}")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print_kpis(attempted=attempted, filled=filled, source_counts=source_counts, samples=sample_successes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
