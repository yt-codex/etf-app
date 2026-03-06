#!/usr/bin/env python3
"""Stage 2.4 iShares metadata enrichment (HTML product pages)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

PARSER_VERSION = "stage2_4_ishares_enrich_v1"
ISHARES_SOURCE = "ishares_product_page"

SEARCH_URL = "https://www.ishares.com/uk/individual/en/products"
AUTOCOMPLETE_URL = "https://www.ishares.com/uk/individual/en/autoComplete.search"
AUTOCOMPLETE_CANDIDATE_LIMIT = 12
VERIFICATION_CANDIDATE_LIMIT = 10

DISCOVERY_NAME_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    (r"\bISHRS\b", "ISHARES"),
    (r"\bISHS\b", "ISHARES"),
    (r"\bU\.?ETF\b", "UCITS ETF"),
    (r"\bUCT\b", "UCITS"),
    (r"\bUCTS\b", "UCITS"),
    (r"\bUC\.?E\.?ACC\b", "UCITS ETF ACC"),
    (r"\bUC\.?E\.?DIST\b", "UCITS ETF DIST"),
    (r"\bUC\.?E\.?DIS\b", "UCITS ETF DIST"),
    (r"\bC\.BD\b", "CORP BOND"),
    (r"\bCORP BD\b", "CORP BOND"),
    (r"\bGOV BD\b", "GOV BOND"),
    (r"\bTRSY\b", "TREASURY"),
    (r"\bN\.AMERICA\b", "NORTH AMERICA"),
    (r"\bLGE CAP\b", "LARGE CAP"),
    (r"\bS\+P\b", "S&P"),
)

DISCOVERY_STRIP_PATTERNS: tuple[str, ...] = (
    r"\bISHARES\b",
    r"\bCORE\b",
    r"\bEDGE\b",
    r"\bUCITS\b",
    r"\bETF\b",
    r"\bUSD\b",
    r"\bEUR\b",
    r"\bGBP\b",
    r"\bJPY\b",
    r"\bCHF\b",
    r"\bACC\b",
    r"\bDIST\b",
    r"\bDIS\b",
    r"\bHEDGED\b",
    r"\bHDG\b",
    r"\b\(ACC\)\b",
    r"\b\(DIST\)\b",
)


@dataclass
class HttpResult:
    ok: bool
    status_code: Optional[int]
    content_type: Optional[str]
    text: Optional[str]
    final_url: Optional[str]
    error: Optional[str]


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2.4 iShares metadata enrichment")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--limit", type=int, default=500, help="Maximum instruments to attempt")
    parser.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue filter",
    )
    parser.add_argument("--rate-limit", type=float, default=0.2, help="HTTP delay in seconds")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--max-retries", type=int, default=1, help="HTTP retries")
    parser.add_argument(
        "--reuse-only",
        action="store_true",
        help="Only reuse existing URL mappings; skip constrained lookup",
    )
    return parser.parse_args(argv)


class HttpClient:
    def __init__(self, rate_limit: float, timeout: int, max_retries: int) -> None:
        self.rate_limit = max(0.0, rate_limit)
        self.timeout = timeout
        self.max_retries = max_retries
        self.last_ts = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            }
        )

    def _throttle(self) -> None:
        now = time.monotonic()
        wait_for = self.rate_limit - (now - self.last_ts)
        if wait_for > 0:
            time.sleep(wait_for)
        self.last_ts = time.monotonic()

    def get(self, url: str, *, params: Optional[dict[str, str]] = None) -> HttpResult:
        last_error: Optional[str] = None
        for attempt in range(self.max_retries + 1):
            try:
                self._throttle()
                resp = self.session.get(url, params=params, timeout=self.timeout)
                ctype = resp.headers.get("Content-Type")
                return HttpResult(
                    ok=resp.status_code == 200,
                    status_code=resp.status_code,
                    content_type=ctype,
                    text=resp.text,
                    final_url=resp.url,
                    error=None if resp.status_code == 200 else f"http_{resp.status_code}",
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < self.max_retries:
                    time.sleep(min(2.0, 0.3 * (attempt + 1)))
                    continue
                return HttpResult(
                    ok=False,
                    status_code=None,
                    content_type=None,
                    text=None,
                    final_url=None,
                    error=last_error,
                )
        return HttpResult(False, None, None, None, None, last_error)


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

    # Keep Stage 4 strict fee-complete flow aware of issuer-page sourced TER.
    conn.execute("DROP VIEW IF EXISTS instrument_cost_current")
    conn.execute(
        """
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
              AND quality_flag IN ('ok', 'partial', 'issuer_page_ok', 'amundi_factsheet_ok')
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


def venue_scope(arg: str) -> list[str]:
    if arg == "XLON":
        return ["XLON"]
    if arg == "XETR":
        return ["XETR"]
    return ["XLON", "XETR"]


def product_profile_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='product_profile'"
    ).fetchone()
    return row is not None


def load_targets(conn: sqlite3.Connection, limit: int, venue: str) -> list[sqlite3.Row]:
    venues = venue_scope(venue)
    placeholders = ",".join("?" for _ in venues)
    profile_join = ""
    profile_filter = "AND icc.ongoing_charges IS NULL"
    if product_profile_exists(conn):
        profile_join = "LEFT JOIN product_profile p ON p.instrument_id = i.instrument_id"
        profile_filter = """
          AND (
              icc.ongoing_charges IS NULL
              OR p.instrument_id IS NULL
              OR p.benchmark_name IS NULL OR TRIM(p.benchmark_name) = ''
              OR p.asset_class_hint IS NULL OR TRIM(p.asset_class_hint) = ''
              OR p.domicile_country IS NULL OR TRIM(p.domicile_country) = ''
              OR p.replication_method IS NULL OR TRIM(p.replication_method) = ''
              OR p.hedged_flag IS NULL
          )
        """
    sql = f"""
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            l.ticker,
            l.venue_mic,
            COALESCE(iss.normalized_name, iss.issuer_name, '') AS issuer_normalized
        FROM instrument i
        JOIN listing l ON l.instrument_id = i.instrument_id AND l.primary_flag = 1
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id
        {profile_join}
        WHERE i.universe_mvp_flag = 1
          AND l.venue_mic IN ({placeholders})
          AND (
              UPPER(COALESCE(iss.normalized_name, '')) LIKE '%ISHARES%'
              OR UPPER(i.instrument_name) LIKE '%ISHARES%'
          )
          {profile_filter}
        ORDER BY i.isin
        LIMIT ?
    """
    params: list[object] = [*venues, limit]
    return conn.execute(sql, params).fetchall()


def canonical_product_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(query="", fragment="")
    return urlunparse(clean)


def add_site_entry_params(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params["switchLocale"] = ["y"]
    params["siteEntryPassthrough"] = ["true"]
    query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=query))


def find_existing_ishares_product_url(conn: sqlite3.Connection, instrument_id: int) -> Optional[str]:
    row = conn.execute(
        """
        SELECT url
        FROM instrument_url_map
        WHERE instrument_id = ?
          AND url_type = 'ishares_product_page'
        """,
        (instrument_id,),
    ).fetchone()
    if row and row["url"]:
        return str(row["url"])

    row = conn.execute(
        """
        SELECT url
        FROM document
        WHERE instrument_id = ?
          AND url LIKE '%ishares.com%'
          AND url LIKE '%/products/%'
        ORDER BY document_id DESC
        LIMIT 1
        """,
        (instrument_id,),
    ).fetchone()
    if row and row["url"]:
        return canonical_product_url(str(row["url"]))

    row = conn.execute(
        """
        SELECT candidate_url
        FROM kid_candidate_url
        WHERE instrument_id = ?
          AND candidate_url LIKE '%ishares.com%'
          AND candidate_url LIKE '%/products/%'
        ORDER BY id DESC
        LIMIT 1
        """,
        (instrument_id,),
    ).fetchone()
    if row and row["candidate_url"]:
        return canonical_product_url(str(row["candidate_url"]))

    return None


def upsert_instrument_url_map(conn: sqlite3.Connection, instrument_id: int, url: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_url_map(instrument_id, url_type, url)
        VALUES (?, 'ishares_product_page', ?)
        ON CONFLICT(instrument_id, url_type) DO UPDATE SET
            url = excluded.url
        """,
        (instrument_id, canonical_product_url(url)),
    )


def extract_product_links_from_html(html: str, base_url: str) -> list[str]:
    pattern = re.compile(r'href=["\']([^"\']*?/products/\d+(?:/[^"\']*)?)["\']', flags=re.IGNORECASE)
    out: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(html):
        raw = match.group(1).strip()
        absolute = urljoin(base_url, raw)
        canonical = canonical_product_url(absolute)
        if "/products/" not in canonical:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        out.append(canonical)
    return out


def discover_candidates_by_isin(client: HttpClient, isin: str) -> tuple[list[str], dict[str, object]]:
    debug: dict[str, object] = {}
    candidates: list[str] = []
    seen: set[str] = set()

    search_resp = client.get(SEARCH_URL, params={"search": isin})
    debug["search_status"] = search_resp.status_code
    if search_resp.ok and search_resp.text:
        found = extract_product_links_from_html(search_resp.text, str(search_resp.final_url or SEARCH_URL))
        debug["search_candidates"] = found[:10]
        for url in found:
            if url not in seen:
                seen.add(url)
                candidates.append(url)
    else:
        debug["search_error"] = search_resp.error

    auto_resp = client.get(
        AUTOCOMPLETE_URL,
        params={"type": "autocomplete", "term": isin, "siteEntryPassthrough": "true"},
    )
    debug["autocomplete_status"] = auto_resp.status_code
    if auto_resp.ok and auto_resp.text:
        content_type = (auto_resp.content_type or "").lower()
        if "json" in content_type:
            try:
                payload = json.loads(auto_resp.text)
                from_auto: list[str] = []
                for item in payload:
                    url = str(item.get("id") or "").strip()
                    if "/products/" not in url:
                        continue
                    canonical = canonical_product_url(urljoin("https://www.ishares.com", url))
                    if canonical not in seen:
                        seen.add(canonical)
                        candidates.append(canonical)
                    from_auto.append(canonical)
                debug["autocomplete_candidates"] = from_auto[:10]
            except json.JSONDecodeError:
                debug["autocomplete_parse_error"] = "invalid_json"
        else:
            debug["autocomplete_parse_error"] = f"unexpected_content_type:{auto_resp.content_type}"
    else:
        debug["autocomplete_error"] = auto_resp.error

    return candidates, debug


def discover_candidates_for_instrument(
    client: HttpClient,
    *,
    isin: str,
    ticker: Optional[str],
    instrument_name: Optional[str],
) -> tuple[list[str], dict[str, object]]:
    debug: dict[str, object] = {}
    candidates_by_url: dict[str, dict[str, object]] = {}
    search_terms = build_discovery_search_terms(isin=isin, ticker=ticker, instrument_name=instrument_name)
    debug["search_terms"] = search_terms

    for term in search_terms:
        term_debug: dict[str, object] = {"term": term}
        response = client.get(
            AUTOCOMPLETE_URL,
            params={"type": "autocomplete", "term": term, "siteEntryPassthrough": "true"},
        )
        term_debug["status"] = response.status_code
        term_debug["content_type"] = response.content_type
        if not response.ok or not response.text:
            term_debug["error"] = response.error
            debug.setdefault("term_attempts", []).append(term_debug)
            continue

        if "json" not in str(response.content_type or "").lower():
            term_debug["error"] = f"unexpected_content_type:{response.content_type}"
            debug.setdefault("term_attempts", []).append(term_debug)
            continue

        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            term_debug["error"] = "invalid_json"
            debug.setdefault("term_attempts", []).append(term_debug)
            continue

        accepted: list[dict[str, object]] = []
        for item in payload[:AUTOCOMPLETE_CANDIDATE_LIMIT]:
            if str(item.get("category") or "") != "productAutocomplete":
                continue
            raw_url = str(item.get("id") or "").strip()
            if "/products/" not in raw_url:
                continue
            absolute = canonical_product_url(urljoin("https://www.ishares.com", raw_url))
            label = normalize_space(str(item.get("label") or ""))
            score = score_autocomplete_candidate(
                label=label,
                instrument_name=instrument_name,
                ticker=ticker,
                search_term=term,
            )
            accepted.append({"label": label, "url": absolute, "score": score})
            current = candidates_by_url.get(absolute)
            if current is None or score > int(current["score"]):
                candidates_by_url[absolute] = {
                    "url": absolute,
                    "label": label,
                    "score": score,
                    "term": term,
                }

        term_debug["candidates"] = accepted[:8]
        debug.setdefault("term_attempts", []).append(term_debug)

    ranked = sorted(
        candidates_by_url.values(),
        key=lambda item: (-int(item["score"]), str(item["label"]), str(item["url"])),
    )
    debug["ranked_candidates"] = ranked[:VERIFICATION_CANDIDATE_LIMIT]
    return [str(item["url"]) for item in ranked[:VERIFICATION_CANDIDATE_LIMIT]], debug


def verify_candidate_for_isin(
    client: HttpClient,
    candidate_url: str,
    isin: str,
) -> tuple[bool, Optional[str], Optional[str], dict[str, object]]:
    fetch_url = add_site_entry_params(candidate_url)
    resp = client.get(fetch_url)
    debug: dict[str, object] = {
        "candidate_url": candidate_url,
        "fetch_url": fetch_url,
        "status": resp.status_code,
        "content_type": resp.content_type,
    }
    if not resp.ok or not resp.text:
        debug["reason"] = resp.error or "fetch_failed"
        return False, None, None, debug

    html = resp.text
    if isin.upper() not in html.upper():
        debug["reason"] = "isin_not_found_on_page"
        return False, None, html, debug

    # Prefer canonical href if present.
    canonical = None
    try:
        soup = BeautifulSoup(html, "html.parser")
        canonical_tag = soup.find("link", attrs={"rel": "canonical"})
        if canonical_tag and canonical_tag.get("href"):
            canonical = canonical_product_url(str(canonical_tag.get("href")))
    except Exception:
        canonical = None
    accepted = canonical or canonical_product_url(str(resp.final_url or candidate_url))
    debug["reason"] = "accepted_contains_isin"
    debug["accepted_url"] = accepted
    return True, accepted, html, debug


def normalize_space(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_discovery_name(value: Optional[str]) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    text = text.upper().replace("&", " AND ")
    for pattern, replacement in DISCOVERY_NAME_REPLACEMENTS:
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"[^A-Z0-9$&+()/-]+", " ", text)
    return normalize_space(text)


def build_discovery_search_terms(
    *,
    isin: str,
    ticker: Optional[str],
    instrument_name: Optional[str],
) -> list[str]:
    terms: list[str] = []
    raw_name = normalize_space(instrument_name)
    normalized_name = normalize_discovery_name(instrument_name)
    benchmark_fragment = normalized_name
    for pattern in DISCOVERY_STRIP_PATTERNS:
        benchmark_fragment = re.sub(pattern, " ", benchmark_fragment)
    benchmark_fragment = normalize_space(benchmark_fragment)

    for candidate in (
        isin.strip().upper(),
        normalize_space(ticker).upper(),
        raw_name,
        normalized_name,
        benchmark_fragment,
        f"ISHARES {benchmark_fragment}" if benchmark_fragment else "",
        f"ISHARES CORE {benchmark_fragment}" if benchmark_fragment and "CORE" not in normalized_name else "",
    ):
        candidate = normalize_space(candidate)
        if not candidate:
            continue
        if candidate not in terms:
            terms.append(candidate)
    return terms[:6]


def normalize_candidate_match_text(value: Optional[str]) -> str:
    text = normalize_discovery_name(value)
    text = re.sub(r"\bDIST(RIBUTING)?\b", "DIST", text)
    text = re.sub(r"\bACC(UMULATING)?\b", "ACC", text)
    return normalize_space(text)


def score_autocomplete_candidate(
    *,
    label: str,
    instrument_name: Optional[str],
    ticker: Optional[str],
    search_term: str,
) -> int:
    normalized_label = normalize_candidate_match_text(label)
    normalized_name = normalize_candidate_match_text(instrument_name)
    normalized_term = normalize_candidate_match_text(search_term)
    label_tokens = {token for token in normalized_label.split() if len(token) > 1}
    name_tokens = {token for token in normalized_name.split() if len(token) > 1}
    term_tokens = {token for token in normalized_term.split() if len(token) > 1}
    overlap = len(label_tokens & name_tokens)
    score = overlap * 5 + len(label_tokens & term_tokens) * 3

    ticker_token = normalize_space(ticker).upper()
    if ticker_token and ticker_token == normalize_space(search_term).upper():
        score += 8

    if " DIST" in f" {normalized_label} " and " DIST" in f" {normalized_name} ":
        score += 4
    if " ACC" in f" {normalized_label} " and " ACC" in f" {normalized_name} ":
        score += 4
    if normalized_term and normalized_term in normalized_label:
        score += 6
    if normalized_name and normalized_label == normalized_name:
        score += 12
    return score


def parse_percent(value: str) -> Optional[float]:
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*%", value)
    if m:
        token = m.group(1).replace(",", ".")
        try:
            return float(token)
        except ValueError:
            return None
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", value.strip()):
        try:
            return float(value.strip().replace(",", "."))
        except ValueError:
            return None
    return None


def _lookup_fact_value(
    facts: dict[str, str],
    *,
    exact_keys: tuple[str, ...] = (),
    contains_keys: tuple[str, ...] = (),
) -> Optional[str]:
    exact_upper = {key.upper() for key in exact_keys}
    for key, value in facts.items():
        key_upper = normalize_space(key).upper()
        if key_upper in exact_upper:
            cleaned = normalize_space(value)
            if cleaned:
                return cleaned
    for key, value in facts.items():
        key_upper = normalize_space(key).upper()
        if any(token in key_upper for token in contains_keys):
            cleaned = normalize_space(value)
            if cleaned:
                return cleaned
    return None


def _normalize_replication_method(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    upper = text.upper()
    if "PHYSICAL" in upper:
        return "physical"
    if "SYNTHETIC" in upper or "SWAP" in upper:
        return "synthetic"
    return text


def _parse_hedged_metadata(value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    text = normalize_space(value)
    if not text:
        return None, None
    upper = text.upper()
    if upper in {"NO", "FALSE", "UNHEDGED"} or "UNHEDGED" in upper:
        return 0, None
    target_match = re.search(r"\b(USD|EUR|GBP|JPY|CHF)\b", upper)
    target = target_match.group(1) if target_match else None
    if upper in {"YES", "TRUE"} or "HEDG" in upper or target is not None:
        return 1, target
    return None, None


def _clean_caption_text(caption_node: BeautifulSoup) -> str:
    node = caption_node
    for child in node.find_all(["button", "div"]):
        child.decompose()
    return normalize_space(node.get_text(" ", strip=True))


def parse_ishares_product_page(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    facts: dict[str, str] = {}

    for item in soup.select("div.product-data-item"):
        caption = item.select_one(".caption")
        data = item.select_one(".data")
        if not caption or not data:
            continue
        caption_text = _clean_caption_text(caption)
        data_text = normalize_space(data.get_text(" ", strip=True))
        if caption_text and data_text:
            facts[caption_text] = data_text

    ter: Optional[float] = None
    use_of_income: Optional[str] = None
    ucits_compliant: Optional[int] = None

    for key, value in facts.items():
        key_upper = key.upper()
        if ter is None and ("TOTAL EXPENSE RATIO" in key_upper or key_upper == "TER (%)"):
            ter = parse_percent(value)
        if use_of_income is None and "USE OF INCOME" in key_upper:
            v = value.strip().title()
            if v.startswith("Accumulating"):
                use_of_income = "Accumulating"
            elif v.startswith("Distributing"):
                use_of_income = "Distributing"
            else:
                use_of_income = v or None
        if ucits_compliant is None and ("UCITS COMPLIANT" in key_upper or key_upper == "UCITS"):
            val = value.strip().lower()
            if val in {"yes", "true", "y"}:
                ucits_compliant = 1
            elif val in {"no", "false", "n"}:
                ucits_compliant = 0

    if ter is None:
        # Fallback if markup shifts: look for key phrase with nearby percent value.
        normalized = normalize_space(soup.get_text(" ", strip=True))
        m = re.search(
            r"Total Expense Ratio.{0,120}?([0-9]+(?:[.,][0-9]+)?)\s*%",
            normalized,
            flags=re.IGNORECASE,
        )
        if m:
            ter = parse_percent(m.group(1) + "%")

    benchmark_name = _lookup_fact_value(
        facts,
        exact_keys=("Benchmark Index", "Index", "Reference Index", "Underlying Index"),
    )
    asset_class_hint = _lookup_fact_value(
        facts,
        exact_keys=("Asset Class",),
        contains_keys=("ASSET CLASS",),
    )
    domicile_country = _lookup_fact_value(
        facts,
        exact_keys=("Fund Domicile", "Domicile"),
        contains_keys=("DOMICILE",),
    )
    replication_method = _normalize_replication_method(
        _lookup_fact_value(
            facts,
            exact_keys=("Replication Method",),
            contains_keys=("REPLICATION",),
        )
    )
    hedged_flag, hedged_target = _parse_hedged_metadata(
        _lookup_fact_value(
            facts,
            exact_keys=("Currency Hedged", "Hedged"),
            contains_keys=("HEDGED",),
        )
    )

    return {
        "ter": ter,
        "use_of_income": use_of_income,
        "ucits_compliant": ucits_compliant,
        "benchmark_name": benchmark_name,
        "asset_class_hint": asset_class_hint,
        "domicile_country": domicile_country,
        "replication_method": replication_method,
        "hedged_flag": hedged_flag,
        "hedged_target": hedged_target,
        "facts": facts,
    }


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
            ISHARES_SOURCE,
            source_url,
            ter,
            use_of_income,
            ucits_compliant,
            quality_flag,
            json.dumps(raw_json, ensure_ascii=True),
        ),
    )


def insert_cost_snapshot_from_ter(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    ter: float,
    source_url: str,
    use_of_income: Optional[str],
    ucits_compliant: Optional[int],
    profile_metadata: Optional[dict[str, object]] = None,
) -> None:
    raw_json = {
        "source": ISHARES_SOURCE,
        "source_url": source_url,
        "parser_version": PARSER_VERSION,
        "use_of_income": use_of_income,
        "ucits_compliant": ucits_compliant,
    }
    if profile_metadata:
        raw_json["profile_metadata"] = {
            key: value for key, value in profile_metadata.items() if value is not None
        }
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
        ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, 'issuer_page_ok', ?)
        """,
        (instrument_id, asof_date, ter, json.dumps(raw_json, ensure_ascii=True)),
    )


def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def maybe_update_product_profile(
    conn: sqlite3.Connection, instrument_id: int, use_of_income: Optional[str]
) -> bool:
    if not use_of_income:
        return False
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='product_profile'"
    ).fetchone()
    if not row:
        return False
    if not table_has_column(conn, "product_profile", "distribution_policy"):
        return False
    if not table_has_column(conn, "product_profile", "instrument_id"):
        return False
    if table_has_column(conn, "product_profile", "updated_at"):
        conn.execute(
            """
            INSERT INTO product_profile(instrument_id, distribution_policy, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                distribution_policy = excluded.distribution_policy,
                updated_at = excluded.updated_at
            """,
            (instrument_id, use_of_income, now_utc_iso()),
        )
    else:
        conn.execute(
            """
            INSERT INTO product_profile(instrument_id, distribution_policy)
            VALUES (?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                distribution_policy = excluded.distribution_policy
            """,
            (instrument_id, use_of_income),
        )
    return True


def print_kpis(
    attempted: int,
    mapped: int,
    fetched: int,
    parsed: int,
    filled: int,
) -> None:
    def pct(n: int, d: int) -> float:
        return (100.0 * n / d) if d else 0.0

    print("\n=== Stage 2.4 iShares KPIs ===")
    print(f"attempted instruments: {attempted}")
    print(f"url_mapped%: {pct(mapped, attempted):.2f}% ({mapped}/{attempted})")
    print(f"page_fetched%: {pct(fetched, attempted):.2f}% ({fetched}/{attempted})")
    print(f"ter_parsed%: {pct(parsed, attempted):.2f}% ({parsed}/{attempted})")
    print(f"ongoing_charges_filled%: {pct(filled, attempted):.2f}% ({filled}/{attempted})")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    client = HttpClient(rate_limit=args.rate_limit, timeout=args.timeout, max_retries=args.max_retries)
    asof_date = dt.date.today().isoformat()

    attempted = 0
    mapped = 0
    fetched = 0
    parsed = 0
    filled = 0
    sample_rows: list[tuple[str, str, str, str, str]] = []
    profile_updates = 0

    try:
        conn.execute("BEGIN")
        ensure_tables_and_view(conn)

        targets = load_targets(conn, args.limit, args.venue)
        attempted = len(targets)
        log(f"Target iShares subset loaded: {attempted} rows (venue={args.venue}, limit={args.limit})")
        if attempted == 0:
            conn.commit()
            print_kpis(0, 0, 0, 0, 0)
            print("\nNo instruments eligible for Stage 2.4 (already fee-complete or not iShares subset).")
            return 0

        for idx, row in enumerate(targets, start=1):
            instrument_id = int(row["instrument_id"])
            isin = str(row["isin"])
            ticker = str(row["ticker"] or "")
            debug: dict[str, object] = {
                "parser_version": PARSER_VERSION,
                "instrument_id": instrument_id,
                "isin": isin,
            }

            product_url = find_existing_ishares_product_url(conn, instrument_id)
            if product_url:
                debug["url_source"] = "existing_map_or_logs"

            if not product_url and not args.reuse_only:
                candidates, discovery_debug = discover_candidates_for_instrument(
                    client,
                    isin=isin,
                    ticker=ticker,
                    instrument_name=str(row["instrument_name"] or ""),
                )
                debug["discovery"] = discovery_debug
                debug["candidate_count"] = len(candidates)
                for candidate in candidates:
                    ok, accepted_url, html, verify_debug = verify_candidate_for_isin(client, candidate, isin)
                    checks = debug.setdefault("candidate_checks", [])
                    if isinstance(checks, list):
                        checks.append(verify_debug)
                    if ok and accepted_url:
                        product_url = accepted_url
                        if html:
                            debug["verified_html_prefetch"] = True
                            debug["verified_html"] = html
                        debug["url_source"] = "isin_constrained_lookup"
                        break

            if not product_url:
                insert_issuer_metadata_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    source_url=None,
                    ter=None,
                    use_of_income=None,
                    ucits_compliant=None,
                    quality_flag="no_url",
                    raw_json=debug,
                )
                if idx % 25 == 0:
                    log(f"Processed {idx}/{attempted}: no_url")
                continue

            mapped += 1
            upsert_instrument_url_map(conn, instrument_id, product_url)

            html = debug.get("verified_html") if isinstance(debug.get("verified_html"), str) else None
            fetch_result: Optional[HttpResult] = None
            if not html:
                fetch_result = client.get(add_site_entry_params(product_url))
                debug["page_fetch_status"] = fetch_result.status_code
                debug["page_fetch_content_type"] = fetch_result.content_type
                debug["page_fetch_error"] = fetch_result.error
                if not fetch_result.ok or not fetch_result.text:
                    insert_issuer_metadata_snapshot(
                        conn,
                        instrument_id=instrument_id,
                        asof_date=asof_date,
                        source_url=product_url,
                        ter=None,
                        use_of_income=None,
                        ucits_compliant=None,
                        quality_flag="fail",
                        raw_json=debug,
                    )
                    if idx % 25 == 0:
                        log(f"Processed {idx}/{attempted}: fetch_fail")
                    continue
                html = fetch_result.text
                product_url = canonical_product_url(fetch_result.final_url or product_url)

            fetched += 1
            parsed_payload = parse_ishares_product_page(html or "")
            ter = parsed_payload["ter"] if isinstance(parsed_payload.get("ter"), (float, int)) else None
            use_of_income = (
                str(parsed_payload["use_of_income"])
                if parsed_payload.get("use_of_income") is not None
                else None
            )
            ucits_compliant = (
                int(parsed_payload["ucits_compliant"])
                if parsed_payload.get("ucits_compliant") is not None
                else None
            )
            debug["parsed"] = {
                "ter": ter,
                "use_of_income": use_of_income,
                "ucits_compliant": ucits_compliant,
                "benchmark_name": parsed_payload.get("benchmark_name"),
                "asset_class_hint": parsed_payload.get("asset_class_hint"),
                "domicile_country": parsed_payload.get("domicile_country"),
                "replication_method": parsed_payload.get("replication_method"),
                "hedged_flag": parsed_payload.get("hedged_flag"),
                "hedged_target": parsed_payload.get("hedged_target"),
                "facts_keys": sorted(list((parsed_payload.get("facts") or {}).keys()))[:30],
            }

            quality = "ok" if ter is not None else "parse_fail"
            if ter is not None:
                parsed += 1
                insert_cost_snapshot_from_ter(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    ter=float(ter),
                    source_url=product_url,
                    use_of_income=use_of_income,
                    ucits_compliant=ucits_compliant,
                    profile_metadata={
                        "benchmark_name": parsed_payload.get("benchmark_name"),
                        "asset_class_hint": parsed_payload.get("asset_class_hint"),
                        "domicile_country": parsed_payload.get("domicile_country"),
                        "replication_method": parsed_payload.get("replication_method"),
                        "hedged_flag": parsed_payload.get("hedged_flag"),
                        "hedged_target": parsed_payload.get("hedged_target"),
                    },
                )
                filled += 1
                if len(sample_rows) < 20:
                    sample_rows.append(
                        (
                            isin,
                            ticker,
                            product_url,
                            f"{float(ter):.4f}",
                            use_of_income or "NULL",
                        )
                    )

            if maybe_update_product_profile(conn, instrument_id, use_of_income):
                profile_updates += 1

            insert_issuer_metadata_snapshot(
                conn,
                instrument_id=instrument_id,
                asof_date=asof_date,
                source_url=product_url,
                ter=float(ter) if ter is not None else None,
                use_of_income=use_of_income,
                ucits_compliant=ucits_compliant,
                quality_flag=quality,
                raw_json=debug,
            )

            if idx % 25 == 0:
                log(
                    f"Processed {idx}/{attempted}: mapped={mapped}, fetched={fetched}, "
                    f"ter_parsed={parsed}, filled={filled}"
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print_kpis(attempted, mapped, fetched, parsed, filled)
    print(f"\nproduct_profile distribution_policy updates: {profile_updates}")
    print("\n=== Sample 20 (ISIN, ticker, mapped_url, TER, use_of_income) ===")
    if not sample_rows:
        print("No successful TER parses.")
    else:
        for isin, ticker, url, ter_text, income in sample_rows:
            print(f"{isin} | {ticker} | {url} | {ter_text} | {income}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
