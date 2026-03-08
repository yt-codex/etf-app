
#!/usr/bin/env python3
"""Stage 2 PRIIPs KID ingest for universe_mvp."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import io
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
try:
    import fitz  # type: ignore
except Exception:
    fitz = None

PARSER_VERSION = "stage2_kid_ingest_v2_4"
DOC_TYPE = "PRIIPS_KID"

BANNED_RESULT_DOMAINS = {
    "justetf.com",
    "morningstar.com",
    "morningstar.co.uk",
    "etfdb.com",
    "wikipedia.org",
    "sec.report",
    "sec.gov",
    "reddit.com",
    "x.com",
    "twitter.com",
}

DOMAIN_HINTS_BY_ISSUER = {
    "BlackRock / iShares": ["ishares.com"],
    "BlackRock iShares": ["ishares.com"],
    "Vanguard": ["vanguard.com"],
    "Invesco": ["invesco.com"],
    "State Street / SPDR": ["ssga.com", "spdrs.com"],
    "State Street SPDR": ["ssga.com", "spdrs.com"],
    "Amundi": ["amundietf.com", "amundi.com"],
    "HSBC": ["assetmanagement.hsbc.com", "hsbc.com"],
    "UBS": ["ubs.com"],
    "Xtrackers / DWS": ["xtrackers.com", "dws.com"],
    "Xtrackers (DWS)": ["xtrackers.com", "dws.com"],
    "WisdomTree": ["wisdomtree.eu", "wisdomtree.com"],
    "VanEck": ["vaneck.com", "vaneck.eu"],
    "Legal & General": ["lgim.com"],
    "JPMorgan": ["am.jpmorgan.com"],
    "PIMCO": ["pimco.com"],
    "Franklin Templeton": ["franklintempleton.com"],
    "First Trust": ["ftglobalportfolios.com", "firsttrust.com"],
    "Fidelity": ["fidelityinternational.com", "fidelity.com"],
    "Global X": ["globalxetfs.com"],
    "BNP Paribas": ["assetmanagement.bnpparibas.com", "easy.bnpparibas.com", "bnpparibas-am.com"],
    "OSSIAM": ["ossiam.com"],
    "Deka": ["deka-etf.de", "deka.de"],
    "EXPAT ASSET MANAGEMENT EAD": ["expat.bg"],
}

DOMAIN_TO_ISSUER = [
    ("ishares.com", "BlackRock / iShares"),
    ("blackrock.com", "BlackRock / iShares"),
    ("vanguard.com", "Vanguard"),
    ("invesco.com", "Invesco"),
    ("ssga.com", "State Street / SPDR"),
    ("spdrs.com", "State Street / SPDR"),
    ("amundietf.com", "Amundi"),
    ("amundi.com", "Amundi"),
    ("assetmanagement.hsbc.com", "HSBC"),
    ("hsbc.com", "HSBC"),
    ("ubs.com", "UBS"),
    ("xtrackers.com", "Xtrackers / DWS"),
    ("dws.com", "Xtrackers / DWS"),
    ("wisdomtree.eu", "WisdomTree"),
    ("wisdomtree.com", "WisdomTree"),
    ("vaneck.com", "VanEck"),
    ("vaneck.eu", "VanEck"),
    ("lgim.com", "Legal & General"),
    ("jpmorgan.com", "JPMorgan"),
    ("pimco.com", "PIMCO"),
    ("franklintempleton.com", "Franklin Templeton"),
    ("firsttrust.com", "First Trust"),
    ("ftglobalportfolios.com", "First Trust"),
    ("fidelity.com", "Fidelity"),
    ("fidelityinternational.com", "Fidelity"),
    ("globalxetfs.com", "Global X"),
    ("assetmanagement.bnpparibas.com", "BNP Paribas"),
    ("easy.bnpparibas.com", "BNP Paribas"),
    ("bnpparibas-am.com", "BNP Paribas"),
    ("ossiam.com", "OSSIAM"),
    ("deka-etf.de", "Deka"),
    ("deka.de", "Deka"),
    ("expat.bg", "EXPAT ASSET MANAGEMENT EAD"),
]

PRIORITY_ISSUERS = {
    "BLACKROCK ISHARES",
    "BLACKROCK / ISHARES",
    "VANGUARD",
    "INVESCO",
    "STATE STREET SPDR",
    "STATE STREET / SPDR",
    "AMUNDI",
    "XTRACKERS (DWS)",
    "XTRACKERS / DWS",
    "HSBC",
    "WISDOMTREE",
    "VANECK",
}

BRAND_TOKEN_RULES = [
    (re.compile(r"\bISHARES\b", flags=re.IGNORECASE), "BlackRock iShares"),
    (re.compile(r"\bVANGUARD\b", flags=re.IGNORECASE), "Vanguard"),
    (re.compile(r"\bSPDR\b", flags=re.IGNORECASE), "State Street SPDR"),
    (re.compile(r"\bINVESCO\b", flags=re.IGNORECASE), "Invesco"),
    (re.compile(r"\bAMUNDI\b", flags=re.IGNORECASE), "Amundi"),
    (re.compile(r"\bXTRACKERS\b", flags=re.IGNORECASE), "Xtrackers (DWS)"),
    (re.compile(r"\bHSBC\b", flags=re.IGNORECASE), "HSBC"),
    (re.compile(r"\bWISDOMTREE\b", flags=re.IGNORECASE), "WisdomTree"),
    (re.compile(r"\bVANECK\b", flags=re.IGNORECASE), "VanEck"),
]

ALLOWLIST_DOMAINS = {
    domain for domain, _ in DOMAIN_TO_ISSUER
} | {
    "fund-docs.vanguard.com",
}

BRAND_TEMPLATES = {
    "vanguard": [
        lambda isin: f"https://fund-docs.vanguard.com/{isin.lower()}_priipskid_en.pdf",
    ],
    "invesco": [
        lambda isin: (
            "https://www.invesco.com/content/dam/invesco/emea/en/product-documents/etf/"
            f"share-class/kid/{isin.upper()}_kid_en.pdf"
        ),
    ],
    "xtrackers": [
        lambda isin: f"https://etf.dws.com/download/PRIIPs%20KID/{isin.upper()}/gb/en",
        lambda isin: f"https://etf.dws.com/download/PRIIPs%20KID/{isin.upper()}/lu/en",
        lambda isin: f"https://etf.dws.com/download/PRIIPs%20KID/{isin.upper()}/de/de",
    ],
    "amundi": [
        lambda isin: f"https://www.amundietf.lu/pdfDocuments/kid-priips/{isin.upper()}/ENG/LUX",
        lambda isin: f"https://www.amundietf.lu/pdfDocuments/kid-priips/{isin.upper()}/ENG/DEU",
        lambda isin: f"https://www.amundietf.lu/pdfDocuments/kid-priips/{isin.upper()}/ENG/ESP",
        lambda isin: f"https://www.amundietf.lu/pdfDocuments/kid-priips/{isin.upper()}/ENG/NLD",
    ],
    "hsbc": [
        lambda isin: f"https://www.assetmanagement.hsbc.ch/api/v1/download/document/{isin.lower()}/ch/en/priips",
    ],
    "wisdomtree": [
        lambda isin: (
            "https://dataspanapi.wisdomtree.com/pdr/documents/PRIIP_KID/UCITS/IE/EN-IE/"
            f"{isin.upper()}/"
        ),
    ],
}

SELF_TEST_URLS = {
    "vanguard": "https://fund-docs.vanguard.com/ie0009591805_priipskid_en.pdf",
    "invesco": "https://www.invesco.com/content/dam/invesco/emea/en/product-documents/etf/share-class/kid/IE00B60SWV01_kid_en.pdf",
    "xtrackers": "https://etf.dws.com/download/PRIIPs%20KID/DE000A2T5DZ1/gb/en",
    "amundi": "https://www.amundietf.lu/pdfDocuments/kid-priips/IE000K1P4V37/ENG/LUX",
    "hsbc": "https://www.assetmanagement.hsbc.ch/api/v1/download/document/ie00b5sg8z57/ch/en/priips",
    "wisdomtree": "https://dataspanapi.wisdomtree.com/pdr/documents/PRIIP_KID/UCITS/IE/EN-IE/IE0000902GT6/",
}

TEMPLATE_BRANDS = ("vanguard", "invesco", "xtrackers", "amundi", "hsbc", "wisdomtree")

TEMPLATE_BRAND_TO_ISSUER = {
    "vanguard": "Vanguard",
    "invesco": "Invesco",
    "xtrackers": "Xtrackers (DWS)",
    "amundi": "Amundi",
    "hsbc": "HSBC",
    "wisdomtree": "WisdomTree",
}

INVESCO_TEMPLATE_HEADERS = {
    "Accept": "application/pdf,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Encoding": "gzip, deflate, br",
}


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


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_space(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2 PRIIPs KID ingest")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--cache-dir", default="kid_cache", help="Local PDF cache directory")
    parser.add_argument("--batch-size", type=int, default=200, help="Batch size")
    parser.add_argument("--limit", type=int, default=200, help="Optional max instruments to process")
    parser.add_argument("--rate-limit", type=float, default=0.8, help="HTTP delay in seconds")
    parser.add_argument("--max-retries", type=int, default=2, help="Retry count for HTTP requests")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument(
        "--mode",
        choices=["template", "search"],
        default="template",
        help="Discovery mode",
    )
    parser.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="XLON",
        help="Universe venue filter",
    )
    parser.add_argument(
        "--priority-mode",
        choices=["on", "off"],
        default="on",
        help="Priority ordering mode for universe selection",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Reprocess instruments even if latest snapshot quality is ok/partial",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run connectivity test for template domains and exit",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Re-parse cached PDFs only (no network calls)",
    )
    parser.add_argument(
        "--issuer",
        action="append",
        default=[],
        help="Optional issuer filter; repeat or pass comma-separated values",
    )
    parser.add_argument(
        "--selection-scope",
        choices=["universe", "all-etfs"],
        default="universe",
        help="Choose between universe_mvp or all active ETF primary listings",
    )
    return parser.parse_args(argv)


def parse_issuer_filters(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        for piece in str(value or "").split(","):
            token = normalize_space(piece).upper()
            if token:
                out.append(token)
    deduped: list[str] = []
    seen: set[str] = set()
    for token in out:
        if token not in seen:
            deduped.append(token)
            seen.add(token)
    return deduped


class HttpClient:
    def __init__(self, rate_limit: float, max_retries: int, timeout: int) -> None:
        self.rate_limit = max(0.0, rate_limit)
        self.max_retries = max(0, max_retries)
        self.timeout = timeout
        self.last_request_at = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def _throttle(self) -> None:
        elapsed = time.time() - self.last_request_at
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

    def get(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        allow_redirects: bool = True,
        stream: bool = False,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        attempt = 0
        last_exc: Optional[Exception] = None
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=request_timeout,
                    allow_redirects=allow_redirects,
                    stream=stream,
                )
                self.last_request_at = time.time()
                if response.status_code in {429, 500, 502, 503, 504} and attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                return response
            except requests.RequestException as exc:
                last_exc = exc
                if attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("HTTP request failed unexpectedly")

    def head(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        allow_redirects: bool = True,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        attempt = 0
        last_exc: Optional[Exception] = None
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                response = self.session.head(
                    url,
                    params=params,
                    headers=headers,
                    timeout=request_timeout,
                    allow_redirects=allow_redirects,
                )
                self.last_request_at = time.time()
                if response.status_code in {429, 500, 502, 503, 504} and attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                return response
            except requests.RequestException as exc:
                last_exc = exc
                if attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("HTTP HEAD request failed unexpectedly")

def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    col_name: str,
    col_type: str,
    default_sql: Optional[str] = None,
) -> bool:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    if col_name in cols:
        return False
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}{default_clause}")
    return True


def apply_migrations(conn: sqlite3.Connection) -> dict[str, object]:
    added_cols = []
    if ensure_column(conn, "instrument", "issuer_id", "INTEGER"):
        added_cols.append("instrument.issuer_id")
    if ensure_column(conn, "instrument", "issuer_source", "TEXT"):
        added_cols.append("instrument.issuer_source")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS document(
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            doc_type TEXT,
            url TEXT,
            retrieved_at TEXT,
            hash_sha256 TEXT,
            effective_date TEXT NULL,
            language TEXT NULL,
            parser_version TEXT,
            UNIQUE(instrument_id, doc_type, url)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cost_snapshot(
            cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            asof_date TEXT,
            ongoing_charges REAL NULL,
            entry_costs REAL NULL,
            exit_costs REAL NULL,
            transaction_costs REAL NULL,
            doc_id INTEGER,
            quality_flag TEXT,
            raw_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kid_candidate_url(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            candidate_url TEXT,
            source_method TEXT,
            rank INTEGER,
            decision TEXT,
            reason TEXT,
            retrieved_at TEXT
        )
        """
    )
    return {"added_columns": added_cols}


def decode_search_result_url(url: str) -> str:
    parsed = urlparse(url)
    if "bing.com" in parsed.netloc and parsed.path.startswith("/ck/"):
        qs = parse_qs(parsed.query)
        token = (qs.get("u") or [""])[0]
        if token.startswith("a1"):
            encoded = token[2:]
            try:
                padded = encoded + "=" * (-len(encoded) % 4)
                return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", errors="ignore")
            except Exception:
                pass
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        qs = parse_qs(parsed.query)
        uddg = qs.get("uddg")
        if uddg:
            return unquote(uddg[0])
    return url


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower().lstrip("www.")


def domain_matches(host: str, expected_domain: str) -> bool:
    host = host.lower().lstrip("www.")
    exp = expected_domain.lower().lstrip("www.")
    return host == exp or host.endswith("." + exp)


def is_allowlisted_domain(host: str) -> bool:
    return any(domain_matches(host, allowed) for allowed in ALLOWLIST_DOMAINS)


def guess_issuer_from_name(instrument_name: Optional[str]) -> Optional[str]:
    if not instrument_name:
        return None
    for pattern, issuer_name in BRAND_TOKEN_RULES:
        if pattern.search(instrument_name):
            return issuer_name
    return None


def detect_template_brand_token(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value).lower()
    if not text:
        return None
    for brand in TEMPLATE_BRANDS:
        if brand in text:
            return brand
    return None


def detect_template_brand(
    issuer_normalized: Optional[str],
    instrument_name: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    # Per Stage 2.2, check issuer-normalized first, then fallback to instrument name tokens.
    if issuer_normalized:
        brand = detect_template_brand_token(issuer_normalized)
        return brand, ("issuer_normalized" if brand else None)
    brand = detect_template_brand_token(instrument_name)
    return brand, ("instrument_name" if brand else None)


def build_template_urls(brand: str, isin: str) -> list[str]:
    builders = BRAND_TEMPLATES.get(brand, [])
    urls: list[str] = []
    for fn in builders:
        try:
            urls.append(fn(isin))
        except Exception:
            continue
    return urls


def is_pdf_probe_success(content_type: str, final_url: str, first_bytes: bytes) -> bool:
    ctype = (content_type or "").lower()
    if "pdf" in ctype:
        return True
    if not ctype and final_url.lower().endswith(".pdf"):
        return True
    if first_bytes.startswith(b"%PDF"):
        return True
    return False


def _probe_template_with_get(
    client: HttpClient,
    candidate_url: str,
    *,
    headers: dict[str, str],
    probe_timeout: int,
    blocked_statuses: set[int],
    blocked_on_timeout: bool = True,
    reason_prefix: str = "",
) -> dict[str, object]:
    try:
        get_resp = client.get(
            candidate_url,
            headers=headers,
            allow_redirects=True,
            stream=True,
            timeout=probe_timeout,
        )
        status_code = int(get_resp.status_code)
        final_url = get_resp.url or candidate_url
        content_type = (get_resp.headers.get("content-type") or "").lower()
        first_bytes = b""
        try:
            for chunk in get_resp.iter_content(chunk_size=1024):
                if chunk:
                    first_bytes = chunk[:1024]
                    break
        finally:
            get_resp.close()
    except requests.Timeout:
        return {
            "accepted": False,
            "reason": "timeout",
            "status_code": None,
            "final_url": candidate_url,
            "content_type": None,
            "probe_method": "GET",
            "blocked": blocked_on_timeout,
        }
    except requests.RequestException as exc:
        return {
            "accepted": False,
            "reason": f"request_error:{exc.__class__.__name__}",
            "status_code": None,
            "final_url": candidate_url,
            "content_type": None,
            "probe_method": "GET",
            "blocked": blocked_on_timeout,
        }

    if status_code != 200:
        reason = f"{reason_prefix}status_{status_code}" if reason_prefix else f"status_{status_code}"
        return {
            "accepted": False,
            "reason": reason,
            "status_code": status_code,
            "final_url": final_url,
            "content_type": content_type,
            "probe_method": "GET",
            "blocked": status_code in blocked_statuses,
        }
    if is_pdf_probe_success(content_type, final_url, first_bytes):
        return {
            "accepted": True,
            "reason": "status_200_pdf",
            "status_code": status_code,
            "final_url": final_url,
            "content_type": content_type,
            "probe_method": "GET",
            "blocked": False,
        }
    return {
        "accepted": False,
        "reason": "not_pdf",
        "status_code": status_code,
        "final_url": final_url,
        "content_type": content_type,
        "probe_method": "GET",
        "blocked": False,
    }


def probe_template_candidate(
    client: HttpClient,
    candidate_url: str,
    *,
    brand: Optional[str] = None,
    probe_timeout: int = 10,
) -> dict[str, object]:
    if brand == "invesco":
        return _probe_template_with_get(
            client,
            candidate_url,
            headers=INVESCO_TEMPLATE_HEADERS,
            probe_timeout=probe_timeout,
            blocked_statuses={403, 404},
            blocked_on_timeout=True,
            reason_prefix="",
        )

    headers = {"Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8"}
    try:
        head = client.head(candidate_url, headers=headers, allow_redirects=True, timeout=probe_timeout)
        status_code = int(head.status_code)
        final_url = head.url or candidate_url
        content_type = (head.headers.get("content-type") or "").lower()
    except requests.Timeout:
        return {
            "accepted": False,
            "reason": "timeout",
            "status_code": None,
            "final_url": candidate_url,
            "content_type": None,
            "probe_method": "HEAD",
            "blocked": True,
        }
    except requests.RequestException as exc:
        return {
            "accepted": False,
            "reason": f"request_error:{exc.__class__.__name__}",
            "status_code": None,
            "final_url": candidate_url,
            "content_type": None,
            "probe_method": "HEAD",
            "blocked": True,
        }

    if status_code in {403, 404, 405}:
        get_probe = _probe_template_with_get(
            client,
            candidate_url,
            headers=headers,
            probe_timeout=probe_timeout,
            blocked_statuses={403, 404, 405},
            blocked_on_timeout=True,
            reason_prefix="",
        )
        if get_probe["accepted"]:
            return get_probe
        if status_code == 405:
            return get_probe

    if status_code != 200:
        return {
            "accepted": False,
            "reason": f"status_{status_code}",
            "status_code": status_code,
            "final_url": final_url,
            "content_type": content_type,
            "probe_method": "HEAD",
            "blocked": True,
        }
    if is_pdf_probe_success(content_type, final_url, b""):
        return {
            "accepted": True,
            "reason": "status_200_pdf",
            "status_code": status_code,
            "final_url": final_url,
            "content_type": content_type,
            "probe_method": "HEAD",
            "blocked": False,
        }
    return {
        "accepted": False,
        "reason": "not_pdf",
        "status_code": status_code,
        "final_url": final_url,
        "content_type": content_type,
        "probe_method": "HEAD",
        "blocked": True,
    }


def run_template_self_test(client: HttpClient) -> bool:
    print("=== Template Mode Self-Test ===")
    failed = False
    for brand in TEMPLATE_BRANDS:
        url = SELF_TEST_URLS[brand]
        probe = probe_template_candidate(client, url, brand=brand, probe_timeout=10)
        status = probe["status_code"] if probe["status_code"] is not None else "NA"
        result = "ok" if probe["accepted"] else "fail"
        print(
            f"{brand}: {result} | status={status} | reason={probe['reason']} "
            f"| method={probe['probe_method']} | blocked={probe.get('blocked')} | url={url}"
        )
        if brand == "invesco":
            if probe.get("blocked"):
                failed = True
        elif not probe["accepted"]:
            failed = True
    if failed:
        print("network/domain blocked")
        return False
    return True


def is_pdf_like(url: str, title: str, snippet: str) -> bool:
    parsed = urlparse(url.lower())
    text = f"{title} {snippet} {url}".lower()
    return parsed.path.endswith(".pdf") or ".pdf" in parsed.path or ".pdf" in parsed.query or " pdf" in text


def has_kid_keywords(url: str, title: str, snippet: str) -> bool:
    text = f"{title} {snippet} {url}".lower()
    return any(
        key in text
        for key in [
            "priips",
            "priip",
            "kid",
            "key information document",
            "key-information-document",
            "key investor document",
            "kiid",
        ]
    )


def is_credible_result(
    result: dict[str, str],
    *,
    isin: Optional[str] = None,
    require_isin_match: bool = True,
    allowed_domains: Optional[list[str]] = None,
    require_pdf: bool = True,
) -> bool:
    url = result["url"]
    title = result.get("title", "")
    snippet = result.get("snippet", "")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = extract_domain(url)
    if any(domain_matches(host, banned) for banned in BANNED_RESULT_DOMAINS):
        return False
    if allowed_domains and not any(domain_matches(host, d) for d in allowed_domains):
        return False
    if require_pdf and not is_pdf_like(url, title, snippet):
        return False
    if not has_kid_keywords(url, title, snippet):
        return False
    if isin and require_isin_match:
        blob = f"{url} {title} {snippet}".lower()
        if isin.lower() not in blob:
            return False
    return True


def search_bing(client: HttpClient, query: str, max_results: int = 10) -> list[dict[str, str]]:
    response = client.get("https://www.bing.com/search", params={"q": query, "count": str(max_results)})
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for li in soup.select("li.b_algo"):
        a = li.select_one("h2 a[href]")
        if not a:
            continue
        url = decode_search_result_url(a.get("href", "").strip())
        if not url:
            continue
        snippet_node = li.select_one(".b_caption p") or li.select_one("p")
        snippet = normalize_space(snippet_node.get_text(" ", strip=True) if snippet_node else "")
        title = normalize_space(a.get_text(" ", strip=True))
        results.append({"url": url, "title": title, "snippet": snippet, "engine": "bing", "query": query})
    return results


def search_duckduckgo(client: HttpClient, query: str, max_results: int = 10) -> list[dict[str, str]]:
    response = client.get("https://duckduckgo.com/html/", params={"q": query})
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for row in soup.select(".result"):
        a = row.select_one("a.result__a[href]")
        if not a:
            continue
        url = decode_search_result_url(a.get("href", "").strip())
        if not url:
            continue
        snippet_node = row.select_one(".result__snippet")
        snippet = normalize_space(snippet_node.get_text(" ", strip=True) if snippet_node else "")
        title = normalize_space(a.get_text(" ", strip=True))
        results.append({"url": url, "title": title, "snippet": snippet, "engine": "duckduckgo", "query": query})
        if len(results) >= max_results:
            break
    return results


def search_web(client: HttpClient, query: str, max_results: int = 10) -> list[dict[str, str]]:
    seen = set()
    merged: list[dict[str, str]] = []
    try:
        bing_rows = search_bing(client, query, max_results=max_results)
    except Exception:
        bing_rows = []
    for row in bing_rows:
        key = row["url"]
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
        if len(merged) >= max_results:
            return merged

    if len(merged) == 0:
        try:
            ddg_rows = search_duckduckgo(client, query, max_results=max_results)
        except Exception:
            ddg_rows = []
        for row in ddg_rows:
            key = row["url"]
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
            if len(merged) >= max_results:
                return merged
    return merged

def issuer_domains_from_row(issuer_normalized: Optional[str], issuer_domain: Optional[str]) -> list[str]:
    out: list[str] = []
    if issuer_domain:
        out.append(issuer_domain.lower().strip())
    if issuer_normalized:
        out.extend(DOMAIN_HINTS_BY_ISSUER.get(issuer_normalized, []))
    seen = set()
    uniq = []
    for item in out:
        clean = item.lower().strip().lstrip("www.")
        if clean and clean not in seen:
            seen.add(clean)
            uniq.append(clean)
    return uniq


def insert_kid_candidate(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    candidate_url: str,
    source_method: str,
    rank: int,
    decision: str,
    reason: str,
) -> None:
    conn.execute(
        """
        INSERT INTO kid_candidate_url(
            instrument_id, candidate_url, source_method, rank, decision, reason, retrieved_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (instrument_id, candidate_url, source_method, rank, decision, reason, now_utc_iso()),
    )


def pdf_contains_manufacturer(pdf_bytes: bytes) -> bool:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = " ".join((p.extract_text() or "") for p in reader.pages[:4])
        return bool(re.search(r"\b(product\s+manufacturer|manufacturer)\b", text, flags=re.IGNORECASE))
    except Exception:
        return False


def discover_kid_url_by_issuer(
    client: HttpClient,
    isin: str,
    issuer_normalized: Optional[str],
    issuer_domain: Optional[str],
) -> tuple[Optional[str], dict]:
    debug = {"pass": "A", "queries": [], "result": None}
    domains = issuer_domains_from_row(issuer_normalized, issuer_domain)
    if not domains:
        return None, debug

    for domain in domains:
        queries = [
            f"site:{domain} {isin} PRIIPs KID pdf",
            f"site:{domain} {isin} key information document pdf",
        ]
        for query in queries:
            debug["queries"].append(query)
            results = search_web(client, query, max_results=6)
            for result in results:
                if is_credible_result(
                    result,
                    isin=isin,
                    require_isin_match=False,
                    allowed_domains=[domain],
                    require_pdf=True,
                ):
                    debug["result"] = result
                    return result["url"], debug
    return None, debug


def discover_kid_url_by_isin_search(
    conn: sqlite3.Connection,
    client: HttpClient,
    cache_dir: Path,
    instrument_id: int,
    isin: str,
    top_n: int = 8,
) -> tuple[Optional[str], dict]:
    debug = {"pass": "B", "queries": [], "result": None, "accepted_reason": None}
    queries = [
        f"{isin} PRIIPs KID PDF",
        f"{isin} PRIIP KID PDF",
        f"{isin} key information document PDF",
    ]

    candidate_rows: list[dict[str, str]] = []
    seen = set()
    for query in queries:
        debug["queries"].append(query)
        results = search_web(client, query, max_results=top_n)
        for result in results:
            url = result.get("url")
            if not url or url in seen:
                continue
            seen.add(url)
            candidate_rows.append(result)
            if len(candidate_rows) >= top_n:
                break
        if len(candidate_rows) >= top_n:
            break

    accepted_url: Optional[str] = None
    for rank, result in enumerate(candidate_rows, start=1):
        candidate_url = result["url"]
        source_method = f"isin_search:{result.get('engine', 'unknown')}"
        decision = "rejected"
        reason = "not_credible"
        accepted_final_url = candidate_url

        if is_credible_result(result, isin=isin, require_isin_match=True, require_pdf=True):
            host = extract_domain(candidate_url)
            if is_allowlisted_domain(host):
                decision = "accepted"
                reason = "allowlisted_domain"
            else:
                dl = download_pdf(client, candidate_url, cache_dir)
                if dl.success and dl.pdf_bytes:
                    accepted_final_url = dl.final_url or candidate_url
                    if pdf_contains_manufacturer(dl.pdf_bytes):
                        decision = "accepted"
                        reason = "manufacturer_in_pdf"
                    else:
                        reason = "non_allowlisted_no_manufacturer"
                else:
                    reason = "non_allowlisted_download_fail"

        insert_kid_candidate(
            conn,
            instrument_id=instrument_id,
            candidate_url=candidate_url,
            source_method=source_method,
            rank=rank,
            decision=decision,
            reason=reason,
        )

        if decision == "accepted" and accepted_url is None:
            accepted_url = accepted_final_url
            debug["result"] = result
            debug["accepted_reason"] = reason

    return accepted_url, debug


def download_pdf(client: HttpClient, url: str, cache_dir: Path) -> DownloadResult:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = hashlib.sha256(url.encode("utf-8")).hexdigest()
    cache_path = cache_dir / f"{cache_key}.pdf"

    if cache_path.exists():
        pdf_bytes = cache_path.read_bytes()
        return DownloadResult(True, pdf_bytes, url, True, None, cache_path, 200, "application/pdf")

    headers = {"Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8"}
    if "invesco.com/content/dam/invesco" in url.lower():
        headers = dict(INVESCO_TEMPLATE_HEADERS)
    try:
        response = client.get(url, headers=headers, allow_redirects=True, stream=False)
    except Exception as exc:
        return DownloadResult(False, None, None, False, f"request_error: {exc}", cache_path, None, None)

    content_type = (response.headers.get("content-type") or "").lower()
    data = response.content or b""
    final_url = response.url
    if response.status_code != 200:
        return DownloadResult(False, None, final_url, False, f"http_{response.status_code}", cache_path, response.status_code, content_type)

    if not data.startswith(b"%PDF"):
        if "pdf" not in content_type and not final_url.lower().endswith(".pdf"):
            return DownloadResult(False, None, final_url, False, "not_pdf_content", cache_path, response.status_code, content_type)
        marker = data.find(b"%PDF")
        if marker == -1:
            return DownloadResult(False, None, final_url, False, "pdf_signature_missing", cache_path, response.status_code, content_type)
        data = data[marker:]

    cache_path.write_bytes(data)
    return DownloadResult(True, data, final_url, False, None, cache_path, response.status_code, content_type)


def parse_percent_number(token: str) -> Optional[float]:
    if not token:
        return None
    cleaned = token.strip().replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if 0 <= value <= 100 else None


def normalize_decimal_commas(text: str) -> str:
    return re.sub(r"(?<=\d),(?=\d)", ".", text)


def find_percent_near_labels(
    text: str, label_patterns: list[str], window: int = 120
) -> tuple[Optional[float], list[dict[str, object]]]:
    attempts: list[dict[str, object]] = []
    for label in label_patterns:
        regex_str = rf"{label}.{{0,{window}}}?([0-9]{{1,2}}(?:[.,][0-9]{{1,3}})?)\s*%"
        regex = re.compile(regex_str, flags=re.IGNORECASE | re.DOTALL)
        match = regex.search(text)
        if not match:
            attempts.append({"label": label, "regex": regex_str, "matched": None, "parsed": None})
            continue
        parsed = parse_percent_number(match.group(1))
        attempts.append(
            {
                "label": label,
                "regex": regex_str,
                "matched": match.group(0)[:180],
                "parsed": parsed,
            }
        )
        if parsed is not None:
            return parsed, attempts
    return None, attempts


def extract_text_pypdf(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = normalize_space("\n".join((p.extract_text() or "") for p in reader.pages))
        return text, len(reader.pages), None
    except Exception as exc:
        return "", 0, f"pypdf_exception: {exc}"


def extract_text_fitz(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    if fitz is None:
        return "", 0, "fitz_unavailable"
    doc = None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text("text") or "" for page in doc]
        text = normalize_space("\n".join(pages))
        return text, len(pages), None
    except Exception as exc:
        return "", 0, f"fitz_exception: {exc}"
    finally:
        if doc is not None:
            doc.close()


def extract_pdf_text_with_fallback(pdf_bytes: bytes) -> dict[str, object]:
    text, page_count, err = extract_text_pypdf(pdf_bytes)
    errors = [err] if err else []
    keyword_hit = bool(re.search(r"\b(ongoing|charges?|costs?)\b", text, flags=re.IGNORECASE))
    needs_fallback = (len(text) < 800) or (not keyword_hit)
    if not needs_fallback:
        return {
            "text": text,
            "page_count": page_count,
            "extractor": "pypdf",
            "fallback_used": False,
            "errors": errors,
        }

    fitz_text, fitz_pages, fitz_err = extract_text_fitz(pdf_bytes)
    if fitz_err:
        errors.append(fitz_err)
    if fitz_text:
        return {
            "text": fitz_text,
            "page_count": fitz_pages or page_count,
            "extractor": "fitz",
            "fallback_used": True,
            "errors": errors,
        }
    return {
        "text": text,
        "page_count": page_count,
        "extractor": "pypdf",
        "fallback_used": False,
        "errors": errors,
    }


def find_ongoing_charges_windowed(
    text: str, window: int = 600
) -> tuple[Optional[float], list[dict[str, object]], Optional[int]]:
    keyword_regex = re.compile(r"\bongoing\s+(?:charges?|costs?|charge\s+figure)\b", flags=re.IGNORECASE)
    pct_regex = re.compile(r"([0-9]{1,2}(?:[.,][0-9]{1,3})?)\s*%")

    attempts: list[dict[str, object]] = []
    first_direct_plausible: Optional[float] = None
    best_fallback_plausible: Optional[float] = None
    first_keyword_idx: Optional[int] = None

    for m in keyword_regex.finditer(text):
        if first_keyword_idx is None:
            first_keyword_idx = m.start()
        s = max(0, m.start() - window)
        e = min(len(text), m.end() + window)
        window_text = text[s:e]
        pct_matches = list(pct_regex.finditer(window_text))
        parsed_pairs: list[tuple[int, float, str]] = []
        for pm in pct_matches:
            parsed = parse_percent_number(pm.group(1))
            if parsed is None:
                continue
            abs_pos = s + pm.start()
            parsed_pairs.append((abs_pos, parsed, pm.group(1)))

        first_after_keyword: Optional[float] = None
        first_after_keyword_plausible: Optional[float] = None
        for abs_pos, parsed, _ in parsed_pairs:
            if abs_pos >= m.start():
                first_after_keyword = parsed
                if 0 <= parsed <= 3:
                    first_after_keyword_plausible = parsed
                break

        chosen = first_after_keyword if first_after_keyword is not None else (parsed_pairs[0][1] if parsed_pairs else None)
        plausible = [v for _, v, _ in parsed_pairs if 0 <= v <= 3]
        plausible_positive = [v for v in plausible if v > 0]
        preferred_plausible = min(plausible_positive) if plausible_positive else (min(plausible) if plausible else None)
        if first_after_keyword_plausible is not None:
            chosen = first_after_keyword_plausible
        elif preferred_plausible is not None:
            chosen = preferred_plausible
        if chosen is not None and 0 <= chosen <= 3:
            if first_after_keyword_plausible is not None:
                if first_direct_plausible is None:
                    first_direct_plausible = first_after_keyword_plausible
            else:
                candidate = chosen
                if best_fallback_plausible is None or candidate < best_fallback_plausible:
                    best_fallback_plausible = candidate
        attempts.append(
            {
                "keyword": m.group(0),
                "index": m.start(),
                "window_start": s,
                "window_end": e,
                "percent_tokens": [tok for _, _, tok in parsed_pairs[:12]],
                "parsed_values": [val for _, val, _ in parsed_pairs[:12]],
                "plausible_0_3": plausible[:12],
                "first_after_keyword": first_after_keyword,
                "first_after_keyword_plausible": first_after_keyword_plausible,
                "preferred_plausible": preferred_plausible,
                "selected": chosen,
            }
        )

    return (
        first_direct_plausible if first_direct_plausible is not None else best_fallback_plausible,
        attempts,
        first_keyword_idx,
    )


def detect_language_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    segments = [seg.lower() for seg in parsed.path.split("/") if seg]
    for seg in segments:
        if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", seg):
            return seg[:2]
    return None


def detect_effective_date(text: str) -> Optional[str]:
    for pattern in [r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b", r"\b(\d{4}-\d{2}-\d{2})\b"]:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return None


def _normalize_profile_country(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    return text.title()


def extract_profile_metadata_from_text(text: str) -> dict[str, object]:
    normalized = normalize_space(text)
    upper = normalized.upper()
    header_window = normalized[:800]
    objective_window = normalized[:3000]

    benchmark_name: Optional[str] = None
    for pattern in (
        r"reflect the performance, before fees and expenses, of (?:the )?(.+?) \(index\)",
        r"track the performance(?:, before fees and expenses,)? of (?:the )?(.+?)(?:\.|,)",
        r"seek(?:s)? to track(?: as closely as possible)? the performance of (?:the )?(.+?)(?:\.|,)",
        r"reference index(?: is| shall be)?\s*(.+?)(?:\.|,| objective| investment policy| what is this product\?)",
        r"underlying index(?: is| shall be)? (.+?)(?:\.|,)",
        r"benchmark(?: index)?\s*[:\-]\s*(.+?)(?:\.|,| objective| investment policy| what is this product\?)",
    ):
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            benchmark_name = normalize_space(match.group(1))
            break
    if benchmark_name and not re.search(
        r"\b(INDEX|MSCI|FTSE|STOXX|S&P|NASDAQ|RUSSELL|SOLACTIVE|BLOOMBERG|MARKIT|MORNINGSTAR|NIKKEI|TOPIX|ICE|JP MORGAN)\b",
        benchmark_name.upper(),
    ):
        benchmark_name = None

    domicile_country: Optional[str] = None
    domicile_patterns = (
        ("Ireland", (r"\bIRISH BASED UCITS\b", r"\bauthori[sz]ed in Ireland\b", r"\bunder the laws of Ireland\b")),
        (
            "Luxembourg",
            (
                r"\bauthori[sz]ed in Luxembourg\b",
                r"\bunder the laws of Luxembourg\b",
                r"\bregulated by the Commission de Surveillance du Secteur Financier\b",
            ),
        ),
        ("France", (r"\bauthori[sz]ed in France\b", r"\bunder the laws of France\b")),
        ("Germany", (r"\bauthori[sz]ed in Germany\b", r"\bunder the laws of Germany\b")),
        ("United Kingdom", (r"\bauthori[sz]ed in the United Kingdom\b", r"\bunder the laws of the United Kingdom\b")),
        ("Jersey", (r"\bauthori[sz]ed in Jersey\b", r"\bunder the laws of Jersey\b")),
        ("Switzerland", (r"\bauthori[sz]ed in Switzerland\b", r"\bunder the laws of Switzerland\b")),
        ("Netherlands", (r"\bauthori[sz]ed in the Netherlands\b", r"\bunder the laws of the Netherlands\b")),
    )
    for country, patterns in domicile_patterns:
        if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns):
            domicile_country = country
            break

    asset_class_hint: Optional[str] = None
    benchmark_upper = (benchmark_name or "").upper()
    if re.search(r"\b(COMMODITY|GOLD|SILVER|BULLION)\b", benchmark_upper):
        asset_class_hint = "Commodity"
    elif re.search(r"\b(BOND|TREASURY|GILT|FIXED INCOME|CREDIT|AGGREGATE)\b", benchmark_upper):
        asset_class_hint = "Bond"
    elif re.search(r"\b(MSCI|FTSE|STOXX|S&P|NASDAQ|RUSSELL|NIKKEI|TOPIX)\b", benchmark_upper):
        asset_class_hint = "Equity"
    elif re.search(r"\b(EQUITY|SMALL CAP|MID CAP|LARGE CAP|MSCI|FTSE|STOXX|S&P|NASDAQ|RUSSELL|NIKKEI|TOPIX)\b", header_window, flags=re.IGNORECASE):
        asset_class_hint = "Equity"
    elif re.search(r"\b(BOND|TREASURY|GILT|FLOATING RATE|FIXED INCOME|CREDIT|AGGREGATE)\b", header_window, flags=re.IGNORECASE):
        asset_class_hint = "Bond"
    elif re.search(r"\b(COMMODITY|GOLD|SILVER|BULLION)\b", objective_window, flags=re.IGNORECASE):
        asset_class_hint = "Commodity"
    elif re.search(r"\b(MONEY MARKET|CASH EQUIVALENT)\b", objective_window, flags=re.IGNORECASE):
        asset_class_hint = "Cash"
    elif re.search(r"\b(SHARES?|EQUITY|STOCKS?|LISTED COMPANIES)\b", objective_window, flags=re.IGNORECASE):
        asset_class_hint = "Equity"
    elif re.search(r"\b(BONDS?|TREASURY|GILT|FIXED INCOME|CREDIT)\b", objective_window, flags=re.IGNORECASE):
        asset_class_hint = "Bond"
    elif re.search(r"\b(MULTI[- ]ASSET|BALANCED|PORTFOLIO)\b", header_window, flags=re.IGNORECASE):
        asset_class_hint = "Multi"

    replication_method: Optional[str] = None
    if re.search(r"\b(SWAP|SYNTHETIC)\b", upper):
        replication_method = "synthetic"
    elif re.search(
        r"attempt to replicate the index.*?by buying all or a substantial number of the securities",
        normalized,
        flags=re.IGNORECASE,
    ) or re.search(
        r"\b(PHYSICAL REPLICATION|DIRECT REPLICATION)\b",
        upper,
    ) or re.search(
        r"\b(by holding|holds?|invests? directly in)\b.{0,120}\b(securities|constituents|underlying assets?)\b",
        normalized,
        flags=re.IGNORECASE,
    ):
        replication_method = "physical"

    hedged_flag: Optional[int] = None
    hedged_target: Optional[str] = None
    if "UNHEDGED" in upper:
        hedged_flag = 0
    else:
        hedge_match = re.search(
            r"\b(USD|EUR|GBP|JPY|CHF)\s+(?:CURRENCY\s+)?HEDGED\b|\bHEDGED\b.*?\b(USD|EUR|GBP|JPY|CHF)\b",
            upper,
        )
        if hedge_match:
            hedged_flag = 1
            hedged_target = hedge_match.group(1) or hedge_match.group(2)

    return {
        "benchmark_name": benchmark_name,
        "asset_class_hint": asset_class_hint,
        "domicile_country": domicile_country,
        "replication_method": replication_method,
        "hedged_flag": hedged_flag,
        "hedged_target": hedged_target,
    }


def parse_ongoing_charges(pdf_bytes: bytes) -> dict[str, object]:
    result = {
        "ongoing_charges": None,
        "entry_costs": None,
        "exit_costs": None,
        "transaction_costs": None,
        "effective_date": None,
        "language": None,
        "snippet": None,
        "regex_attempts": {},
        "text_length": 0,
        "page_count": 0,
        "extractor": None,
        "fallback_used": False,
        "extractor_errors": [],
        "benchmark_name": None,
        "asset_class_hint": None,
        "domicile_country": None,
        "replication_method": None,
        "hedged_flag": None,
        "hedged_target": None,
        "error": None,
    }
    try:
        extraction = extract_pdf_text_with_fallback(pdf_bytes)
        text = normalize_space(normalize_decimal_commas(str(extraction["text"])))
        result["text_length"] = len(text)
        result["page_count"] = int(extraction["page_count"] or 0)
        result["extractor"] = extraction["extractor"]
        result["fallback_used"] = bool(extraction["fallback_used"])
        result["extractor_errors"] = extraction["errors"]
        if not text:
            result["error"] = "empty_text"
            return result

        (
            result["ongoing_charges"],
            result["regex_attempts"]["ongoing"],
            keyword_idx,
        ) = find_ongoing_charges_windowed(text, window=600)

        result["entry_costs"], result["regex_attempts"]["entry"] = find_percent_near_labels(
            text, [r"entry\s+costs?", r"one-?off\s+entry\s+costs?"], window=200
        )
        result["exit_costs"], result["regex_attempts"]["exit"] = find_percent_near_labels(
            text, [r"exit\s+costs?", r"one-?off\s+exit\s+costs?"], window=200
        )
        result["transaction_costs"], result["regex_attempts"]["transaction"] = find_percent_near_labels(
            text, [r"transaction\s+costs?", r"portfolio\s+transaction\s+costs?"], window=240
        )
        result["effective_date"] = detect_effective_date(text)
        profile_metadata = extract_profile_metadata_from_text(text)
        for key, value in profile_metadata.items():
            result[key] = value

        if keyword_idx is not None:
            result["snippet"] = text[max(0, keyword_idx - 500) : min(len(text), keyword_idx + 500)]
        elif text:
            result["snippet"] = text[:1000]
        return result
    except Exception as exc:
        result["error"] = f"parse_exception: {exc}"
        return result

def find_existing_document(conn: sqlite3.Connection, instrument_id: int, url: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT document_id, hash_sha256, url
        FROM document
        WHERE instrument_id = ? AND doc_type = ? AND url = ?
        ORDER BY document_id DESC
        LIMIT 1
        """,
        (instrument_id, DOC_TYPE, url),
    ).fetchone()


def insert_document_version(
    conn: sqlite3.Connection,
    instrument_id: int,
    url: str,
    pdf_bytes: bytes,
    effective_date: Optional[str],
    language: Optional[str],
) -> tuple[int, str, str]:
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    existing = find_existing_document(conn, instrument_id, url)
    stored_url = url

    if existing and existing["hash_sha256"] == sha:
        return int(existing["document_id"]), stored_url, sha

    if existing and existing["hash_sha256"] != sha:
        stored_url = f"{url}#sha256={sha[:12]}"

    conn.execute(
        """
        INSERT INTO document(
            instrument_id, doc_type, url, retrieved_at, hash_sha256,
            effective_date, language, parser_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instrument_id,
            DOC_TYPE,
            stored_url,
            now_utc_iso(),
            sha,
            effective_date,
            language,
            PARSER_VERSION,
        ),
    )
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return int(doc_id), stored_url, sha


def upsert_issuer_by_normalized(conn: sqlite3.Connection, normalized_name: str, domain: Optional[str]) -> int:
    row = conn.execute("SELECT issuer_id FROM issuer WHERE normalized_name = ? ORDER BY issuer_id LIMIT 1", (normalized_name,)).fetchone()
    if row:
        issuer_id = int(row["issuer_id"])
        if domain:
            conn.execute("UPDATE issuer SET domain = COALESCE(domain, ?) WHERE issuer_id = ?", (domain, issuer_id))
        return issuer_id

    row = conn.execute("SELECT issuer_id FROM issuer WHERE issuer_name = ? ORDER BY issuer_id LIMIT 1", (normalized_name,)).fetchone()
    if row:
        issuer_id = int(row["issuer_id"])
        conn.execute(
            "UPDATE issuer SET normalized_name = COALESCE(normalized_name, ?), domain = COALESCE(domain, ?) WHERE issuer_id = ?",
            (normalized_name, domain, issuer_id),
        )
        return issuer_id

    cur = conn.execute(
        "INSERT INTO issuer(issuer_name, website, created_at, normalized_name, domain) VALUES (?, NULL, ?, ?, ?)",
        (normalized_name, now_utc_iso(), normalized_name, domain),
    )
    return int(cur.lastrowid)


def map_domain_to_issuer(domain: str) -> Optional[str]:
    host = domain.lower().lstrip("www.")
    for known, issuer_name in DOMAIN_TO_ISSUER:
        if host == known or host.endswith("." + known):
            return issuer_name
    return None


def backfill_issuer_from_domain_or_pdf(
    conn: sqlite3.Connection,
    instrument_id: int,
    current_issuer_id: Optional[int],
    url: str,
    issuer_name_hint: Optional[str] = None,
    source: str = "kid_domain",
) -> Optional[dict[str, object]]:
    if current_issuer_id is not None:
        return None
    domain = extract_domain(url)
    issuer_name = issuer_name_hint or map_domain_to_issuer(domain)
    if not issuer_name:
        return None

    issuer_id = upsert_issuer_by_normalized(conn, issuer_name, domain)
    updated = conn.execute(
        "UPDATE instrument SET issuer_id = ?, issuer_source = ? WHERE instrument_id = ? AND issuer_id IS NULL",
        (issuer_id, source, instrument_id),
    ).rowcount
    if updated:
        conn.execute("UPDATE universe_mvp SET issuer_normalized = ? WHERE instrument_id = ?", (issuer_name, str(instrument_id)))
    return {"issuer_id": issuer_id, "issuer_name": issuer_name, "updated": updated, "source": source}


def insert_cost_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    ongoing_charges: Optional[float],
    entry_costs: Optional[float],
    exit_costs: Optional[float],
    transaction_costs: Optional[float],
    doc_id: Optional[int],
    quality_flag: str,
    raw_json: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO cost_snapshot(
            instrument_id, asof_date, ongoing_charges, entry_costs, exit_costs,
            transaction_costs, doc_id, quality_flag, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instrument_id,
            asof_date,
            ongoing_charges,
            entry_costs,
            exit_costs,
            transaction_costs,
            doc_id,
            quality_flag,
            json.dumps(raw_json, ensure_ascii=True),
        ),
    )


def latest_quality_for_instrument(conn: sqlite3.Connection, instrument_id: int) -> Optional[str]:
    row = conn.execute(
        "SELECT quality_flag FROM cost_snapshot WHERE instrument_id = ? ORDER BY cost_id DESC LIMIT 1",
        (instrument_id,),
    ).fetchone()
    return row["quality_flag"] if row else None


def cache_paths_for_url(cache_dir: Path, url: str) -> list[Path]:
    url_variants = [url]
    base = url.split("#", 1)[0]
    if base != url:
        url_variants.append(base)
    out: list[Path] = []
    seen = set()
    for item in url_variants:
        key = hashlib.sha256(item.encode("utf-8")).hexdigest()
        path = cache_dir / f"{key}.pdf"
        if str(path) in seen:
            continue
        seen.add(str(path))
        out.append(path)
    return out


def load_cached_pdf_for_url(cache_dir: Path, url: str) -> tuple[Optional[bytes], Optional[Path]]:
    for path in cache_paths_for_url(cache_dir, url):
        if path.exists():
            return path.read_bytes(), path
    return None, None


def latest_document_for_instrument(conn: sqlite3.Connection, instrument_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT document_id, url, effective_date, language, hash_sha256, retrieved_at
        FROM document
        WHERE instrument_id = ? AND doc_type = ?
        ORDER BY document_id DESC
        LIMIT 1
        """,
        (instrument_id, DOC_TYPE),
    ).fetchone()


def process_instrument_parse_only(
    conn: sqlite3.Connection,
    cache_dir: Path,
    row: sqlite3.Row,
    asof_date: str,
) -> dict[str, object]:
    instrument_id = int(row["instrument_id"])
    debug: dict[str, object] = {
        "mode": "parse_only",
        "parser_version": PARSER_VERSION,
        "isin": row["isin"],
        "instrument_name": row["instrument_name"],
    }
    doc = latest_document_for_instrument(conn, instrument_id)
    if not doc:
        debug["parse_only_reason"] = "no_document"
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=None,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=None,
            quality_flag="no_url",
            raw_json=debug,
        )
        return {
            "url_found": False,
            "downloaded": False,
            "parsed": False,
            "outlier": False,
            "charges_populated": False,
        }

    doc_url = str(doc["url"])
    pdf_bytes, cache_path = load_cached_pdf_for_url(cache_dir, doc_url)
    debug["document"] = {
        "doc_id": int(doc["document_id"]),
        "url": doc_url,
        "cached_path": str(cache_path) if cache_path else None,
    }
    if not pdf_bytes:
        debug["parse_only_reason"] = "cache_missing"
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=None,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=int(doc["document_id"]),
            quality_flag="download_fail",
            raw_json=debug,
        )
        return {
            "url_found": True,
            "downloaded": False,
            "parsed": False,
            "outlier": False,
            "charges_populated": False,
        }

    parsed = parse_ongoing_charges(pdf_bytes)
    if not parsed.get("language"):
        parsed["language"] = doc["language"] or detect_language_from_url(doc_url)
    if not parsed.get("effective_date"):
        parsed["effective_date"] = doc["effective_date"]
    debug["parse"] = parsed

    ongoing = parsed.get("ongoing_charges")
    entry = parsed.get("entry_costs")
    exit_cost = parsed.get("exit_costs")
    txn = parsed.get("transaction_costs")

    if ongoing is None:
        quality = "parse_fail"
        parsed_ok = False
    elif entry is None or exit_cost is None or txn is None:
        quality = "partial"
        parsed_ok = True
    else:
        quality = "ok"
        parsed_ok = True

    insert_cost_snapshot(
        conn,
        instrument_id=instrument_id,
        asof_date=asof_date,
        ongoing_charges=ongoing,
        entry_costs=entry,
        exit_costs=exit_cost,
        transaction_costs=txn,
        doc_id=int(doc["document_id"]),
        quality_flag=quality,
        raw_json=debug,
    )

    outlier = bool(ongoing is not None and (ongoing < 0 or ongoing > 3))
    return {
        "url_found": True,
        "downloaded": True,
        "parsed": parsed_ok,
        "outlier": outlier,
        "charges_populated": ongoing is not None,
    }


def load_universe_rows(
    conn: sqlite3.Connection,
    limit: Optional[int],
    venue: str,
    priority_mode: bool,
    mode: str,
    issuer_filters: Optional[list[str]] = None,
    selection_scope: str = "universe",
) -> list[sqlite3.Row]:
    if selection_scope == "all-etfs":
        base_sql = """
            SELECT
                i.instrument_id AS instrument_id,
                i.isin,
                i.instrument_name,
                i.issuer_id,
                i.issuer_source,
                iss.normalized_name AS issuer_normalized,
                iss.domain AS issuer_domain,
                l.venue_mic AS primary_venue_mic
            FROM instrument i
            JOIN listing l
              ON l.instrument_id = i.instrument_id
             AND l.primary_flag = 1
             AND COALESCE(l.status, 'active') = 'active'
            LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
            LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id
        """
        issuer_hint_sql = "iss.normalized_name"
        issuer_name_sql = "iss.normalized_name"
        instrument_name_sql = "i.instrument_name"
        isin_sql = "i.isin"
        venue_sql = "l.venue_mic"
        where_clauses = [
            "UPPER(COALESCE(i.instrument_type, '')) = 'ETF'",
            "COALESCE(i.status, 'active') = 'active'",
        ]
    else:
        base_sql = """
            SELECT
                CAST(u.instrument_id AS INTEGER) AS instrument_id,
                u.isin,
                u.instrument_name,
                i.issuer_id,
                i.issuer_source,
                iss.normalized_name AS issuer_normalized,
                iss.domain AS issuer_domain,
                u.primary_venue_mic
            FROM universe_mvp u
            JOIN instrument i ON i.instrument_id = CAST(u.instrument_id AS INTEGER)
            LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
            LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id
        """
        issuer_hint_sql = "u.issuer_normalized"
        issuer_name_sql = "iss.normalized_name"
        instrument_name_sql = "u.instrument_name"
        isin_sql = "u.isin"
        venue_sql = "u.primary_venue_mic"
        where_clauses = ["UPPER(COALESCE(u.instrument_type, '')) = 'ETF'"]
    params: list[object] = []
    if venue != "ALL":
        where_clauses.append(f"{venue_sql} = ?")
        params.append(venue)

    if issuer_filters:
        issuer_terms = [str(term).upper() for term in issuer_filters if str(term).strip()]
        issuer_clauses: list[str] = []
        for term in issuer_terms:
            pattern = f"%{term}%"
            issuer_clauses.append(
                "("
                f"UPPER(COALESCE({issuer_hint_sql}, '')) LIKE ? OR "
                f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE ? OR "
                f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE ?"
                ")"
            )
            params.extend([pattern, pattern, pattern])
        if issuer_clauses:
            where_clauses.append("(" + " OR ".join(issuer_clauses) + ")")

    if mode == "template":
        where_clauses.append(
            "("
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%VANGUARD%' OR "
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%INVESCO%' OR "
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%XTRACKERS%' OR "
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%AMUNDI%' OR "
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%HSBC%' OR "
            f"UPPER(COALESCE({issuer_name_sql}, '')) LIKE '%WISDOMTREE%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%VANGUARD%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%INVESCO%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%XTRACKERS%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%AMUNDI%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%HSBC%' OR "
            f"UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%WISDOMTREE%'"
            ")"
        )

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    if priority_mode:
        issuer_terms = sorted(PRIORITY_ISSUERS)
        issuer_placeholders = ",".join("?" for _ in issuer_terms)
        params.extend(issuer_terms)

        fee_case = "CASE WHEN icc.ongoing_charges IS NULL THEN 0 ELSE 1 END"
        token_case = (
            "CASE "
            f"WHEN UPPER(COALESCE({issuer_hint_sql}, {issuer_name_sql}, '')) IN ("
            + issuer_placeholders
            + ") THEN 0 "
            f"WHEN UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%ISHARES%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%VANGUARD%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%SPDR%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%INVESCO%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%AMUNDI%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%XTRACKERS%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%HSBC%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%WISDOMTREE%' "
            f"OR UPPER(COALESCE({instrument_name_sql}, '')) LIKE '%VANECK%' "
            "THEN 1 ELSE 2 END"
        )
        order_sql = f"ORDER BY {fee_case}, {token_case}, {isin_sql}"
    else:
        order_sql = f"ORDER BY CASE WHEN icc.ongoing_charges IS NULL THEN 0 ELSE 1 END, {isin_sql}"

    sql = f"""
        {base_sql}
        {where_sql}
        {order_sql}
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return conn.execute(sql, tuple(params)).fetchall()


def process_instrument(
    conn: sqlite3.Connection,
    client: HttpClient,
    cache_dir: Path,
    row: sqlite3.Row,
    asof_date: str,
    mode: str,
) -> dict[str, object]:
    instrument_id = int(row["instrument_id"])
    isin = row["isin"]
    issuer_id = row["issuer_id"]
    issuer_normalized = row["issuer_normalized"]
    issuer_domain = row["issuer_domain"]
    instrument_name = row["instrument_name"]
    debug: dict[str, object] = {
        "isin": isin,
        "instrument_name": instrument_name,
        "mode": mode,
        "parser_version": PARSER_VERSION,
    }

    discovered_url = None
    backfill_done = False
    template_brand = None
    template_brand_source = None

    if mode == "template":
        template_brand, template_brand_source = detect_template_brand(issuer_normalized, instrument_name)
        debug["template_brand"] = template_brand
        debug["template_brand_source"] = template_brand_source

        if template_brand:
            candidates = build_template_urls(template_brand, isin)
            debug["template_candidates"] = candidates
            for rank, candidate_url in enumerate(candidates, start=1):
                probe = probe_template_candidate(client, candidate_url, brand=template_brand, probe_timeout=10)
                decision = "accepted" if probe["accepted"] else "rejected"
                insert_kid_candidate(
                    conn,
                    instrument_id=instrument_id,
                    candidate_url=candidate_url,
                    source_method=f"template:{template_brand}",
                    rank=rank,
                    decision=decision,
                    reason=str(probe["reason"]),
                )
                if probe["accepted"] and discovered_url is None:
                    discovered_url = str(probe.get("final_url") or candidate_url)
                    debug["pass_template"] = {
                        "brand": template_brand,
                        "source": template_brand_source,
                        "probe": probe,
                    }
                    break
        else:
            debug["template_skip_reason"] = "no_brand_match"
    else:
        if issuer_id is not None:
            discovered_url, debug["pass_a"] = discover_kid_url_by_issuer(client, isin, issuer_normalized, issuer_domain)
        else:
            token_issuer = guess_issuer_from_name(instrument_name)
            if token_issuer:
                discovered_url, debug["pass_a5"] = discover_kid_url_by_issuer(client, isin, token_issuer, None)
                if discovered_url:
                    backfill = backfill_issuer_from_domain_or_pdf(
                        conn,
                        instrument_id,
                        issuer_id,
                        discovered_url,
                        issuer_name_hint=token_issuer,
                        source="name_token",
                    )
                    if backfill:
                        debug["issuer_backfill"] = backfill
                        backfill_done = True

        if not discovered_url:
            discovered_url, debug["pass_b"] = discover_kid_url_by_isin_search(
                conn=conn,
                client=client,
                cache_dir=cache_dir,
                instrument_id=instrument_id,
                isin=isin,
                top_n=8,
            )

    if not discovered_url:
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=None,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=None,
            quality_flag="no_url",
            raw_json=debug,
        )
        return {
            "url_found": False,
            "downloaded": False,
            "parsed": False,
            "outlier": False,
            "charges_populated": False,
        }

    debug["kid_url"] = discovered_url
    if mode == "template" and issuer_id is None and template_brand:
        issuer_hint = TEMPLATE_BRAND_TO_ISSUER.get(template_brand)
        source = "name_token" if template_brand_source == "instrument_name" else "kid_domain"
        if issuer_hint:
            backfill = backfill_issuer_from_domain_or_pdf(
                conn,
                instrument_id,
                issuer_id,
                discovered_url,
                issuer_name_hint=issuer_hint,
                source=source,
            )
            if backfill:
                debug["issuer_backfill"] = backfill
                backfill_done = True

    if not backfill_done:
        backfill = backfill_issuer_from_domain_or_pdf(
            conn,
            instrument_id,
            issuer_id,
            discovered_url,
            issuer_name_hint=None,
            source="kid_domain",
        )
        if backfill:
            debug["issuer_backfill"] = backfill

    dl = download_pdf(client, discovered_url, cache_dir)
    debug["download"] = {
        "success": dl.success,
        "error": dl.error,
        "final_url": dl.final_url,
        "from_cache": dl.from_cache,
        "status": dl.http_status,
        "content_type": dl.content_type,
        "cache_path": str(dl.cache_path) if dl.cache_path else None,
    }

    if not dl.success or not dl.pdf_bytes:
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=None,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=None,
            quality_flag="download_fail",
            raw_json=debug,
        )
        return {
            "url_found": True,
            "downloaded": False,
            "parsed": False,
            "outlier": False,
            "charges_populated": False,
        }

    parsed = parse_ongoing_charges(dl.pdf_bytes)
    if not parsed.get("language"):
        parsed["language"] = detect_language_from_url(dl.final_url or discovered_url)

    doc_id, stored_url, sha = insert_document_version(
        conn,
        instrument_id,
        dl.final_url or discovered_url,
        dl.pdf_bytes,
        parsed.get("effective_date"),
        parsed.get("language"),
    )
    debug["document"] = {"doc_id": doc_id, "stored_url": stored_url, "sha256": sha}
    debug["parse"] = parsed

    ongoing = parsed.get("ongoing_charges")
    entry = parsed.get("entry_costs")
    exit_cost = parsed.get("exit_costs")
    txn = parsed.get("transaction_costs")

    if ongoing is None:
        quality = "parse_fail"
        parsed_ok = False
    elif entry is None or exit_cost is None or txn is None:
        quality = "partial"
        parsed_ok = True
    else:
        quality = "ok"
        parsed_ok = True

    insert_cost_snapshot(
        conn,
        instrument_id=instrument_id,
        asof_date=asof_date,
        ongoing_charges=ongoing,
        entry_costs=entry,
        exit_costs=exit_cost,
        transaction_costs=txn,
        doc_id=doc_id,
        quality_flag=quality,
        raw_json=debug,
    )

    outlier = bool(ongoing is not None and (ongoing < 0 or ongoing > 3))
    return {
        "url_found": True,
        "downloaded": True,
        "parsed": parsed_ok,
        "outlier": outlier,
        "charges_populated": ongoing is not None,
    }

def compute_universe_coverage(conn: sqlite3.Connection) -> dict[str, int]:
    return conn.execute(
        """
        WITH universe AS (
            SELECT CAST(instrument_id AS INTEGER) AS instrument_id
            FROM universe_mvp
        ),
        latest AS (
            SELECT cs.instrument_id, MAX(cs.cost_id) AS max_cost_id
            FROM cost_snapshot cs
            JOIN universe u ON u.instrument_id = cs.instrument_id
            GROUP BY cs.instrument_id
        )
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN l.max_cost_id IS NOT NULL AND cs.quality_flag <> 'no_url' THEN 1 ELSE 0 END) AS found_url,
            SUM(CASE WHEN l.max_cost_id IS NOT NULL AND cs.quality_flag NOT IN ('no_url','download_fail') THEN 1 ELSE 0 END) AS downloaded,
            SUM(CASE WHEN l.max_cost_id IS NOT NULL AND cs.quality_flag IN ('ok','partial') THEN 1 ELSE 0 END) AS parsed,
            SUM(CASE WHEN l.max_cost_id IS NOT NULL AND cs.ongoing_charges IS NOT NULL THEN 1 ELSE 0 END) AS charges_populated
        FROM universe u
        LEFT JOIN latest l ON l.instrument_id = u.instrument_id
        LEFT JOIN cost_snapshot cs ON cs.cost_id = l.max_cost_id
        """
    ).fetchone()


def compute_subset_charges_populated(conn: sqlite3.Connection, instrument_ids: list[int]) -> int:
    if not instrument_ids:
        return 0
    placeholders = ",".join("?" for _ in instrument_ids)
    row = conn.execute(
        f"""
        WITH latest AS (
            SELECT cs.instrument_id, MAX(cs.cost_id) AS max_cost_id
            FROM cost_snapshot cs
            WHERE cs.instrument_id IN ({placeholders})
            GROUP BY cs.instrument_id
        )
        SELECT
            SUM(CASE WHEN cs.ongoing_charges IS NOT NULL THEN 1 ELSE 0 END) AS charges_populated
        FROM latest l
        JOIN cost_snapshot cs ON cs.cost_id = l.max_cost_id
        """,
        tuple(instrument_ids),
    ).fetchone()
    return int(row["charges_populated"] or 0)


def print_coverage_stats(
    total: int,
    found_url: int,
    downloaded: int,
    parsed: int,
    charges_populated: int,
) -> None:
    def pct(n: int, d: int) -> float:
        return (100.0 * n / d) if d else 0.0

    print("\n=== Coverage (universe_mvp) ===")
    print(f"universe instruments processed: {total}")
    print(f"url_found%: {pct(found_url, total):.2f}% ({found_url}/{total})")
    print(f"downloaded%: {pct(downloaded, total):.2f}% ({downloaded}/{total})")
    print(f"parsed%: {pct(parsed, total):.2f}% ({parsed}/{total})")
    print(
        f"overall_charges_populated%: {pct(charges_populated, total):.2f}% "
        f"({charges_populated}/{total})"
    )


def print_template_mode_kpis(
    attempted: int,
    found_url: int,
    downloaded: int,
    parsed: int,
    charges_populated: int,
) -> None:
    def pct(n: int, d: int) -> float:
        return (100.0 * n / d) if d else 0.0

    print("\n=== Template Mode KPIs ===")
    print(f"attempted instruments: {attempted}")
    print(f"url_found%: {pct(found_url, attempted):.2f}% ({found_url}/{attempted})")
    print(f"downloaded%: {pct(downloaded, attempted):.2f}% ({downloaded}/{attempted})")
    print(f"parsed%: {pct(parsed, attempted):.2f}% ({parsed}/{attempted})")
    print(
        f"overall_charges_populated%: {pct(charges_populated, attempted):.2f}% "
        f"({charges_populated}/{attempted})"
    )


def print_samples(conn: sqlite3.Connection) -> None:
    print("\n=== Sample 30 (ISIN, issuer, KID URL, ongoing_charges) ===")
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT instrument_id, MAX(cost_id) AS max_cost_id
            FROM cost_snapshot
            GROUP BY instrument_id
        )
        SELECT
            i.isin,
            COALESCE(iss.normalized_name, iss.issuer_name, 'NULL') AS issuer_name,
            COALESCE(d.url, 'NULL') AS kid_url,
            cs.ongoing_charges
        FROM latest l
        JOIN cost_snapshot cs ON cs.cost_id = l.max_cost_id
        JOIN instrument i ON i.instrument_id = l.instrument_id
        JOIN universe_mvp u ON CAST(u.instrument_id AS INTEGER) = i.instrument_id
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN document d ON d.document_id = cs.doc_id
        ORDER BY i.isin
        LIMIT 30
        """
    ).fetchall()
    for row in rows:
        ongoing_text = "NULL" if row["ongoing_charges"] is None else f"{row['ongoing_charges']:.4f}"
        print(f"{row['isin']} | {row['issuer_name']} | {row['kid_url']} | {ongoing_text}")


def print_template_success_samples(conn: sqlite3.Connection, instrument_ids: list[int], limit: int = 20) -> None:
    print(f"\n=== Template Success Samples ({limit}) ===")
    if not instrument_ids:
        print("No instruments selected.")
        return
    placeholders = ",".join("?" for _ in instrument_ids)
    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT cs.instrument_id, MAX(cs.cost_id) AS max_cost_id
            FROM cost_snapshot cs
            WHERE cs.instrument_id IN ({placeholders})
            GROUP BY cs.instrument_id
        )
        SELECT
            i.isin,
            i.instrument_name,
            COALESCE(iss.normalized_name, '') AS issuer_normalized,
            COALESCE(d.url, 'NULL') AS kid_url,
            cs.ongoing_charges
        FROM latest l
        JOIN cost_snapshot cs ON cs.cost_id = l.max_cost_id
        JOIN instrument i ON i.instrument_id = l.instrument_id
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN document d ON d.document_id = cs.doc_id
        WHERE cs.ongoing_charges IS NOT NULL
        ORDER BY i.isin
        LIMIT ?
        """,
        tuple(instrument_ids) + (limit,),
    ).fetchall()
    for row in rows:
        brand, _ = detect_template_brand(row["issuer_normalized"], row["instrument_name"])
        brand_text = brand or "unknown"
        print(f"{row['isin']} | {brand_text} | {row['kid_url']} | {float(row['ongoing_charges']):.4f}")
    if not rows:
        print("No successful parsed rows in current selection.")


def print_outlier_stats(conn: sqlite3.Connection) -> None:
    total_parsed = conn.execute(
        """
        WITH ranked AS (
            SELECT
                cs.instrument_id,
                cs.ongoing_charges,
                ROW_NUMBER() OVER (
                    PARTITION BY cs.instrument_id
                    ORDER BY cs.asof_date DESC, cs.cost_id DESC
                ) AS rn
            FROM cost_snapshot cs
            WHERE cs.instrument_id IN (SELECT CAST(instrument_id AS INTEGER) FROM universe_mvp)
        )
        SELECT COUNT(*)
        FROM ranked
        WHERE rn = 1
          AND ongoing_charges IS NOT NULL
        """
    ).fetchone()[0]
    outliers = conn.execute(
        """
        WITH ranked AS (
            SELECT
                cs.instrument_id,
                cs.ongoing_charges,
                ROW_NUMBER() OVER (
                    PARTITION BY cs.instrument_id
                    ORDER BY cs.asof_date DESC, cs.cost_id DESC
                ) AS rn
            FROM cost_snapshot cs
            WHERE cs.instrument_id IN (SELECT CAST(instrument_id AS INTEGER) FROM universe_mvp)
        )
        SELECT COUNT(*)
        FROM ranked
        WHERE rn = 1
          AND ongoing_charges IS NOT NULL
          AND (ongoing_charges < 0 OR ongoing_charges > 3)
        """
    ).fetchone()[0]
    print("\n=== Sanity ===")
    print(f"latest parsed ongoing_charges rows: {total_parsed}")
    print(f"latest outliers outside [0, 3]%: {outliers}")
    if outliers:
        print("sample latest outliers:")
        for row in conn.execute(
            """
            WITH ranked AS (
                SELECT
                    cs.instrument_id,
                    cs.ongoing_charges,
                    cs.doc_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY cs.instrument_id
                        ORDER BY cs.asof_date DESC, cs.cost_id DESC
                    ) AS rn
                FROM cost_snapshot cs
                WHERE cs.instrument_id IN (SELECT CAST(instrument_id AS INTEGER) FROM universe_mvp)
            )
            SELECT i.isin, r.ongoing_charges, COALESCE(d.url, 'NULL') AS kid_url
            FROM ranked r
            JOIN instrument i ON i.instrument_id = r.instrument_id
            LEFT JOIN document d ON d.document_id = r.doc_id
            WHERE r.rn = 1
              AND r.ongoing_charges IS NOT NULL
              AND (r.ongoing_charges < 0 OR r.ongoing_charges > 3)
            ORDER BY r.ongoing_charges DESC
            LIMIT 20
            """
        ):
            print(f"  {row['isin']} | {row['ongoing_charges']} | {row['kid_url']}")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    client = HttpClient(rate_limit=args.rate_limit, max_retries=args.max_retries, timeout=args.timeout)

    if args.self_test:
        return 0 if run_template_self_test(client) else 1

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    cache_dir = Path(args.cache_dir)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    asof_date = dt.date.today().isoformat()

    try:
        conn.execute("BEGIN")
        migration_stats = apply_migrations(conn)
        log(
            "Migration complete; added columns: "
            + (", ".join(migration_stats["added_columns"]) if migration_stats["added_columns"] else "none")
        )

        priority_mode = args.priority_mode == "on"
        issuer_filters = parse_issuer_filters(args.issuer)
        log(
            f"Selection mode: mode={args.mode}, venue={args.venue}, "
            f"priority_mode={'on' if priority_mode else 'off'}, parse_only={args.parse_only}, "
            f"issuer_filters={issuer_filters if issuer_filters else 'none'}, "
            f"selection_scope={args.selection_scope}"
        )
        rows = load_universe_rows(
            conn,
            args.limit,
            args.venue,
            priority_mode,
            args.mode,
            issuer_filters,
            selection_scope=args.selection_scope,
        )
        if not rows:
            log("No selected rows to process.")
            conn.commit()
            return 0

        if args.parse_only:
            log("Parse-only mode enabled: using cached PDFs only, no network requests.")
        elif not args.refresh_existing:
            filtered = []
            for row in rows:
                q = latest_quality_for_instrument(conn, int(row["instrument_id"]))
                if q in {"ok", "partial"}:
                    continue
                filtered.append(row)
            rows = filtered
            log(f"Filtered to {len(rows)} instruments needing refresh (refresh_existing=False)")

        total = len(rows)
        selected_instrument_ids = [int(r["instrument_id"]) for r in rows]
        if total == 0:
            log("All selected instruments already have successful snapshots.")
            conn.commit()
            return 0

        found_url = 0
        downloaded = 0
        parsed = 0
        charges_populated_run = 0

        for batch_start in range(0, total, args.batch_size):
            batch = rows[batch_start : batch_start + args.batch_size]
            batch_num = batch_start // args.batch_size + 1
            log(f"Processing batch {batch_num} ({batch_start + 1}-{batch_start + len(batch)} / {total})")
            for row in batch:
                if args.parse_only:
                    metrics = process_instrument_parse_only(conn, cache_dir, row, asof_date)
                else:
                    metrics = process_instrument(conn, client, cache_dir, row, asof_date, args.mode)
                found_url += int(metrics["url_found"])
                downloaded += int(metrics["downloaded"])
                parsed += int(metrics["parsed"])
                charges_populated_run += int(metrics["charges_populated"])

        if args.mode == "template":
            charges_populated_latest = compute_subset_charges_populated(conn, selected_instrument_ids)
            print_template_mode_kpis(
                attempted=total,
                found_url=found_url,
                downloaded=downloaded,
                parsed=parsed,
                charges_populated=charges_populated_latest,
            )
            print_template_success_samples(conn, selected_instrument_ids, limit=20)
            log(
                "template run summary: "
                f"url_found={found_url}/{total}, downloaded={downloaded}/{total}, "
                f"parsed={parsed}/{total}, charges_populated_run={charges_populated_run}/{total}"
            )
        else:
            coverage = compute_universe_coverage(conn)
            print_coverage_stats(
                int(coverage["total"] or 0),
                int(coverage["found_url"] or 0),
                int(coverage["downloaded"] or 0),
                int(coverage["parsed"] or 0),
                int(coverage["charges_populated"] or 0),
            )
            print_samples(conn)

        print_outlier_stats(conn)
        print(
            "\nissuer backfilled from kid_domain total: "
            + str(conn.execute("SELECT COUNT(*) FROM instrument WHERE issuer_source='kid_domain'").fetchone()[0])
        )

        conn.commit()
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
