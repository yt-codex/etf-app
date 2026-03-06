#!/usr/bin/env python3
"""Stage 2.7 deterministic Avantis UCITS KID enrichment."""

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
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from pypdf import PdfReader

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover
    fitz = None

from etf_app.kid_ingest import parse_ongoing_charges

PARSER_VERSION = "stage2_7_avantis_kid_enrich_v1"
DOC_TYPE = "PRIIPS_KID"
SOURCE_NAME = "avantis_kid"
AVANTIS_LANDING_URL = "https://www.avantisinvestors.com/ucitsetf/"
AVANTIS_ISSUER_NORMALIZED = "Avantis / American Century ICAV"
AVANTIS_ISSUER_DOMAIN = "avantisinvestors.com"

# Deterministic fallback when the landing page does not enumerate funds.
FALLBACK_FUND_PAGES = [
    "https://www.avantisinvestors.com/ucitsetf/avantis-global-equity-ucits-etf/",
    "https://www.avantisinvestors.com/ucitsetf/avantis-america-equity-ucits-etf/",
    "https://www.avantisinvestors.com/ucitsetf/avantis-europe-equity-ucits-etf/",
    "https://www.avantisinvestors.com/ucitsetf/avantis-emerging-markets-equity-ucits-etf/",
    "https://www.avantisinvestors.com/ucitsetf/avantis-pacific-equity-ucits-etf/",
    "https://www.avantisinvestors.com/ucitsetf/avantis-global-small-cap-value-ucits-etf/",
]

ALLOWED_COST_FLAGS = (
    "ok",
    "partial",
    "issuer_page_ok",
    "amundi_factsheet_ok",
    "avantis_kid_ok",
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
    parser = argparse.ArgumentParser(description="Stage 2.7 Avantis UCITS KID enrichment")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--cache-dir", default="kid_cache/avantis", help="Cache directory")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--rate-limit", type=float, default=0.2, help="Inter-request delay seconds")
    parser.add_argument("--max-retries", type=int, default=1, help="Retry count for transient HTTP errors")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of funds to process (0=all)")
    parser.add_argument(
        "--landing-url",
        default=AVANTIS_LANDING_URL,
        help="Primary UCITS landing URL (default: https://www.avantisinvestors.com/ucitsetf/)",
    )
    return parser.parse_args(argv)


class HttpClient:
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
                "Accept": "application/pdf,application/octet-stream;q=0.9,text/html,*/*;q=0.8",
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
        attempt = 0
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                response = self.session.get(url, headers=headers, timeout=request_timeout, allow_redirects=True)
                self.last_request_at = time.monotonic()
                if response.status_code in {429, 500, 502, 503, 504} and attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                return response
            except requests.RequestException:
                if attempt <= self.max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise
        raise RuntimeError("GET failed unexpectedly")


def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_type: str,
    default_sql: Optional[str] = None,
) -> bool:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    if column_name in cols:
        return False
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}{default_clause}")
    return True


def ensure_schema(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "issuer", "normalized_name", "TEXT")
    ensure_column(conn, "issuer", "domain", "TEXT")
    ensure_column(conn, "instrument", "issuer_source", "TEXT")

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

    conn.execute("DROP VIEW IF EXISTS instrument_cost_current")
    flag_sql = ", ".join(f"'{flag}'" for flag in ALLOWED_COST_FLAGS)
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


def canonicalize_fund_url(url: str) -> Optional[str]:
    if not url:
        return None
    absolute = urljoin("https://www.avantisinvestors.com", url)
    parsed = urlparse(absolute)
    if not parsed.netloc.lower().endswith("avantisinvestors.com"):
        return None
    path = parsed.path or ""
    if "/ucitsetf/" not in path.lower():
        return None
    segments = [seg for seg in path.split("/") if seg]
    if not segments or segments[0].lower() != "ucitsetf":
        return None
    # Canonicalize share-class pages (/.../<ticker>/) to fund-level URL.
    if len(segments) >= 3 and re.fullmatch(r"[a-z0-9]{3,6}", segments[-1], flags=re.IGNORECASE):
        segments = segments[:-1]
    canonical_path = "/" + "/".join(segments) + "/"
    cleaned = parsed._replace(path=canonical_path, query="", fragment="")
    return urlunparse(cleaned)


def extract_fund_links_from_landing(html: str) -> list[str]:
    out: set[str] = set()
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        candidate = canonicalize_fund_url(match.group(1))
        if candidate:
            out.add(candidate)
    for match in re.finditer(r"https?://www\.avantisinvestors\.com/ucitsetf/[^\s\"'<>]+", html, flags=re.IGNORECASE):
        candidate = canonicalize_fund_url(match.group(0))
        if candidate:
            out.add(candidate)
    out.discard("https://www.avantisinvestors.com/ucitsetf/")
    return sorted(out)


def discover_fund_pages(client: HttpClient, landing_url: str) -> tuple[list[str], dict[str, object]]:
    debug: dict[str, object] = {"landing_url": landing_url}
    discovered: list[str] = []
    try:
        resp = client.get(landing_url)
        debug["landing_status"] = int(resp.status_code)
        debug["landing_final_url"] = resp.url
        debug["landing_content_type"] = resp.headers.get("content-type")
        if resp.text:
            discovered = extract_fund_links_from_landing(resp.text)
    except requests.RequestException as exc:
        debug["landing_error"] = f"request_error:{exc.__class__.__name__}"

    if discovered:
        debug["discovery_method"] = "landing_links"
        return discovered, debug

    # Deterministic fallback list when /ucitsetf/ no longer enumerates links.
    live_fallbacks: list[str] = []
    fallback_checks: list[dict[str, object]] = []
    for candidate in FALLBACK_FUND_PAGES:
        check: dict[str, object] = {"url": candidate}
        try:
            resp = client.get(candidate)
            check["status"] = int(resp.status_code)
            check["final_url"] = resp.url
            check["has_kid_label"] = "KID PRIIP - ACC ETF - EN" in (resp.text or "")
            if resp.status_code == 200 and check["has_kid_label"]:
                canonical = canonicalize_fund_url(resp.url or candidate)
                if canonical:
                    live_fallbacks.append(canonical)
        except requests.RequestException as exc:
            check["error"] = f"request_error:{exc.__class__.__name__}"
        fallback_checks.append(check)
    debug["discovery_method"] = "fallback_probe"
    debug["fallback_checks"] = fallback_checks
    return sorted(set(live_fallbacks)), debug


def extract_kid_url_from_fund_page(html: str) -> Optional[str]:
    m = re.search(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*KID\s*PRIIP\s*-\s*ACC ETF\s*-\s*EN\s*</a>',
        html,
        flags=re.IGNORECASE,
    )
    if m:
        return urljoin("https://www.avantisinvestors.com", m.group(1))

    candidates = re.findall(
        r"https?://res\.avantisinvestors\.com/avantis/ucits-etfs/PRIIP-[^\"'\s<>]+-EN\.pdf",
        html,
        flags=re.IGNORECASE,
    )
    if candidates:
        return candidates[0]
    return None


def is_pdf_content(content_type: Optional[str], final_url: Optional[str], content: bytes) -> bool:
    ctype = (content_type or "").lower()
    if "pdf" in ctype:
        return True
    if final_url and final_url.lower().endswith(".pdf"):
        return True
    if content.startswith(b"%PDF"):
        return True
    return False


def download_pdf_with_cache(client: HttpClient, url: str, cache_dir: Path) -> DownloadResult:
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
        response = client.get(url)
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


def extract_text_pypdf(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    try:
        reader = PdfReader(pdf_bytes)
        pages = [(page.extract_text() or "") for page in reader.pages]
        return "\n".join(pages), len(pages), None
    except Exception as exc:
        return "", 0, f"pypdf_exception:{exc.__class__.__name__}"


def extract_text_fitz(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    if fitz is None:
        return "", 0, "fitz_unavailable"
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [doc[i].get_text("text") or "" for i in range(doc.page_count)]
        return "\n".join(pages), doc.page_count, None
    except Exception as exc:
        return "", 0, f"fitz_exception:{exc.__class__.__name__}"


def extract_pdf_text_with_fallback(pdf_bytes: bytes) -> dict[str, object]:
    pypdf_text, pypdf_pages, pypdf_err = extract_text_pypdf(pdf_bytes)
    errors: list[str] = []
    if pypdf_err:
        errors.append(pypdf_err)
    normalized = normalize_space(pypdf_text)
    if normalized:
        return {
            "text": pypdf_text,
            "page_count": pypdf_pages,
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
            "page_count": fitz_pages or pypdf_pages,
            "extractor": "fitz",
            "fallback_used": True,
            "errors": errors,
        }
    return {
        "text": pypdf_text,
        "page_count": pypdf_pages,
        "extractor": "pypdf",
        "fallback_used": True,
        "errors": errors,
    }


def parse_distribution(text: str) -> Optional[str]:
    t = normalize_space(text)
    if not t:
        return None
    patterns = [
        r"(Type of shares|Share class|Class)\s*[:\-]?\s*(Accumulating|Accumulation|Distributing|Distribution)",
        r"(Accumulating|Accumulation)",
        r"(Distributing|Distribution)",
    ]
    for pattern in patterns:
        m = re.search(pattern, t, flags=re.IGNORECASE)
        if not m:
            continue
        token = m.group(m.lastindex or 1).strip().lower()
        if token.startswith("accumul"):
            return "Accumulating"
        if token.startswith("distrib"):
            return "Distributing"
    return None


def is_valid_isin(isin: str) -> bool:
    code = (isin or "").upper().strip()
    if not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", code):
        return False
    expanded: list[str] = []
    for ch in code:
        if "A" <= ch <= "Z":
            expanded.append(str(ord(ch) - 55))
        else:
            expanded.append(ch)
    digits = "".join(expanded)
    total = 0
    double = False
    for ch in reversed(digits):
        n = int(ch)
        if double:
            n *= 2
            total += (n // 10) + (n % 10)
        else:
            total += n
        double = not double
    return total % 10 == 0


def extract_isins_from_text(text: str) -> list[str]:
    if not text:
        return []
    hits = re.findall(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b", text.upper())
    seen = set()
    out: list[str] = []
    for isin in hits:
        if not is_valid_isin(isin):
            continue
        if isin in seen:
            continue
        seen.add(isin)
        out.append(isin)
    return out


def extract_fee_from_text(text: str) -> tuple[Optional[float], Optional[str]]:
    normalized = normalize_space(text)
    if not normalized:
        return None, None
    patterns = [
        r"Management fees and other administrative or operating costs\s*([0-9]+(?:[.,][0-9]+)?)\s*%",
        r"Management fee[s]?\s*([0-9]+(?:[.,][0-9]+)?)\s*%",
        r"Ongoing costs(?:\s+taken each year)?[^%]{0,220}?([0-9]+(?:[.,][0-9]+)?)\s*%",
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
        if 0.0 <= value <= 100.0:
            snippet = normalized[max(0, m.start() - 100) : min(len(normalized), m.end() + 160)]
            return value, snippet
    return None, None


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


def upsert_avantis_issuer(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT issuer_id FROM issuer WHERE normalized_name = ? ORDER BY issuer_id LIMIT 1",
        (AVANTIS_ISSUER_NORMALIZED,),
    ).fetchone()
    if row:
        issuer_id = int(row["issuer_id"])
        conn.execute(
            "UPDATE issuer SET domain = COALESCE(domain, ?) WHERE issuer_id = ?",
            (AVANTIS_ISSUER_DOMAIN, issuer_id),
        )
        return issuer_id

    row = conn.execute(
        "SELECT issuer_id FROM issuer WHERE issuer_name = ? ORDER BY issuer_id LIMIT 1",
        (AVANTIS_ISSUER_NORMALIZED,),
    ).fetchone()
    if row:
        issuer_id = int(row["issuer_id"])
        conn.execute(
            """
            UPDATE issuer
            SET normalized_name = COALESCE(normalized_name, ?),
                domain = COALESCE(domain, ?)
            WHERE issuer_id = ?
            """,
            (AVANTIS_ISSUER_NORMALIZED, AVANTIS_ISSUER_DOMAIN, issuer_id),
        )
        return issuer_id

    cur = conn.execute(
        """
        INSERT INTO issuer(issuer_name, website, created_at, normalized_name, domain)
        VALUES (?, NULL, ?, ?, ?)
        """,
        (AVANTIS_ISSUER_NORMALIZED, now_utc_iso(), AVANTIS_ISSUER_NORMALIZED, AVANTIS_ISSUER_DOMAIN),
    )
    return int(cur.lastrowid)


def backfill_instrument_issuer(conn: sqlite3.Connection, instrument_id: int, issuer_id: int) -> int:
    row = conn.execute(
        "SELECT issuer_id FROM instrument WHERE instrument_id = ?",
        (instrument_id,),
    ).fetchone()
    if not row:
        return 0
    current = row["issuer_id"]
    if current is not None:
        return 0
    return conn.execute(
        "UPDATE instrument SET issuer_id = ?, issuer_source = 'kid' WHERE instrument_id = ? AND issuer_id IS NULL",
        (issuer_id, instrument_id),
    ).rowcount


def find_instrument_by_isin(conn: sqlite3.Connection, isin: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT i.instrument_id, i.isin, i.instrument_name, l.venue_mic, l.ticker
        FROM instrument i
        LEFT JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        WHERE i.isin = ?
        LIMIT 1
        """,
        (isin,),
    ).fetchone()


def choose_isin_for_existing_instrument(conn: sqlite3.Connection, isins: list[str]) -> Optional[str]:
    if not isins:
        return None
    placeholders = ",".join("?" for _ in isins)
    rows = conn.execute(
        f"""
        SELECT isin
        FROM instrument
        WHERE isin IN ({placeholders})
        ORDER BY isin
        """,
        tuple(isins),
    ).fetchall()
    if rows:
        return str(rows[0]["isin"])
    return None


def find_existing_document(conn: sqlite3.Connection, instrument_id: int, url: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT document_id, hash_sha256
        FROM document
        WHERE instrument_id = ? AND doc_type = ? AND url = ?
        ORDER BY document_id DESC
        LIMIT 1
        """,
        (instrument_id, DOC_TYPE, url),
    ).fetchone()


def insert_document_version(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    url: str,
    pdf_bytes: bytes,
    effective_date: Optional[str],
    language: Optional[str],
) -> int:
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    existing = find_existing_document(conn, instrument_id, url)
    stored_url = url
    if existing and str(existing["hash_sha256"] or "") == sha:
        return int(existing["document_id"])
    if existing and str(existing["hash_sha256"] or "") != sha:
        stored_url = f"{url}#sha256={sha[:12]}"
    conn.execute(
        """
        INSERT INTO document(
            instrument_id, doc_type, url, retrieved_at, hash_sha256,
            effective_date, language, parser_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def insert_cost_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    ongoing_charges: float,
    doc_id: int,
    raw_json: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO cost_snapshot(
            instrument_id, asof_date, ongoing_charges, entry_costs, exit_costs,
            transaction_costs, doc_id, quality_flag, raw_json
        ) VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            instrument_id,
            asof_date,
            ongoing_charges,
            doc_id,
            "avantis_kid_ok",
            json.dumps(raw_json, ensure_ascii=True),
        ),
    )


def insert_issuer_metadata_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    source_url: str,
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


def print_kpis(
    *,
    funds_discovered: int,
    kids_downloaded: int,
    fees_parsed: int,
    costs_written: int,
    issuer_backfilled: int,
    samples: list[tuple[str, str, str, float]],
) -> None:
    print("\n=== Stage 2.7 KPIs ===")
    print(f"funds_discovered: {funds_discovered}")
    print(f"kids_downloaded: {kids_downloaded}")
    print(f"fees_parsed: {fees_parsed}")
    print(f"cost_snapshot_written: {costs_written}")
    print(f"issuer_backfilled: {issuer_backfilled}")
    print("\n=== Sample Successes (up to 10) ===")
    if not samples:
        print("No successful fee enrichments.")
        return
    for isin, ticker, kid_url, fee in samples[:10]:
        print(f"{isin} | {ticker} | {fee:.4f} | {kid_url}")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    http = HttpClient(rate_limit=args.rate_limit, timeout=args.timeout, max_retries=args.max_retries)
    cache_dir = Path(args.cache_dir)
    asof_date = dt.date.today().isoformat()

    funds_discovered = 0
    kids_downloaded = 0
    fees_parsed = 0
    costs_written = 0
    issuer_backfilled = 0
    samples: list[tuple[str, str, str, float]] = []

    try:
        conn.execute("BEGIN")
        ensure_schema(conn)

        fund_pages, discovery_debug = discover_fund_pages(http, args.landing_url)
        if args.limit and args.limit > 0:
            fund_pages = fund_pages[: args.limit]
        funds_discovered = len(fund_pages)

        print("\n=== Avantis Funds Discovered ===")
        if not fund_pages:
            print("No fund pages discovered.")
        for url in fund_pages:
            print(url)

        if not fund_pages:
            log(f"No fund pages discovered. debug={json.dumps(discovery_debug, ensure_ascii=True)[:600]}")
            conn.commit()
            print_kpis(
                funds_discovered=funds_discovered,
                kids_downloaded=kids_downloaded,
                fees_parsed=fees_parsed,
                costs_written=costs_written,
                issuer_backfilled=issuer_backfilled,
                samples=samples,
            )
            return 0

        avantis_issuer_id = upsert_avantis_issuer(conn)

        for idx, fund_url in enumerate(fund_pages, start=1):
            page_resp = http.get(fund_url)
            if page_resp.status_code != 200:
                log(f"skip fund page (non-200): {fund_url} status={page_resp.status_code}")
                continue
            html = page_resp.text or ""
            kid_url = extract_kid_url_from_fund_page(html)
            if not kid_url:
                log(f"skip fund page (no KID link): {fund_url}")
                continue

            dl = download_pdf_with_cache(http, kid_url, cache_dir)
            if not dl.success or not dl.pdf_bytes:
                log(f"skip KID download failure: {kid_url} ({dl.error})")
                continue
            kids_downloaded += 1

            parsed = parse_ongoing_charges(dl.pdf_bytes)
            ongoing = parsed.get("ongoing_charges")
            fee = float(ongoing) if isinstance(ongoing, (int, float)) else None
            text_meta = extract_pdf_text_with_fallback(dl.pdf_bytes)
            text = str(text_meta.get("text") or "")
            fee_snippet = None
            if fee is None:
                fee, fee_snippet = extract_fee_from_text(text)
            if fee is not None:
                fees_parsed += 1
            distribution = parse_distribution(text)
            isins = extract_isins_from_text(text)
            chosen_isin = choose_isin_for_existing_instrument(conn, isins) or (isins[0] if isins else None)

            if not chosen_isin:
                log(f"skip (ISIN not found in KID): {kid_url}")
                continue

            instrument = find_instrument_by_isin(conn, chosen_isin)
            if not instrument:
                log(f"skip (ISIN not in DB): {chosen_isin} from {kid_url}")
                continue
            instrument_id = int(instrument["instrument_id"])
            ticker = normalize_space(str(instrument["ticker"] or ""))

            upsert_url_map(conn, instrument_id, "avantis_fund_page", fund_url)
            upsert_url_map(conn, instrument_id, "avantis_kid_pdf", kid_url)

            doc_id = insert_document_version(
                conn,
                instrument_id=instrument_id,
                url=kid_url,
                pdf_bytes=dl.pdf_bytes,
                effective_date=parsed.get("effective_date") if isinstance(parsed.get("effective_date"), str) else None,
                language=parsed.get("language") if isinstance(parsed.get("language"), str) else None,
            )

            if fee is not None:
                raw_json = {
                    "source": SOURCE_NAME,
                    "parser_version": PARSER_VERSION,
                    "fund_url": fund_url,
                    "kid_url": kid_url,
                    "distribution": distribution,
                    "isins_in_kid": isins,
                    "chosen_isin": chosen_isin,
                    "parse_ongoing": {
                        "ongoing_charges": parsed.get("ongoing_charges"),
                        "entry_costs": parsed.get("entry_costs"),
                        "exit_costs": parsed.get("exit_costs"),
                        "transaction_costs": parsed.get("transaction_costs"),
                        "effective_date": parsed.get("effective_date"),
                        "language": parsed.get("language"),
                        "snippet": parsed.get("snippet"),
                        "extractor": parsed.get("extractor"),
                        "fallback_used": parsed.get("fallback_used"),
                        "page_count": parsed.get("page_count"),
                        "error": parsed.get("error"),
                    },
                    "fee_fallback_snippet": fee_snippet,
                    "text_extract": {
                        "extractor": text_meta.get("extractor"),
                        "fallback_used": text_meta.get("fallback_used"),
                        "page_count": text_meta.get("page_count"),
                        "errors": text_meta.get("errors"),
                    },
                }
                insert_cost_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    ongoing_charges=fee,
                    doc_id=doc_id,
                    raw_json=raw_json,
                )
                costs_written += 1
                if len(samples) < 10:
                    samples.append((chosen_isin, ticker, kid_url, fee))

            metadata_json = {
                "source": SOURCE_NAME,
                "parser_version": PARSER_VERSION,
                "fund_url": fund_url,
                "kid_url": kid_url,
                "distribution_policy": distribution,
                "ucits_compliant": 1,
            }
            insert_issuer_metadata_snapshot(
                conn,
                instrument_id=instrument_id,
                asof_date=asof_date,
                source_url=kid_url,
                ter=fee,
                use_of_income=distribution,
                ucits_compliant=1,
                quality_flag="ok" if fee is not None else "partial",
                raw_json=metadata_json,
            )

            issuer_backfilled += backfill_instrument_issuer(conn, instrument_id, avantis_issuer_id)

            if idx % 10 == 0:
                log(
                    f"processed={idx}/{funds_discovered} "
                    f"kids_downloaded={kids_downloaded} fees_parsed={fees_parsed} costs_written={costs_written}"
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print_kpis(
        funds_discovered=funds_discovered,
        kids_downloaded=kids_downloaded,
        fees_parsed=fees_parsed,
        costs_written=costs_written,
        issuer_backfilled=issuer_backfilled,
        samples=samples,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
