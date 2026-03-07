#!/usr/bin/env python3
"""Stage 2.5 Amundi metadata enrichment via monthly factsheets (PDF)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import re
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from pypdf import PdfReader

try:
    import fitz  # type: ignore
except Exception:
    fitz = None

PARSER_VERSION = "stage2_5_amundi_enrich_v2"
AMUNDI_SOURCE = "amundi_monthly_factsheet"
AMUNDI_BASE_URL = "https://www.amundietf.com"
FACTSHEET_TEMPLATE = (
    "https://www.amundietf.ch/pdfDocuments/monthly-factsheet/{ISIN_UPPER}/ENG/CHE/INSTITUTIONNEL/ETF"
)
DOCUMENT_API_URL = f"{AMUNDI_BASE_URL}/mapi/DocumentAPI/document/getByProductIdsAndContext"
DOCUMENT_API_CONTEXTS = (
    {"countryCode": "SGP", "languageCode": "en", "userProfileName": "RETAIL"},
    {"countryCode": "GBR", "languageCode": "en", "userProfileName": "RETAIL"},
    {"countryCode": "LUX", "languageCode": "en", "userProfileName": "RETAIL"},
    {"countryCode": "FRA", "languageCode": "en", "userProfileName": "RETAIL"},
    {"countryCode": "CHE", "languageCode": "en", "userProfileName": "INSTIT"},
    {"countryCode": "CHE", "languageCode": "en", "userProfileName": "RETAIL"},
    {"countryCode": "LUX", "languageCode": "en", "userProfileName": "INSTIT"},
)
DISCOVERY_BATCH_SIZE = 100
AMUNDI_KID_TEMPLATE_CONTEXTS = (
    ("ENG", "LUX"),
    ("ENG", "GBR"),
    ("ENG", "CHE"),
    ("ENG", "DEU"),
    ("ENG", "FRA"),
    ("FRA", "FRA"),
    ("DEU", "DEU"),
    ("ENG", "ESP"),
    ("ENG", "NLD"),
)
SELF_TEST_ISIN = "LU1407890547"


@dataclass
class ProbeResult:
    accepted: bool
    reason: str
    status_code: Optional[int]
    content_type: Optional[str]
    final_url: Optional[str]
    method: str
    first_bytes_hex: Optional[str]


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
class DiscoveredFactsheet:
    url: str
    context_country: str
    user_profile: str
    language: Optional[str]
    record_date: Optional[int]
    document_name: Optional[str]
    applied_alias: Optional[str]


@dataclass
class FactsheetCandidate:
    url: str
    source: str


@dataclass
class KidFallbackResult:
    success: bool
    source_url: Optional[str]
    parsed: Optional[dict[str, object]]
    attempts: list[dict[str, object]]


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def normalize_space(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2.5 Amundi enrichment")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--cache-dir", default="kid_cache", help="Local PDF cache directory")
    parser.add_argument("--limit", type=int, default=2000, help="Maximum instruments to attempt")
    parser.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue filter",
    )
    parser.add_argument("--rate-limit", type=float, default=0.2, help="HTTP delay in seconds")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--max-retries", type=int, default=1, help="HTTP retry count")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Fetch+parse the LU1407890547 factsheet and print extracted fields.",
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
                "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            }
        )

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

    def get(self, url: str, *, stream: bool = False, timeout: Optional[int] = None) -> requests.Response:
        attempt = 0
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                resp = self.session.get(
                    url,
                    timeout=request_timeout,
                    allow_redirects=True,
                    stream=stream,
                )
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
        raise RuntimeError("GET request failed unexpectedly")

    def head(self, url: str, *, timeout: Optional[int] = None) -> requests.Response:
        attempt = 0
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                resp = self.session.head(url, timeout=request_timeout, allow_redirects=True)
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
        raise RuntimeError("HEAD request failed unexpectedly")

    def post_json(
        self,
        url: str,
        payload: dict[str, object],
        *,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        attempt = 0
        request_timeout = timeout or self.timeout
        while attempt <= self.max_retries:
            attempt += 1
            try:
                self._throttle()
                resp = self.session.post(
                    url,
                    json=payload,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    timeout=request_timeout,
                    allow_redirects=True,
                )
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
        raise RuntimeError("POST request failed unexpectedly")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def venue_scope(arg: str) -> list[str]:
    if arg == "XLON":
        return ["XLON"]
    if arg == "XETR":
        return ["XETR"]
    return ["XLON", "XETR"]


def build_factsheet_url(isin: str) -> str:
    return FACTSHEET_TEMPLATE.format(ISIN_UPPER=isin.upper())


def build_absolute_amundi_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if re.match(r"^https?://", url, flags=re.IGNORECASE):
        return url
    return urljoin(f"{AMUNDI_BASE_URL}/", url.lstrip("/"))


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def extract_numeric_token(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def select_monthly_factsheet_document(docs: list[dict[str, object]]) -> Optional[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for doc in docs:
        document_type = doc.get("documentType") or {}
        if not isinstance(document_type, dict):
            continue
        if str(document_type.get("name") or "").lower() != "monthlyfactsheet":
            continue
        url = build_absolute_amundi_url(str(doc.get("url") or doc.get("appliedAlias") or "") or None)
        if not url:
            continue
        candidates.append(doc)

    if not candidates:
        return None

    def sort_key(doc: dict[str, object]) -> tuple[int, int, int, str]:
        language = str(doc.get("language") or "").strip().lower()
        alias = str(doc.get("appliedAlias") or "")
        record_date = extract_numeric_token(doc.get("recordDate")) or 0
        return (
            0 if language == "english" else 1,
            -record_date,
            0 if "/ETF/" in alias else 1,
            str(doc.get("name") or ""),
        )

    return sorted(candidates, key=sort_key)[0]


def discover_monthly_factsheet_urls(client: HttpClient, isins: list[str]) -> dict[str, DiscoveredFactsheet]:
    discovered: dict[str, DiscoveredFactsheet] = {}
    unresolved = [isin.upper() for isin in isins if isin]
    if not unresolved:
        return discovered

    for context in DOCUMENT_API_CONTEXTS:
        pending = [isin for isin in unresolved if isin not in discovered]
        if not pending:
            break
        for batch in chunked(pending, DISCOVERY_BATCH_SIZE):
            try:
                response = client.post_json(
                    DOCUMENT_API_URL,
                    {"productIds": batch, "context": context},
                    timeout=25,
                )
            except requests.RequestException:
                continue
            if int(response.status_code) != 200:
                continue
            try:
                payload = response.json()
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue

            for isin, docs in payload.items():
                if not isinstance(isin, str) or not isinstance(docs, list):
                    continue
                if isin in discovered:
                    continue
                selected = select_monthly_factsheet_document(docs)
                if not selected:
                    continue
                absolute_url = build_absolute_amundi_url(
                    str(selected.get("url") or selected.get("appliedAlias") or "") or None
                )
                if not absolute_url:
                    continue
                discovered[isin] = DiscoveredFactsheet(
                    url=absolute_url,
                    context_country=str(context["countryCode"]),
                    user_profile=str(context["userProfileName"]),
                    language=str(selected.get("language") or "") or None,
                    record_date=extract_numeric_token(selected.get("recordDate")),
                    document_name=str(selected.get("name") or "") or None,
                    applied_alias=str(selected.get("appliedAlias") or "") or None,
                )
    return discovered


def build_factsheet_candidates(
    isin: str,
    *,
    discovered: Optional[DiscoveredFactsheet],
    known_url: Optional[str],
) -> list[FactsheetCandidate]:
    candidates: list[FactsheetCandidate] = []
    seen: set[str] = set()

    def add(url: Optional[str], source: str) -> None:
        absolute_url = build_absolute_amundi_url(url)
        if not absolute_url:
            return
        key = absolute_url.strip()
        if not key or key in seen:
            return
        seen.add(key)
        candidates.append(FactsheetCandidate(url=absolute_url, source=source))

    if discovered is not None:
        add(
            discovered.url,
            f"document_api:{discovered.context_country}:{discovered.user_profile}:{discovered.language or 'unknown'}",
        )
    add(known_url, "instrument_url_map")
    add(build_factsheet_url(isin), "legacy_template")
    return candidates


def build_amundi_kid_candidate_urls(isin: str) -> list[str]:
    isin_upper = isin.upper()
    return [
        f"https://www.amundietf.lu/pdfDocuments/kid-priips/{isin_upper}/{language}/{country}"
        for language, country in AMUNDI_KID_TEMPLATE_CONTEXTS
    ]


def try_amundi_kid_fallback(client: HttpClient, cache_dir: Path, isin: str) -> KidFallbackResult:
    from etf_app.kid_ingest import detect_language_from_url, parse_ongoing_charges

    attempts: list[dict[str, object]] = []
    for candidate_url in build_amundi_kid_candidate_urls(isin):
        probe = probe_pdf_url(client, candidate_url)
        attempt: dict[str, object] = {
            "candidate_url": candidate_url,
            "probe": {
                "accepted": probe.accepted,
                "reason": probe.reason,
                "status_code": probe.status_code,
                "content_type": probe.content_type,
                "final_url": probe.final_url,
                "method": probe.method,
                "first_bytes_hex": probe.first_bytes_hex,
            },
        }
        if not probe.accepted:
            attempts.append(attempt)
            continue

        final_url = probe.final_url or candidate_url
        dl = download_pdf(client, final_url, cache_dir)
        attempt["download"] = {
            "success": dl.success,
            "error": dl.error,
            "final_url": dl.final_url,
            "from_cache": dl.from_cache,
            "status": dl.http_status,
            "content_type": dl.content_type,
            "cache_path": str(dl.cache_path) if dl.cache_path else None,
        }
        if not dl.success or not dl.pdf_bytes:
            attempts.append(attempt)
            continue

        parsed = parse_ongoing_charges(dl.pdf_bytes)
        if not parsed.get("language"):
            parsed["language"] = detect_language_from_url(dl.final_url or final_url)
        attempt["parse"] = {
            "ongoing_charges": parsed.get("ongoing_charges"),
            "benchmark_name": parsed.get("benchmark_name"),
            "asset_class_hint": parsed.get("asset_class_hint"),
            "domicile_country": parsed.get("domicile_country"),
            "replication_method": parsed.get("replication_method"),
            "hedged_flag": parsed.get("hedged_flag"),
            "hedged_target": parsed.get("hedged_target"),
            "effective_date": parsed.get("effective_date"),
            "language": parsed.get("language"),
            "extractor": parsed.get("extractor"),
            "fallback_used": parsed.get("fallback_used"),
            "error": parsed.get("error"),
        }
        attempts.append(attempt)
        if parsed.get("ongoing_charges") is not None:
            return KidFallbackResult(
                success=True,
                source_url=dl.final_url or final_url,
                parsed=parsed,
                attempts=attempts,
            )

    return KidFallbackResult(success=False, source_url=None, parsed=None, attempts=attempts)


def merge_profile_metadata(
    primary: dict[str, object],
    secondary: Optional[dict[str, object]],
    *,
    ter_field: str = "ongoing_charges",
) -> dict[str, object]:
    merged = dict(primary)
    if not secondary:
        return merged
    field_map = {
        "benchmark_name": "benchmark_name",
        "asset_class_hint": "asset_class_hint",
        "domicile_country": "domicile_country",
        "fund_size_value": "fund_size_value",
        "fund_size_currency": "fund_size_currency",
        "fund_size_asof": "fund_size_asof",
        "fund_size_scope": "fund_size_scope",
        "replication_method": "replication_method",
        "hedged_flag": "hedged_flag",
        "hedged_target": "hedged_target",
        "ter": ter_field,
    }
    for target_field, source_field in field_map.items():
        if merged.get(target_field) is None and secondary.get(source_field) is not None:
            merged[target_field] = secondary.get(source_field)
    return merged


def is_pdf_probe_success(content_type: Optional[str], final_url: Optional[str], first_bytes: bytes) -> bool:
    ctype = (content_type or "").lower()
    if "pdf" in ctype:
        return True
    if final_url and final_url.lower().endswith(".pdf"):
        return True
    if first_bytes.startswith(b"%PDF"):
        return True
    return False


def probe_pdf_url(client: HttpClient, candidate_url: str) -> ProbeResult:
    try:
        head_resp = client.head(candidate_url, timeout=12)
        head_status = int(head_resp.status_code)
        head_type = head_resp.headers.get("content-type")
        head_final = head_resp.url or candidate_url
        if head_status == 200 and "pdf" in (head_type or "").lower():
            return ProbeResult(
                accepted=True,
                reason="head_200_pdf_content_type",
                status_code=head_status,
                content_type=head_type,
                final_url=head_final,
                method="HEAD",
                first_bytes_hex=None,
            )
    except requests.RequestException as exc:
        head_status = None
        head_type = None
        head_final = candidate_url
        head_err = f"head_error:{exc.__class__.__name__}"
    else:
        head_err = f"head_status_{head_status}" if head_status != 200 else "head_non_pdf"

    try:
        get_resp = client.get(candidate_url, stream=True, timeout=12)
        status = int(get_resp.status_code)
        ctype = get_resp.headers.get("content-type")
        final_url = get_resp.url or candidate_url
        first_bytes = b""
        try:
            for chunk in get_resp.iter_content(chunk_size=1024):
                if chunk:
                    first_bytes = chunk[:1024]
                    break
        finally:
            get_resp.close()
    except requests.RequestException as exc:
        return ProbeResult(
            accepted=False,
            reason=f"{head_err}|get_error:{exc.__class__.__name__}",
            status_code=head_status,
            content_type=head_type,
            final_url=head_final,
            method="GET",
            first_bytes_hex=None,
        )

    accepted = status == 200 and is_pdf_probe_success(ctype, final_url, first_bytes)
    reason = "get_200_pdf_probe_ok" if accepted else f"{head_err}|get_status_{status}"
    return ProbeResult(
        accepted=accepted,
        reason=reason,
        status_code=status,
        content_type=ctype,
        final_url=final_url,
        method="GET",
        first_bytes_hex=first_bytes[:8].hex() if first_bytes else None,
    )


def download_pdf(client: HttpClient, url: str, cache_dir: Path) -> DownloadResult:
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
        response = client.get(url, stream=False)
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

    status = int(response.status_code)
    final_url = response.url or url
    content_type = response.headers.get("content-type")
    data = response.content or b""

    if status != 200:
        return DownloadResult(
            success=False,
            pdf_bytes=None,
            final_url=final_url,
            from_cache=False,
            error=f"http_{status}",
            cache_path=cache_path,
            http_status=status,
            content_type=content_type,
        )

    if not is_pdf_probe_success(content_type, final_url, data[:8]):
        marker = data.find(b"%PDF")
        if marker == -1:
            return DownloadResult(
                success=False,
                pdf_bytes=None,
                final_url=final_url,
                from_cache=False,
                error="not_pdf_content",
                cache_path=cache_path,
                http_status=status,
                content_type=content_type,
            )
        data = data[marker:]

    cache_path.write_bytes(data)
    return DownloadResult(
        success=True,
        pdf_bytes=data,
        final_url=final_url,
        from_cache=False,
        error=None,
        cache_path=cache_path,
        http_status=status,
        content_type=content_type,
    )


def extract_text_pypdf(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [(page.extract_text() or "") for page in reader.pages]
        return "\n".join(pages), len(pages), None
    except Exception as exc:
        return "", 0, f"pypdf_exception:{exc.__class__.__name__}"


def extract_text_fitz(pdf_bytes: bytes) -> tuple[str, int, Optional[str]]:
    if fitz is None:
        return "", 0, "fitz_unavailable"
    doc = None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text("text") or "" for page in doc]
        return "\n".join(pages), len(pages), None
    except Exception as exc:
        return "", 0, f"fitz_exception:{exc.__class__.__name__}"
    finally:
        if doc is not None:
            doc.close()


def extract_pdf_text_with_fallback(pdf_bytes: bytes) -> dict[str, object]:
    pypdf_text, pypdf_pages, pypdf_err = extract_text_pypdf(pdf_bytes)
    errors: list[str] = []
    if pypdf_err:
        errors.append(pypdf_err)
    normalized = normalize_space(pypdf_text)
    has_keywords = bool(
        re.search(
            r"(Management fees and other administrative or operating costs|Type of shares)",
            normalized,
            flags=re.IGNORECASE,
        )
    )
    if len(normalized) >= 600 and has_keywords:
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
        "fallback_used": False,
        "errors": errors,
    }


def split_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in re.split(r"[\r\n]+", text):
        norm = normalize_space(raw)
        if norm:
            lines.append(norm)
    return lines


def parse_percent_token(token: str) -> Optional[float]:
    cleaned = token.strip().replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value < 0 or value > 100:
        return None
    return value


def extract_fee(text: str, lines: list[str]) -> tuple[Optional[float], Optional[str]]:
    label_upper = "MANAGEMENT FEES AND OTHER ADMINISTRATIVE OR OPERATING COSTS"
    for line in lines:
        if label_upper in line.upper():
            match = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*%", line, flags=re.IGNORECASE)
            if match:
                parsed = parse_percent_token(match.group(1))
                if parsed is not None:
                    return parsed, line
        ter_match = re.search(
            r"TOTAL\s+EXPENSE\s+RATIO(?:\s*\(TER\))?\s*(?:P\.?\s*A\.?)?\s*([0-9]+(?:[.,][0-9]+)?)\s*%?",
            line,
            flags=re.IGNORECASE,
        )
        if ter_match:
            parsed = parse_percent_token(ter_match.group(1))
            if parsed is not None:
                return parsed, line

    normalized = normalize_space(text)
    m = re.search(
        r"Management fees and other administrative or operating costs.{0,180}?([0-9]+(?:[.,][0-9]+)?)\s*%",
        normalized,
        flags=re.IGNORECASE,
    )
    if m:
        parsed = parse_percent_token(m.group(1))
        if parsed is not None:
            return parsed, m.group(0)
    ter_match = re.search(
        r"Total Expense Ratio(?:\s*\(TER\))?\s*(?:p\.?\s*a\.?)?\s*([0-9]+(?:[.,][0-9]+)?)\s*%?",
        normalized,
        flags=re.IGNORECASE,
    )
    if ter_match:
        parsed = parse_percent_token(ter_match.group(1))
        if parsed is not None:
            return parsed, ter_match.group(0)
    return None, None


def map_distribution(token: str) -> Optional[str]:
    folded = unicodedata.normalize("NFKD", token.strip()).encode("ascii", errors="ignore").decode("ascii")
    t = folded.lower()
    if t.startswith("accumulat") or t.startswith("capitalis") or t.startswith("capitaliz"):
        return "Accumulating"
    if t.startswith("distribut"):
        return "Distributing"
    return None


def extract_distribution(text: str, lines: list[str]) -> tuple[Optional[str], Optional[str]]:
    pattern = re.compile(
        (
            r"(?:Type of shares|Income treatment).{0,100}?"
            r"(Accumulation|Distribution|Accumulating|Distributing|Capitalisation|Capitalization)"
        ),
        flags=re.IGNORECASE,
    )
    for line in lines:
        m = pattern.search(line)
        if m:
            mapped = map_distribution(m.group(1))
            if mapped:
                return mapped, line

    normalized = normalize_space(text)
    m = pattern.search(normalized)
    if m:
        mapped = map_distribution(m.group(1))
        if mapped:
            return mapped, m.group(0)
    return None, None


def extract_ucits(text: str, lines: list[str]) -> tuple[Optional[int], Optional[str]]:
    for line in lines:
        if "UCITS COMPLIANT" in line.upper():
            upper = line.upper()
            if re.search(r"\bNO\b", upper):
                return 0, line
            if re.search(r"\bYES\b", upper):
                return 1, line
            if "UCITS" in upper:
                return 1, line
            return None, line

    normalized = normalize_space(text)
    m = re.search(r"UCITS compliant.{0,80}", normalized, flags=re.IGNORECASE)
    if m:
        snippet = m.group(0)
        upper = snippet.upper()
        if re.search(r"\bNO\b", upper):
            return 0, snippet
        if re.search(r"\bYES\b", upper):
            return 1, snippet
        if "UCITS" in upper:
            return 1, snippet
        return None, snippet
    return None, None


def normalize_country_name(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    return text.title()


def normalize_date_to_iso(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%b/%Y", "%d/%B/%Y"):
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def parse_amount_token(value: Optional[str]) -> Optional[float]:
    text = normalize_space(value)
    if not text:
        return None
    token = text.replace(" ", "")
    if "," in token and "." in token:
        if token.rfind(".") > token.rfind(","):
            token = token.replace(",", "")
        else:
            token = token.replace(".", "").replace(",", ".")
    elif "," in token:
        parts = token.split(",")
        if len(parts) > 1 and all(part.isdigit() and len(part) == 3 for part in parts[1:]):
            token = "".join(parts)
        else:
            token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def normalize_replication_method(value: Optional[str]) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    folded = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")
    upper = folded.upper()
    if "PHYS" in upper:
        return "physical"
    if "SYNTH" in upper or "SWAP" in upper:
        return "synthetic"
    return text


def parse_hedged_metadata(text: str) -> tuple[Optional[int], Optional[str]]:
    normalized = normalize_space(text).upper()
    if not normalized:
        return None, None
    if "UNHEDGED" in normalized:
        return 0, None
    target_match = re.search(r"\b(USD|EUR|GBP|JPY|CHF)\s+(?:HDG|HEDGED)\b", normalized)
    if target_match:
        return 1, target_match.group(1)
    if "HEDGED" in normalized or " HDG" in f" {normalized} ":
        return 1, None
    return None, None


def extract_profile_metadata_from_factsheet(text: str, lines: list[str]) -> dict[str, object]:
    normalized = normalize_space(text)

    benchmark_name: Optional[str] = None
    benchmark_match = re.search(
        r"\bBenchmark\s*:\s*(.+?)(?:Date of the\s+(?:first|rst)\s+NAV|First NAV|Key Information|Objective and Investment Policy|$)",
        normalized,
        flags=re.IGNORECASE,
    )
    if benchmark_match:
        benchmark_name = normalize_space(benchmark_match.group(1))
        benchmark_name = re.sub(r"^[0-9]+(?:[.,][0-9]+)?%\s*", "", benchmark_name)

    asset_class_hint: Optional[str] = None
    asset_match = re.search(
        r"\bAsset class\s*:\s*(.+?)(?:Exposure\s*:|Information\s*\(|Fund structure\b|$)",
        normalized,
        flags=re.IGNORECASE,
    )
    if asset_match:
        asset_class_hint = normalize_space(asset_match.group(1))
    elif lines and lines[0].upper() in {"EQUITY", "BOND", "COMMODITY", "MULTI ASSET", "MONEY MARKET"}:
        asset_class_hint = lines[0].title()

    domicile_country: Optional[str] = None
    domicile_match = re.search(
        r"\bFund structure\b.*?\bunder\s+([A-Za-z ]+?)\s+law\b",
        normalized,
        flags=re.IGNORECASE,
    )
    if domicile_match:
        domicile_country = normalize_country_name(domicile_match.group(1))

    replication_match = re.search(
        r"\bReplication type\s*:\s*(.+?)(?:Benchmark\s*:|Date of the\s+(?:first|rst)\s+NAV|First NAV|Asset class\s*:|$)",
        normalized,
        flags=re.IGNORECASE,
    )
    replication_method = normalize_replication_method(replication_match.group(1) if replication_match else None)
    hedged_flag, hedged_target = parse_hedged_metadata(normalized)
    fund_size_value: Optional[float] = None
    fund_size_currency: Optional[str] = None
    fund_size_asof: Optional[str] = None
    fund_size_scope: Optional[str] = None

    fund_size_asof_match = re.search(
        r"\bAUM\s+as\s+of\s*:\s*(\d{2}/\d{2}/\d{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if fund_size_asof_match:
        fund_size_asof = normalize_date_to_iso(fund_size_asof_match.group(1))

    aum_match = re.search(
        r"Assets?\s+Under\s+Management(?:\s*\(AUM\))?\s*:?\s*([0-9][0-9., ]*)\s*\(\s*(million|billion|bn|m)\s+([A-Z]{3})\s*\)",
        normalized,
        flags=re.IGNORECASE,
    )
    if aum_match:
        base_value = parse_amount_token(aum_match.group(1))
        unit = aum_match.group(2).lower()
        multiplier = 1_000_000.0 if unit in {"million", "m"} else 1_000_000_000.0
        if base_value is not None:
            fund_size_value = base_value * multiplier
            fund_size_currency = aum_match.group(3).upper()
            fund_size_scope = "fund"
    else:
        alt_unit_match = re.search(
            r"(?:Total Fund Assets|Fund Net asset Value|Total asset)\s*:?\s*([0-9][0-9., ]*)\s*\(\s*(million|billion|bn|m)\s+([A-Z]{3})\s*\)",
            normalized,
            flags=re.IGNORECASE,
        )
        if alt_unit_match:
            base_value = parse_amount_token(alt_unit_match.group(1))
            unit = alt_unit_match.group(2).lower()
            multiplier = 1_000_000.0 if unit in {"million", "m"} else 1_000_000_000.0
            if base_value is not None:
                fund_size_value = base_value * multiplier
                fund_size_currency = alt_unit_match.group(3).upper()
                fund_size_scope = "fund"
        else:
            alt_match = re.search(
                r"(?:Total Fund Assets|Fund Net asset Value|Total asset)\s*:?\s*([0-9][0-9., ]*)\s*\(\s*([A-Z]{3})\s*\)",
                normalized,
                flags=re.IGNORECASE,
            )
            if alt_match:
                fund_size_value = parse_amount_token(alt_match.group(1))
                fund_size_currency = alt_match.group(2).upper()
                fund_size_scope = "fund"

    return {
        "benchmark_name": benchmark_name,
        "asset_class_hint": asset_class_hint,
        "domicile_country": domicile_country,
        "fund_size_value": fund_size_value,
        "fund_size_currency": fund_size_currency,
        "fund_size_asof": fund_size_asof,
        "fund_size_scope": fund_size_scope,
        "replication_method": replication_method,
        "hedged_flag": hedged_flag,
        "hedged_target": hedged_target,
    }


def parse_factsheet_pdf(pdf_bytes: bytes) -> dict[str, object]:
    text_meta = extract_pdf_text_with_fallback(pdf_bytes)
    text = str(text_meta.get("text") or "")
    lines = split_lines(text)

    fee, fee_line = extract_fee(text, lines)
    distribution, distribution_line = extract_distribution(text, lines)
    ucits, ucits_line = extract_ucits(text, lines)
    profile_metadata = extract_profile_metadata_from_factsheet(text, lines)

    return {
        "ter": fee,
        "use_of_income": distribution,
        "ucits_compliant": ucits,
        "benchmark_name": profile_metadata.get("benchmark_name"),
        "asset_class_hint": profile_metadata.get("asset_class_hint"),
        "domicile_country": profile_metadata.get("domicile_country"),
        "fund_size_value": profile_metadata.get("fund_size_value"),
        "fund_size_currency": profile_metadata.get("fund_size_currency"),
        "fund_size_asof": profile_metadata.get("fund_size_asof"),
        "fund_size_scope": profile_metadata.get("fund_size_scope"),
        "replication_method": profile_metadata.get("replication_method"),
        "hedged_flag": profile_metadata.get("hedged_flag"),
        "hedged_target": profile_metadata.get("hedged_target"),
        "matched_fee_line": fee_line,
        "matched_distribution_line": distribution_line,
        "matched_ucits_line": ucits_line,
        "extractor": text_meta.get("extractor"),
        "fallback_used": text_meta.get("fallback_used"),
        "page_count": text_meta.get("page_count"),
        "extract_errors": text_meta.get("errors") or [],
        "text_char_count": len(normalize_space(text)),
    }


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


def load_targets(conn: sqlite3.Connection, limit: int, venue: str) -> list[sqlite3.Row]:
    venues = venue_scope(venue)
    placeholders = ",".join("?" for _ in venues)
    profile_join = ""
    profile_filter = "AND icc.ongoing_charges IS NULL"
    profile_order = "i.isin"
    url_map_row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='instrument_url_map'"
    ).fetchone()
    url_priority = ""
    if url_map_row is not None:
        url_priority = """
            CASE WHEN EXISTS (
                SELECT 1
                FROM instrument_url_map url_map
                WHERE url_map.instrument_id = i.instrument_id
                  AND url_map.url_type = 'amundi_monthly_factsheet'
                  AND url_map.url IS NOT NULL
                  AND TRIM(url_map.url) <> ''
            ) THEN 0 ELSE 1 END,
        """
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='product_profile'"
    ).fetchone()
    if row is not None:
        profile_join = "LEFT JOIN product_profile p ON p.instrument_id = i.instrument_id"
        profile_filter = """
          AND (
              icc.ongoing_charges IS NULL
              OR p.instrument_id IS NULL
              OR p.benchmark_name IS NULL OR TRIM(p.benchmark_name) = ''
              OR p.asset_class_hint IS NULL OR TRIM(p.asset_class_hint) = ''
              OR p.domicile_country IS NULL OR TRIM(p.domicile_country) = ''
              OR p.fund_size_value IS NULL
              OR p.replication_method IS NULL OR TRIM(p.replication_method) = ''
              OR p.hedged_flag IS NULL
          )
        """
        profile_order = f"""
            {url_priority}
            CASE WHEN icc.ongoing_charges IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.fund_size_value IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.domicile_country IS NULL OR TRIM(p.domicile_country) = '' THEN 0 ELSE 1 END,
            CASE WHEN p.benchmark_name IS NULL OR TRIM(p.benchmark_name) = '' THEN 0 ELSE 1 END,
            CASE WHEN p.asset_class_hint IS NULL OR TRIM(p.asset_class_hint) = '' THEN 0 ELSE 1 END,
            CASE WHEN p.replication_method IS NULL OR TRIM(p.replication_method) = '' THEN 0 ELSE 1 END,
            CASE WHEN p.hedged_flag IS NULL THEN 0 ELSE 1 END,
            i.isin
        """
    elif url_priority:
        profile_order = f"""
            {url_priority}
            CASE WHEN icc.ongoing_charges IS NULL THEN 0 ELSE 1 END,
            i.isin
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
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND l.primary_flag = 1
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id
        {profile_join}
        WHERE i.universe_mvp_flag = 1
          AND l.venue_mic IN ({placeholders})
          {profile_filter}
          AND (
              UPPER(COALESCE(iss.normalized_name, '')) LIKE '%AMUNDI%'
              OR UPPER(i.instrument_name) LIKE '%AMUNDI%'
              OR UPPER(COALESCE(iss.normalized_name, '')) LIKE '%MULTI UNITS%'
              OR UPPER(i.instrument_name) LIKE '%LYXOR%'
          )
        ORDER BY
            {profile_order}
        LIMIT ?
    """
    params: list[object] = [*venues, limit]
    return conn.execute(sql, params).fetchall()


def upsert_instrument_url_map(conn: sqlite3.Connection, instrument_id: int, url: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_url_map(instrument_id, url_type, url)
        VALUES (?, 'amundi_monthly_factsheet', ?)
        ON CONFLICT(instrument_id, url_type) DO UPDATE SET
            url = excluded.url
        """,
        (instrument_id, url),
    )


def load_existing_factsheet_urls(conn: sqlite3.Connection, instrument_ids: list[int]) -> dict[int, str]:
    if not instrument_ids:
        return {}
    placeholders = ",".join("?" for _ in instrument_ids)
    rows = conn.execute(
        f"""
        SELECT instrument_id, url
        FROM instrument_url_map
        WHERE url_type = 'amundi_monthly_factsheet'
          AND instrument_id IN ({placeholders})
          AND url IS NOT NULL
          AND TRIM(url) <> ''
        """,
        instrument_ids,
    ).fetchall()
    return {int(row["instrument_id"]): str(row["url"]) for row in rows}


def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def maybe_update_product_profile(conn: sqlite3.Connection, instrument_id: int, use_of_income: Optional[str]) -> bool:
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
            AMUNDI_SOURCE,
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
        "source": AMUNDI_SOURCE,
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
        ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, 'amundi_factsheet_ok', ?)
        """,
        (instrument_id, asof_date, ter, json.dumps(raw_json, ensure_ascii=True)),
    )


def print_kpis(
    attempted: int,
    url_ok: int,
    downloaded: int,
    fee_parsed: int,
    distribution_parsed: int,
    filled: int,
) -> None:
    def pct(n: int, d: int) -> float:
        return (100.0 * n / d) if d else 0.0

    print("\n=== Stage 2.5 Amundi KPIs ===")
    print(f"attempted instruments: {attempted}")
    print(f"url_ok%: {pct(url_ok, attempted):.2f}% ({url_ok}/{attempted})")
    print(f"downloaded%: {pct(downloaded, attempted):.2f}% ({downloaded}/{attempted})")
    print(f"fee_parsed%: {pct(fee_parsed, attempted):.2f}% ({fee_parsed}/{attempted})")
    print(
        f"distribution_parsed%: {pct(distribution_parsed, attempted):.2f}% "
        f"({distribution_parsed}/{attempted})"
    )
    print(f"ongoing_charges_filled%: {pct(filled, attempted):.2f}% ({filled}/{attempted})")


def safe_print(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return value.encode("ascii", errors="ignore").decode("ascii")


def run_self_test(client: HttpClient, cache_dir: Path) -> bool:
    discovered = discover_monthly_factsheet_urls(client, [SELF_TEST_ISIN]).get(SELF_TEST_ISIN)
    candidates = build_factsheet_candidates(SELF_TEST_ISIN, discovered=discovered, known_url=None)
    url = candidates[0].url if candidates else build_factsheet_url(SELF_TEST_ISIN)
    probe = probe_pdf_url(client, url)
    print("=== Stage 2.5 Self-test ===")
    print(f"isin: {SELF_TEST_ISIN}")
    print(f"url: {url}")
    if discovered:
        print(
            "discovery: "
            f"context={discovered.context_country}/{discovered.user_profile} "
            f"language={discovered.language} record_date={discovered.record_date}"
        )
    print(
        f"probe: accepted={probe.accepted} method={probe.method} status={probe.status_code} "
        f"ctype={probe.content_type} reason={probe.reason}"
    )
    if not probe.accepted:
        return False

    dl = download_pdf(client, probe.final_url or url, cache_dir)
    print(
        f"download: success={dl.success} from_cache={dl.from_cache} status={dl.http_status} "
        f"ctype={dl.content_type} error={dl.error}"
    )
    if not dl.success or not dl.pdf_bytes:
        return False

    parsed = parse_factsheet_pdf(dl.pdf_bytes)
    print(f"fee parsed: {parsed.get('ter')}")
    print(f"distribution parsed: {parsed.get('use_of_income')}")
    print(f"ucits parsed: {parsed.get('ucits_compliant')}")
    print(f"matched fee line: {safe_print(parsed.get('matched_fee_line'))}")
    print(f"matched distribution line: {safe_print(parsed.get('matched_distribution_line'))}")
    print(f"matched ucits line: {safe_print(parsed.get('matched_ucits_line'))}")
    return parsed.get("ter") is not None and parsed.get("use_of_income") is not None


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    client = HttpClient(rate_limit=args.rate_limit, timeout=args.timeout, max_retries=args.max_retries)
    cache_dir = Path(args.cache_dir)

    if args.self_test:
        return 0 if run_self_test(client, cache_dir) else 1

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    asof_date = dt.date.today().isoformat()

    attempted = 0
    url_ok = 0
    downloaded = 0
    fee_parsed = 0
    distribution_parsed = 0
    filled = 0
    profile_updates = 0
    sample_rows: list[tuple[str, str, str, str, str, str]] = []

    try:
        ensure_tables_and_view(conn)
        conn.commit()
        targets = load_targets(conn, args.limit, args.venue)
        attempted = len(targets)
        existing_urls = load_existing_factsheet_urls(
            conn,
            [int(row["instrument_id"]) for row in targets],
        )
        discovered_urls = discover_monthly_factsheet_urls(client, [str(row["isin"]) for row in targets])
        log(f"Target Amundi subset loaded: {attempted} rows (venue={args.venue}, limit={args.limit})")
        log(f"Document API resolved factsheet URLs for {len(discovered_urls)}/{attempted} target instruments")
        if attempted == 0:
            print_kpis(0, 0, 0, 0, 0, 0)
            print("\nNo instruments eligible for Stage 2.5.")
            return 0

        for idx, row in enumerate(targets, start=1):
            instrument_id = int(row["instrument_id"])
            isin = str(row["isin"])
            ticker = str(row["ticker"] or "")
            discovered = discovered_urls.get(isin)
            factsheet_candidates = build_factsheet_candidates(
                isin,
                discovered=discovered,
                known_url=existing_urls.get(instrument_id),
            )
            debug: dict[str, object] = {
                "parser_version": PARSER_VERSION,
                "instrument_id": instrument_id,
                "isin": isin,
                "factsheet_candidates": [
                    {"url": candidate.url, "source": candidate.source}
                    for candidate in factsheet_candidates
                ],
                "attempted_at": now_utc_iso(),
            }
            if discovered is not None:
                debug["discovery"] = {
                    "url": discovered.url,
                    "context_country": discovered.context_country,
                    "user_profile": discovered.user_profile,
                    "language": discovered.language,
                    "record_date": discovered.record_date,
                    "document_name": discovered.document_name,
                    "applied_alias": discovered.applied_alias,
                }

            probe: Optional[ProbeResult] = None
            selected_candidate: Optional[FactsheetCandidate] = None
            probe_attempts: list[dict[str, object]] = []
            for candidate in factsheet_candidates:
                candidate_probe = probe_pdf_url(client, candidate.url)
                probe_attempts.append(
                    {
                        "candidate_url": candidate.url,
                        "candidate_source": candidate.source,
                        "accepted": candidate_probe.accepted,
                        "reason": candidate_probe.reason,
                        "status_code": candidate_probe.status_code,
                        "content_type": candidate_probe.content_type,
                        "final_url": candidate_probe.final_url,
                        "method": candidate_probe.method,
                        "first_bytes_hex": candidate_probe.first_bytes_hex,
                    }
                )
                if candidate_probe.accepted:
                    probe = candidate_probe
                    selected_candidate = candidate
                    break
            debug["probe_attempts"] = probe_attempts
            final_url: Optional[str] = None
            cost_source_url: Optional[str] = None
            ter: Optional[float] = None
            use_of_income: Optional[str] = None
            ucits_compliant: Optional[int] = None
            quality = "download_fail"
            instrument_url_ok = False
            instrument_downloaded = False
            merged_parse: dict[str, object] = {
                "ter": None,
                "benchmark_name": None,
                "asset_class_hint": None,
                "domicile_country": None,
                "fund_size_value": None,
                "fund_size_currency": None,
                "fund_size_asof": None,
                "fund_size_scope": None,
                "replication_method": None,
                "hedged_flag": None,
                "hedged_target": None,
            }

            if probe is not None and selected_candidate is not None:
                instrument_url_ok = True
                debug["selected_candidate"] = {
                    "url": selected_candidate.url,
                    "source": selected_candidate.source,
                }
                final_url = probe.final_url or selected_candidate.url
                cost_source_url = final_url

                dl = download_pdf(client, final_url, cache_dir)
                debug["download"] = {
                    "success": dl.success,
                    "error": dl.error,
                    "final_url": dl.final_url,
                    "from_cache": dl.from_cache,
                    "status": dl.http_status,
                    "content_type": dl.content_type,
                    "cache_path": str(dl.cache_path) if dl.cache_path else None,
                }
                if dl.success and dl.pdf_bytes:
                    instrument_downloaded = True
                    parsed = parse_factsheet_pdf(dl.pdf_bytes)
                    ter = parsed.get("ter") if isinstance(parsed.get("ter"), (int, float)) else None
                    use_of_income = (
                        str(parsed.get("use_of_income")) if parsed.get("use_of_income") is not None else None
                    )
                    ucits_compliant = (
                        int(parsed.get("ucits_compliant"))
                        if parsed.get("ucits_compliant") is not None
                        else None
                    )
                    merged_parse = {
                        "ter": ter,
                        "use_of_income": use_of_income,
                        "ucits_compliant": ucits_compliant,
                        "benchmark_name": parsed.get("benchmark_name"),
                        "asset_class_hint": parsed.get("asset_class_hint"),
                        "domicile_country": parsed.get("domicile_country"),
                        "fund_size_value": parsed.get("fund_size_value"),
                        "fund_size_currency": parsed.get("fund_size_currency"),
                        "fund_size_asof": parsed.get("fund_size_asof"),
                        "fund_size_scope": parsed.get("fund_size_scope"),
                        "replication_method": parsed.get("replication_method"),
                        "hedged_flag": parsed.get("hedged_flag"),
                        "hedged_target": parsed.get("hedged_target"),
                        "matched_fee_line": parsed.get("matched_fee_line"),
                        "matched_distribution_line": parsed.get("matched_distribution_line"),
                        "matched_ucits_line": parsed.get("matched_ucits_line"),
                        "extractor": parsed.get("extractor"),
                        "fallback_used": parsed.get("fallback_used"),
                        "page_count": parsed.get("page_count"),
                        "extract_errors": parsed.get("extract_errors"),
                        "text_char_count": parsed.get("text_char_count"),
                    }
                    debug["parse"] = merged_parse

            if ter is None:
                kid_fallback = try_amundi_kid_fallback(client, cache_dir, isin)
                debug["kid_fallback"] = {
                    "success": kid_fallback.success,
                    "source_url": kid_fallback.source_url,
                    "attempts": kid_fallback.attempts,
                }
                if kid_fallback.success and kid_fallback.parsed:
                    instrument_url_ok = True
                    instrument_downloaded = True
                    if final_url is None:
                        final_url = kid_fallback.source_url
                    merged_parse = merge_profile_metadata(merged_parse, kid_fallback.parsed, ter_field="ongoing_charges")
                    if ter is None and merged_parse.get("ter") is not None:
                        ter = float(merged_parse["ter"])
                        cost_source_url = kid_fallback.source_url
                    if ucits_compliant is None and "UCITS" in (row["instrument_name"] or "").upper():
                        ucits_compliant = 1
                    if "selected_candidate" not in debug:
                        debug["selected_candidate"] = {
                            "url": kid_fallback.source_url,
                            "source": "amundi_kid_fallback",
                        }
                    debug["selected_cost_source"] = {
                        "url": cost_source_url or final_url,
                        "source": "amundi_kid_fallback" if cost_source_url == kid_fallback.source_url else "factsheet",
                    }

            debug["parse"] = {
                **merged_parse,
                "use_of_income": use_of_income,
                "ucits_compliant": ucits_compliant,
            }

            if instrument_url_ok:
                url_ok += 1
            if instrument_downloaded:
                downloaded += 1
            if ter is not None:
                fee_parsed += 1
            if use_of_income is not None:
                distribution_parsed += 1

            if not instrument_downloaded or final_url is None:
                conn.execute("BEGIN")
                if final_url is not None:
                    upsert_instrument_url_map(conn, instrument_id, final_url)
                insert_issuer_metadata_snapshot(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    source_url=final_url or (factsheet_candidates[0].url if factsheet_candidates else build_factsheet_url(isin)),
                    ter=None,
                    use_of_income=None,
                    ucits_compliant=None,
                    quality_flag="download_fail",
                    raw_json=debug,
                )
                conn.commit()
                if idx % 25 == 0:
                    log(f"Processed {idx}/{attempted}: download_fail")
                continue

            conn.execute("BEGIN")
            upsert_instrument_url_map(conn, instrument_id, final_url)
            quality = "ok" if ter is not None else "parse_fail"
            if ter is not None:
                insert_cost_snapshot_from_ter(
                    conn,
                    instrument_id=instrument_id,
                    asof_date=asof_date,
                    ter=float(ter),
                    source_url=cost_source_url or final_url,
                    use_of_income=use_of_income,
                    ucits_compliant=ucits_compliant,
                    profile_metadata={
                        "benchmark_name": merged_parse.get("benchmark_name"),
                        "asset_class_hint": merged_parse.get("asset_class_hint"),
                        "domicile_country": merged_parse.get("domicile_country"),
                        "fund_size_value": merged_parse.get("fund_size_value"),
                        "fund_size_currency": merged_parse.get("fund_size_currency"),
                        "fund_size_asof": merged_parse.get("fund_size_asof"),
                        "fund_size_scope": merged_parse.get("fund_size_scope"),
                        "replication_method": merged_parse.get("replication_method"),
                        "hedged_flag": merged_parse.get("hedged_flag"),
                        "hedged_target": merged_parse.get("hedged_target"),
                    },
                )
                filled += 1
                if len(sample_rows) < 20:
                    sample_rows.append(
                        (
                            isin,
                            ticker,
                            cost_source_url or final_url,
                            f"{float(ter):.4f}",
                            use_of_income or "NULL",
                            str(ucits_compliant) if ucits_compliant is not None else "NULL",
                        )
                    )

            if maybe_update_product_profile(conn, instrument_id, use_of_income):
                profile_updates += 1

            insert_issuer_metadata_snapshot(
                conn,
                instrument_id=instrument_id,
                asof_date=asof_date,
                source_url=cost_source_url or final_url,
                ter=float(ter) if ter is not None else None,
                use_of_income=use_of_income,
                ucits_compliant=ucits_compliant,
                quality_flag=quality,
                raw_json=debug,
            )
            conn.commit()

            if idx % 25 == 0:
                log(
                    f"Processed {idx}/{attempted}: url_ok={url_ok}, downloaded={downloaded}, "
                    f"fee_parsed={fee_parsed}, distribution_parsed={distribution_parsed}, filled={filled}"
                )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print_kpis(attempted, url_ok, downloaded, fee_parsed, distribution_parsed, filled)
    print(f"\nproduct_profile distribution_policy updates: {profile_updates}")
    print("\n=== Sample 20 (ISIN, ticker, factsheet_url, TER, distribution, ucits) ===")
    if not sample_rows:
        print("No successful fee parses.")
    else:
        for sample in sample_rows:
            print(" | ".join(sample))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
