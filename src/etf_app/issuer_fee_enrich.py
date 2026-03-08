from __future__ import annotations

import datetime as dt
import io
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from pypdf import PdfReader

from etf_app.kid_ingest import apply_migrations, insert_cost_snapshot, normalize_space


@dataclass(frozen=True)
class IssuerFeeSource:
    key: str
    issuer_names: tuple[str, ...]
    source_url: Optional[str]
    source_name: str
    source_url_template: Optional[str] = None
    source_urls: tuple[str, ...] = ()
    request_headers: Optional[dict[str, str]] = None
    match_window: int = 220


SUPPORTED_SOURCES: tuple[IssuerFeeSource, ...] = (
    IssuerFeeSource(
        key="spdr",
        issuer_names=("State Street / SPDR", "State Street SPDR"),
        source_url=(
            "https://www.ssga.com/library-content/products/fund-docs/mf/emea/cost-disclosure/"
            "cost-disclosure-emea-en_gb-ie00bkmdy376.pdf"
        ),
        source_name="ssga_cost_disclosure_pdf",
    ),
    IssuerFeeSource(
        key="jpmorgan",
        issuer_names=("JPMorgan",),
        source_url=(
            "https://am.jpmorgan.com/content/dam/jpm-am-aem/emea/regional/en/regulatory/product-list/"
            "etf-product-list-emea.pdf"
        ),
        source_name="jpm_etf_product_list_pdf",
    ),
    IssuerFeeSource(
        key="invesco",
        issuer_names=("Invesco",),
        source_url=None,
        source_name="invesco_factsheet_pdf",
        source_url_template=(
            "https://www.invesco.com/content/dam/invesco/emea/en/product-documents/etf/share-class/factsheet/"
            "{isin_upper}_factsheet_en.pdf"
        ),
        request_headers={},
    ),
    IssuerFeeSource(
        key="vaneck",
        issuer_names=("VanEck",),
        source_url=(
            "https://www.vaneck.com/globalassets/home/media/managedassets/etf-europe/library/uploads/"
            "vaneck-product-list-uk.pdf"
        ),
        source_name="vaneck_product_list_pdf",
    ),
    IssuerFeeSource(
        key="bnpparibas",
        issuer_names=("BNP Paribas",),
        source_url=None,
        source_name="bnpp_docfinder_etf_range_pdf",
        source_urls=(
            "https://docfinder.bnpparibas-am.com/api/files/2e52fd81-49f2-4479-a6c2-3f2b36521aea",
            "https://docfinder.bnpparibas-am.com/api/files/488fd84c-3455-47a8-b2a4-55ffebdebcd8",
        ),
        match_window=600,
    ),
)


def normalize_source_keys(values: list[str]) -> list[str]:
    if not values:
        return [source.key for source in SUPPORTED_SOURCES]
    aliases = {
        "bnp": "bnpparibas",
        "bnp-paribas": "bnpparibas",
    }
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = aliases.get(normalize_space(value).lower(), normalize_space(value).lower())
        if not token or token in seen:
            continue
        out.append(token)
        seen.add(token)
    return out


def select_missing_fee_targets(
    conn: sqlite3.Connection,
    issuer_names: tuple[str, ...],
    max_existing_fee: Optional[float] = None,
) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in issuer_names)
    fee_filter = "icc.ongoing_charges IS NULL"
    if max_existing_fee is not None:
        fee_filter = f"(icc.ongoing_charges IS NULL OR icc.ongoing_charges <= {float(max_existing_fee):.6f})"
    sql = f"""
        SELECT DISTINCT
            CAST(u.instrument_id AS INTEGER) AS instrument_id,
            u.isin,
            u.instrument_name,
            COALESCE(iss.normalized_name, u.issuer_normalized, '') AS issuer_normalized
        FROM universe_mvp u
        JOIN instrument i ON i.instrument_id = CAST(u.instrument_id AS INTEGER)
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id
        WHERE UPPER(COALESCE(u.instrument_type, '')) = 'ETF'
          AND {fee_filter}
          AND UPPER(COALESCE(iss.normalized_name, u.issuer_normalized, '')) IN ({placeholders})
        ORDER BY u.isin
    """
    return conn.execute(sql, tuple(name.upper() for name in issuer_names)).fetchall()


def download_pdf(
    url: str,
    timeout: int = 30,
    headers: Optional[dict[str, str]] = None,
) -> bytes:
    request_headers = (
        headers
        if headers is not None
        else {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    response = requests.get(
        url,
        timeout=timeout,
        headers=request_headers,
    )
    response.raise_for_status()
    content_type = (response.headers.get("content-type") or "").lower()
    if "pdf" not in content_type and not response.content.startswith(b"%PDF"):
        raise ValueError(f"Expected PDF content from {url}, got {content_type or 'unknown'}")
    return response.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return normalize_space(" ".join((page.extract_text() or "") for page in reader.pages))


def find_fee_after_isin(text: str, isin: str, window: int = 220) -> tuple[Optional[float], Optional[str]]:
    needle = normalize_space(isin).upper()
    haystack = text.upper()
    start = haystack.find(needle)
    if start < 0:
        return None, None
    snippet = text[start : start + window]
    for match in re.finditer(r"(\d+\.\d+)\s*%?", snippet):
        value = float(match.group(1))
        if 0.0 <= value <= 3.0:
            return value, snippet
    return None, snippet


def extract_invesco_factsheet_payload(text: str) -> dict[str, object]:
    normalized = normalize_space(text)

    def capture(pattern: str) -> Optional[str]:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            return None
        return normalize_space(match.group(1))

    fee_value: Optional[float] = None
    fee_match = re.search(r"\bOngoing charge\b(?:\s+\d+)?\s+(\d+\.\d+)\s*%", normalized, flags=re.IGNORECASE)
    if fee_match:
        fee_value = float(fee_match.group(1))

    benchmark_name = capture(r"\bIndex\s+(.+?)\s+Index currency\b")
    replication_text = capture(r"\bReplication method\s+(Physical|Synthetic)\b")
    replication_method = replication_text.lower() if replication_text else None
    domicile_country = capture(r"\bDomicile\s+(.+?)\s+Dividend treatment\b")
    dividend_treatment = capture(r"\bDividend treatment\s+(Distributing|Accumulating)\b")
    hedged_text = capture(r"\bCurrency hedged\s+(Yes|No)\b")
    hedged_flag = None
    if hedged_text is not None:
        hedged_flag = 1 if hedged_text.lower() == "yes" else 0
    ucits_text = capture(r"\bUCITS compliant\s+(Yes|No)\b")
    ucits_compliant = None
    if ucits_text is not None:
        ucits_compliant = 1 if ucits_text.lower() == "yes" else 0

    return {
        "ongoing_charges": fee_value,
        "benchmark_name": benchmark_name,
        "replication_method": replication_method,
        "domicile_country": domicile_country,
        "distribution_policy": dividend_treatment,
        "hedged_flag": hedged_flag,
        "ucits_compliant": ucits_compliant,
    }


def apply_fee_map(
    conn: sqlite3.Connection,
    *,
    rows: list[sqlite3.Row],
    source: IssuerFeeSource,
    pdf_text: str,
    asof_date: str,
) -> dict[str, int]:
    stats = {"attempted": len(rows), "matched": 0, "inserted": 0}
    for row in rows:
        instrument_id = int(row["instrument_id"])
        isin = str(row["isin"] or "")
        ongoing_charges, snippet = find_fee_after_isin(pdf_text, isin, window=source.match_window)
        if ongoing_charges is None:
            continue
        stats["matched"] += 1
        raw_json = {
            "source": "issuer_fee_enrich",
            "issuer_source": source.source_name,
            "source_url": source.source_url,
            "matched_isin": isin,
            "instrument_name": row["instrument_name"],
            "ongoing_charges": ongoing_charges,
            "match_window": snippet,
        }
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=ongoing_charges,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=None,
            quality_flag="ok",
            raw_json=raw_json,
        )
        stats["inserted"] += 1
    return stats


def apply_multi_pdf_fee_map(
    conn: sqlite3.Connection,
    *,
    rows: list[sqlite3.Row],
    source: IssuerFeeSource,
    pdf_texts: list[tuple[str, str]],
    asof_date: str,
) -> dict[str, int]:
    stats = {"attempted": len(rows), "matched": 0, "inserted": 0}
    for row in rows:
        instrument_id = int(row["instrument_id"])
        isin = str(row["isin"] or "")
        matched_url: Optional[str] = None
        matched_snippet: Optional[str] = None
        ongoing_charges: Optional[float] = None
        for source_url, pdf_text in pdf_texts:
            ongoing_charges, snippet = find_fee_after_isin(pdf_text, isin, window=source.match_window)
            if ongoing_charges is None:
                continue
            matched_url = source_url
            matched_snippet = snippet
            break
        if ongoing_charges is None:
            continue
        stats["matched"] += 1
        raw_json = {
            "source": "issuer_fee_enrich",
            "issuer_source": source.source_name,
            "source_url": matched_url,
            "matched_isin": isin,
            "instrument_name": row["instrument_name"],
            "ongoing_charges": ongoing_charges,
            "match_window": matched_snippet,
        }
        insert_cost_snapshot(
            conn,
            instrument_id=instrument_id,
            asof_date=asof_date,
            ongoing_charges=ongoing_charges,
            entry_costs=None,
            exit_costs=None,
            transaction_costs=None,
            doc_id=None,
            quality_flag="ok",
            raw_json=raw_json,
        )
        stats["inserted"] += 1
    return stats


def apply_template_fee_source(
    conn: sqlite3.Connection,
    *,
    rows: list[sqlite3.Row],
    source: IssuerFeeSource,
    asof_date: str,
) -> dict[str, int]:
    if not source.source_url_template:
        raise ValueError(f"Source {source.key} does not define a template URL")
    stats = {"attempted": len(rows), "matched": 0, "inserted": 0}
    for row in rows:
        instrument_id = int(row["instrument_id"])
        isin = str(row["isin"] or "")
        source_url = source.source_url_template.format(isin_upper=isin.upper(), isin_lower=isin.lower())
        try:
            pdf_bytes = download_pdf(source_url, headers=source.request_headers)
        except Exception:
            continue
        payload = extract_invesco_factsheet_payload(extract_pdf_text(pdf_bytes))
        ongoing_charges = payload.get("ongoing_charges")
        if not isinstance(ongoing_charges, (int, float)):
            continue
        stats["matched"] += 1
        raw_json = {
            "source": "issuer_fee_enrich",
            "issuer_source": source.source_name,
            "source_url": source_url,
            "matched_isin": isin,
            "instrument_name": row["instrument_name"],
            "ongoing_charges": float(ongoing_charges),
            "profile_metadata": {
                key: value
                for key, value in payload.items()
                if key not in {"ongoing_charges", "distribution_policy", "ucits_compliant"} and value is not None
            },
            "distribution_policy": payload.get("distribution_policy"),
            "ucits_compliant": payload.get("ucits_compliant"),
        }
        conn.execute("BEGIN")
        try:
            insert_cost_snapshot(
                conn,
                instrument_id=instrument_id,
                asof_date=asof_date,
                ongoing_charges=float(ongoing_charges),
                entry_costs=None,
                exit_costs=None,
                transaction_costs=None,
                doc_id=None,
                quality_flag="ok",
                raw_json=raw_json,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        stats["inserted"] += 1
    return stats


def run_issuer_fee_backfill(
    db_path: str,
    source_keys: list[str] | None = None,
    max_existing_fee: Optional[float] = None,
) -> dict[str, dict[str, int]]:
    selected_keys = normalize_source_keys(source_keys or [])
    selected_sources = [source for source in SUPPORTED_SOURCES if source.key in selected_keys]
    unknown = sorted(set(selected_keys) - {source.key for source in SUPPORTED_SOURCES})
    if unknown:
        raise ValueError(f"Unsupported issuer fee sources: {', '.join(unknown)}")

    conn = sqlite3.connect(str(Path(db_path)))
    conn.row_factory = sqlite3.Row
    asof_date = dt.date.today().isoformat()
    results: dict[str, dict[str, int]] = {}
    try:
        conn.execute("BEGIN")
        apply_migrations(conn)
        conn.commit()
        for source in selected_sources:
            rows = select_missing_fee_targets(conn, source.issuer_names, max_existing_fee=max_existing_fee)
            if not rows:
                results[source.key] = {"attempted": 0, "matched": 0, "inserted": 0}
                continue
            if source.source_url_template:
                results[source.key] = apply_template_fee_source(
                    conn,
                    rows=rows,
                    source=source,
                    asof_date=asof_date,
                )
                continue
            pdf_texts: list[tuple[str, str]] = []
            source_urls = list(source.source_urls)
            if source.source_url:
                source_urls.insert(0, source.source_url)
            for source_url in source_urls:
                pdf_bytes = download_pdf(source_url, headers=source.request_headers)
                pdf_texts.append((source_url, extract_pdf_text(pdf_bytes)))
            conn.execute("BEGIN")
            try:
                if len(pdf_texts) == 1:
                    stats = apply_fee_map(
                        conn,
                        rows=rows,
                        source=source,
                        pdf_text=pdf_texts[0][1],
                        asof_date=asof_date,
                    )
                else:
                    stats = apply_multi_pdf_fee_map(
                        conn,
                        rows=rows,
                        source=source,
                        pdf_texts=pdf_texts,
                        asof_date=asof_date,
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            results[source.key] = stats
        return results
    finally:
        conn.close()


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Backfill issuer fees from official multi-fund PDFs")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Optional source key filter; repeatable. Supported: spdr, jpmorgan, invesco, vaneck, bnpparibas",
    )
    parser.add_argument(
        "--max-existing-fee",
        type=float,
        default=None,
        help="Also reprocess rows whose latest TER is less than or equal to this threshold.",
    )
    args = parser.parse_args(argv)
    results = run_issuer_fee_backfill(args.db_path, args.source, max_existing_fee=args.max_existing_fee)
    for key, stats in results.items():
        print(
            f"{key}: attempted={stats['attempted']} matched={stats['matched']} inserted={stats['inserted']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
