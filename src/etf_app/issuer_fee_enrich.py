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
    source_url: str
    source_name: str


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
)


def normalize_source_keys(values: list[str]) -> list[str]:
    if not values:
        return [source.key for source in SUPPORTED_SOURCES]
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = normalize_space(value).lower()
        if not token or token in seen:
            continue
        out.append(token)
        seen.add(token)
    return out


def select_missing_fee_targets(conn: sqlite3.Connection, issuer_names: tuple[str, ...]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in issuer_names)
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
          AND icc.ongoing_charges IS NULL
          AND UPPER(COALESCE(iss.normalized_name, u.issuer_normalized, '')) IN ({placeholders})
        ORDER BY u.isin
    """
    return conn.execute(sql, tuple(name.upper() for name in issuer_names)).fetchall()


def download_pdf(url: str, timeout: int = 30) -> bytes:
    response = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
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
        ongoing_charges, snippet = find_fee_after_isin(pdf_text, isin)
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


def run_issuer_fee_backfill(db_path: str, source_keys: list[str] | None = None) -> dict[str, dict[str, int]]:
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
            rows = select_missing_fee_targets(conn, source.issuer_names)
            if not rows:
                results[source.key] = {"attempted": 0, "matched": 0, "inserted": 0}
                continue
            pdf_bytes = download_pdf(source.source_url)
            pdf_text = extract_pdf_text(pdf_bytes)
            conn.execute("BEGIN")
            try:
                stats = apply_fee_map(conn, rows=rows, source=source, pdf_text=pdf_text, asof_date=asof_date)
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
        help="Optional source key filter; repeatable. Supported: spdr, jpmorgan",
    )
    args = parser.parse_args(argv)
    results = run_issuer_fee_backfill(args.db_path, args.source)
    for key, stats in results.items():
        print(
            f"{key}: attempted={stats['attempted']} matched={stats['matched']} inserted={stats['inserted']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
