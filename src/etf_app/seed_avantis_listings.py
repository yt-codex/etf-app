#!/usr/bin/env python3
"""Optional Stage 1.3 seeding for Avantis UCITS instruments from KID PDFs."""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import sys
from pathlib import Path

from etf_app.avantis_kid_enrich import (
    AVANTIS_ISSUER_NORMALIZED,
    HttpClient,
    discover_fund_pages,
    download_pdf_with_cache,
    ensure_column,
    ensure_schema,
    extract_isins_from_text,
    extract_kid_url_from_fund_page,
    extract_pdf_text_with_fallback,
    normalize_space,
    upsert_avantis_issuer,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 1.3 optional Avantis seed")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--cache-dir", default="kid_cache/avantis", help="PDF cache directory")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--rate-limit", type=float, default=0.2, help="HTTP delay seconds")
    parser.add_argument("--max-retries", type=int, default=1, help="HTTP retry count")
    parser.add_argument("--limit", type=int, default=0, help="Optional max funds to process (0=all)")
    return parser.parse_args(argv)


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def derive_name_from_fund_url(fund_url: str) -> str:
    path = fund_url.strip("/").split("/")
    slug = path[-1] if path else ""
    if slug in {"ucitsetf", ""} and len(path) >= 2:
        slug = path[-2]
    name = slug.replace("-", " ").strip()
    return " ".join(word.capitalize() for word in name.split()) or "Avantis UCITS ETF"


def has_listing(conn: sqlite3.Connection, instrument_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM listing WHERE instrument_id = ? LIMIT 1", (instrument_id,)).fetchone()
    return row is not None


def upsert_instrument_by_isin(
    conn: sqlite3.Connection,
    *,
    isin: str,
    instrument_name: str,
    issuer_id: int,
) -> tuple[int, bool]:
    row = conn.execute("SELECT instrument_id FROM instrument WHERE isin = ? LIMIT 1", (isin,)).fetchone()
    if row:
        instrument_id = int(row["instrument_id"])
        conn.execute(
            """
            UPDATE instrument
            SET instrument_name = COALESCE(instrument_name, ?),
                ucits_flag = COALESCE(ucits_flag, 1),
                issuer_id = COALESCE(issuer_id, ?),
                issuer_source = COALESCE(issuer_source, 'kid'),
                updated_at = ?
            WHERE instrument_id = ?
            """,
            (instrument_name, issuer_id, now_utc_iso(), instrument_id),
        )
        return instrument_id, False

    ts = now_utc_iso()
    conn.execute(
        """
        INSERT INTO instrument(
            isin, instrument_name, ucits_flag, issuer_id, issuer_source, status, created_at, updated_at
        ) VALUES (?, ?, 1, ?, 'kid', 'active', ?, ?)
        """,
        (isin, instrument_name, issuer_id, ts, ts),
    )
    instrument_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    return instrument_id, True


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    http = HttpClient(rate_limit=args.rate_limit, timeout=args.timeout, max_retries=args.max_retries)
    cache_dir = Path(args.cache_dir)

    funds_discovered = 0
    kids_downloaded = 0
    isins_parsed = 0
    instrument_inserted = 0
    listing_missing = 0
    sample_created: list[str] = []

    try:
        conn.execute("BEGIN")
        ensure_schema(conn)
        ensure_column(conn, "instrument", "issuer_source", "TEXT")

        funds, debug = discover_fund_pages(http, "https://www.avantisinvestors.com/ucitsetf/")
        if args.limit and args.limit > 0:
            funds = funds[: args.limit]
        funds_discovered = len(funds)
        if not funds:
            print("No Avantis funds discovered.")
            print(f"discovery_debug: {str(debug)[:800]}")
            conn.commit()
            return 0

        issuer_id = upsert_avantis_issuer(conn)
        for fund_url in funds:
            page = http.get(fund_url)
            if page.status_code != 200:
                continue
            kid_url = extract_kid_url_from_fund_page(page.text or "")
            if not kid_url:
                continue
            dl = download_pdf_with_cache(http, kid_url, cache_dir)
            if not dl.success or not dl.pdf_bytes:
                continue
            kids_downloaded += 1

            text_meta = extract_pdf_text_with_fallback(dl.pdf_bytes)
            text = str(text_meta.get("text") or "")
            isins = extract_isins_from_text(text)
            if not isins:
                continue

            name = derive_name_from_fund_url(fund_url)
            for isin in isins:
                isins_parsed += 1
                instrument_id, inserted = upsert_instrument_by_isin(
                    conn,
                    isin=isin,
                    instrument_name=name,
                    issuer_id=issuer_id,
                )
                if inserted:
                    instrument_inserted += 1
                    if len(sample_created) < 10:
                        sample_created.append(isin)
                if not has_listing(conn, instrument_id):
                    # listing.venue_mic/ticker are NOT NULL in this schema, so we only report missing listings.
                    listing_missing += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\n=== Stage 1.3 Optional Avantis Seed KPIs ===")
    print(f"issuer_normalized_target: {AVANTIS_ISSUER_NORMALIZED}")
    print(f"funds_discovered: {funds_discovered}")
    print(f"kids_downloaded: {kids_downloaded}")
    print(f"isins_parsed: {isins_parsed}")
    print(f"instrument_inserted: {instrument_inserted}")
    print(f"instruments_without_listing: {listing_missing}")
    print("\nSample inserted ISINs (up to 10):")
    if not sample_created:
        print("None")
    else:
        for isin in sample_created:
            print(isin)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
