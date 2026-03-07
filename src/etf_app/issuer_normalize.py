from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from etf_app.kid_ingest import (
    apply_migrations,
    extract_domain,
    map_domain_to_issuer,
    normalize_space,
    upsert_issuer_by_normalized,
)


@dataclass(frozen=True)
class IssuerMatch:
    issuer_name: str
    domain: Optional[str]
    source: str


NAME_RULES: tuple[tuple[re.Pattern[str], str, Optional[str]], ...] = (
    (re.compile(r"^EXPAT\b", flags=re.IGNORECASE), "EXPAT ASSET MANAGEMENT EAD", None),
    (
        re.compile(r"^(?:ISH|IS\.|IS[0-9]|ISIV|ISV-|IS )", flags=re.IGNORECASE),
        "BlackRock / iShares",
        "ishares.com",
    ),
    (re.compile(r"^JPM(?:\b|[- ])", flags=re.IGNORECASE), "JPMorgan", "am.jpmorgan.com"),
    (re.compile(r"^UBS", flags=re.IGNORECASE), "UBS", "ubs.com"),
    (
        re.compile(
            r"^(?:AIS|AM-SP|AAIS|AEOGO|MUL-AM|MUL AMUN|AM\.|AMU\.|AME-|MUF-AM)",
            flags=re.IGNORECASE,
        ),
        "Amundi",
        "amundietf.com",
    ),
    (
        re.compile(r"^(?:BNP|BNPP|BNPE|BNPPE|BPE|BPEI|BPPE|B\.P\.E|B\.E\.I|PEP-|PFIE)", flags=re.IGNORECASE),
        "BNP Paribas",
        "assetmanagement.bnpparibas.com",
    ),
    (re.compile(r"^OSS", flags=re.IGNORECASE), "OSSIAM", "ossiam.com"),
    (re.compile(r"^(?:DEKA|DK )", flags=re.IGNORECASE), "Deka", "deka-etf.de"),
    (re.compile(r"^FTGF", flags=re.IGNORECASE), "Franklin Templeton", "franklintempleton.com"),
    (re.compile(r"^(?:VAN\.|VANGU)", flags=re.IGNORECASE), "Vanguard", "vanguard.co.uk"),
    (re.compile(r"^JH-", flags=re.IGNORECASE), "Janus Henderson", "janushenderson.com"),
)


def infer_issuer_from_name(instrument_name: Optional[str]) -> Optional[IssuerMatch]:
    text = normalize_space(instrument_name)
    if not text:
        return None
    for pattern, issuer_name, domain in NAME_RULES:
        if pattern.search(text):
            return IssuerMatch(issuer_name=issuer_name, domain=domain, source="issuer_normalize_name")
    return None


def infer_issuer_from_urls(*urls: Optional[str]) -> Optional[IssuerMatch]:
    for url in urls:
        if not url:
            continue
        host = extract_domain(url)
        issuer_name = map_domain_to_issuer(host)
        if issuer_name:
            return IssuerMatch(issuer_name=issuer_name, domain=host, source="issuer_normalize_domain")
    return None


def load_candidates(conn: sqlite3.Connection, only_missing_fees: bool) -> list[sqlite3.Row]:
    fee_join = ""
    fee_filter = ""
    if only_missing_fees:
        fee_join = "LEFT JOIN instrument_cost_current icc ON icc.instrument_id = i.instrument_id"
        fee_filter = "AND icc.ongoing_charges IS NULL"
    sql = f"""
        SELECT
            i.instrument_id,
            i.issuer_id,
            i.issuer_source,
            u.isin,
            u.instrument_name,
            COALESCE(iss.normalized_name, u.issuer_normalized, '') AS current_issuer_name,
            d.url AS latest_document_url,
            m.url AS mapped_url
        FROM instrument i
        JOIN universe_mvp u ON u.instrument_id = CAST(i.instrument_id AS TEXT)
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN (
            SELECT d1.instrument_id, d1.url
            FROM document d1
            JOIN (
                SELECT instrument_id, MAX(document_id) AS max_document_id
                FROM document
                GROUP BY instrument_id
            ) latest ON latest.max_document_id = d1.document_id
        ) d ON d.instrument_id = i.instrument_id
        LEFT JOIN (
            SELECT instrument_id, MIN(url) AS url
            FROM instrument_url_map
            GROUP BY instrument_id
        ) m ON m.instrument_id = i.instrument_id
        {fee_join}
        WHERE i.universe_mvp_flag = 1
          AND (
              i.issuer_id IS NULL
              OR TRIM(COALESCE(iss.normalized_name, u.issuer_normalized, '')) = ''
              OR COALESCE(iss.normalized_name, u.issuer_normalized, 'Unknown') = 'Unknown'
          )
          {fee_filter}
        ORDER BY u.isin
    """
    return conn.execute(sql).fetchall()


def normalize_unknown_issuers(conn: sqlite3.Connection, only_missing_fees: bool = False) -> dict[str, object]:
    apply_migrations(conn)
    candidates = load_candidates(conn, only_missing_fees)
    updated = 0
    source_counts: dict[str, int] = {}
    issuer_counts: dict[str, int] = {}
    for row in candidates:
        match = infer_issuer_from_urls(row["latest_document_url"], row["mapped_url"])
        if match is None:
            match = infer_issuer_from_name(row["instrument_name"])
        if match is None:
            continue
        issuer_id = upsert_issuer_by_normalized(conn, match.issuer_name, match.domain)
        conn.execute(
            "UPDATE instrument SET issuer_id = ?, issuer_source = ? WHERE instrument_id = ?",
            (issuer_id, match.source, int(row["instrument_id"])),
        )
        conn.execute(
            "UPDATE universe_mvp SET issuer_normalized = ? WHERE instrument_id = ?",
            (match.issuer_name, str(row["instrument_id"])),
        )
        updated += 1
        source_counts[match.source] = source_counts.get(match.source, 0) + 1
        issuer_counts[match.issuer_name] = issuer_counts.get(match.issuer_name, 0) + 1
    return {
        "candidates": len(candidates),
        "updated": updated,
        "source_counts": source_counts,
        "issuer_counts": dict(sorted(issuer_counts.items(), key=lambda item: (-item[1], item[0]))),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize missing ETF issuers using domain and name evidence")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument(
        "--only-missing-fees",
        action="store_true",
        help="Only normalize issuer rows that still lack ongoing_charges",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else sys.argv[1:])
    conn = sqlite3.connect(str(Path(args.db_path)))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        apply_migrations(conn)
        stats = normalize_unknown_issuers(conn, only_missing_fees=args.only_missing_fees)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(
        f"issuer normalization: candidates={stats['candidates']} updated={stats['updated']} "
        f"only_missing_fees={args.only_missing_fees}"
    )
    for issuer_name, count in list(stats["issuer_counts"].items())[:20]:
        print(f"  {issuer_name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
