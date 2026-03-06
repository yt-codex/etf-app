#!/usr/bin/env python3
"""
Stage 1.1 Universe hygiene on top of stage1_etf.db.

Applies:
- schema migration (non-destructive)
- instrument lightweight classification flags
- issuer alias + normalized issuer names
- deterministic single primary listing per instrument
- acceptance outputs + primary export CSV
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from etf_app.profile import refresh_product_profile


CONFIG = {
    "instrument_type_patterns": {
        "ETC": [r"\bETC\b", r"EXCHANGE\s+TRADED\s+COMMODITY"],
        "ETN": [r"\bETN\b", r"\bNOTES?\b"],
        # Conservative "clear non-ETF" guardrail.
        "OTHER": [r"\bWARRANTS?\b", r"\bCERTIFICATES?\b", r"\bCOMMON\s+STOCK\b"],
    },
    "leverage_patterns": [
        r"(\b[2-9]\s*x\b|\b[2-9]x\b|\bx[2-9]\b|leveraged|ultra|daily leveraged)",
    ],
    "inverse_patterns": [
        r"(\binverse\b|\bshort\b|bear|-\s*[1-9]\s*x\b|-?[1-9]x)",
    ],
    "venue_priority": ["XLON", "XETR", "CBOE_EU"],
    "currency_priority": ["USD", "EUR", "GBP"],
    "issuer_cleanup_suffixes": ["plc", "ltd", "limited", "sa", "ag", "inc", "co", "co.", "company"],
    "issuer_buckets": [
        {"canonical": "BlackRock / iShares", "domain": "ishares.com", "match_any": ["ISHARES", "BLACKROCK"]},
        {"canonical": "Vanguard", "domain": "vanguard.com", "match_any": ["VANGUARD"]},
        {"canonical": "Invesco", "domain": "invesco.com", "match_any": ["INVESCO"]},
        {
            "canonical": "State Street / SPDR",
            "domain": "ssga.com",
            "match_any": ["SSGA", "SPDR", "STATE STREET"],
        },
        {"canonical": "Lyxor", "domain": "amundietf.com", "match_any": ["LYXOR"]},
        {"canonical": "Amundi", "domain": "amundietf.com", "match_any": ["AMUNDI"]},
        {"canonical": "HSBC", "domain": "assetmanagement.hsbc.com", "match_any": ["HSBC"]},
        {"canonical": "UBS", "domain": "ubs.com", "match_any": ["UBS"]},
        {"canonical": "Xtrackers / DWS", "domain": "xtrackers.com", "match_any": ["XTRACKERS", "DB ETC"]},
        {"canonical": "WisdomTree", "domain": "wisdomtree.eu", "match_any": ["WISDOMTREE"]},
        {"canonical": "VanEck", "domain": "vaneck.com", "match_any": ["VANECK"]},
        {"canonical": "Legal & General", "domain": "lgim.com", "match_any": ["LEGAL & GENERAL"]},
        {"canonical": "JPMorgan", "domain": "am.jpmorgan.com", "match_any": ["JPMORGAN"]},
        {"canonical": "PIMCO", "domain": "pimco.com", "match_any": ["PIMCO"]},
        {
            "canonical": "Franklin Templeton",
            "domain": "franklintempleton.com",
            "match_any": ["FRANKLIN"],
        },
        {"canonical": "First Trust", "domain": "ftglobalportfolios.com", "match_any": ["FIRST TRUST"]},
        {"canonical": "BNP Paribas", "domain": "bnpparibas-am.com", "match_any": ["BNP"]},
        {"canonical": "Global X", "domain": "globalxetfs.com", "match_any": ["GLOBAL X"]},
        {"canonical": "Fidelity", "domain": "fidelity.com", "match_any": ["FIDELITY"]},
        {"canonical": "Goldman Sachs", "domain": "gsam.com", "match_any": ["GOLDMAN SACHS"]},
        {"canonical": "Leverage Shares", "domain": "leverageshares.com", "match_any": ["LEVERAGE SHARES"]},
        {"canonical": "GraniteShares", "domain": "graniteshares.com", "match_any": ["GRANITESHARES"]},
        {"canonical": "Tabula", "domain": "tabulaim.com", "match_any": ["TABULA"]},
    ],
}


@dataclass
class MigrationResult:
    added_columns: list[str]
    issuer_alias_created: bool


def log(message: str) -> None:
    print(f"[stage1.1] {message}")


def normalize_space(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    return text or None


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    ddl_type: str,
    default_sql: Optional[str] = None,
) -> bool:
    existing = table_columns(conn, table_name)
    if column_name in existing:
        return False
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}{default_clause}")
    return True


def apply_migration(conn: sqlite3.Connection) -> MigrationResult:
    added_columns: list[str] = []
    if ensure_column(conn, "instrument", "instrument_type", "TEXT"):
        added_columns.append("instrument.instrument_type")
    if ensure_column(conn, "instrument", "leverage_flag", "INTEGER"):
        added_columns.append("instrument.leverage_flag")
    if ensure_column(conn, "instrument", "inverse_flag", "INTEGER"):
        added_columns.append("instrument.inverse_flag")
    if ensure_column(conn, "instrument", "ucits_flag", "INTEGER"):
        added_columns.append("instrument.ucits_flag")
    if ensure_column(conn, "issuer", "normalized_name", "TEXT"):
        added_columns.append("issuer.normalized_name")
    if ensure_column(conn, "issuer", "domain", "TEXT"):
        added_columns.append("issuer.domain")
    if ensure_column(conn, "listing", "primary_flag", "INTEGER", default_sql="0"):
        added_columns.append("listing.primary_flag")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_alias(
            alias TEXT PRIMARY KEY,
            issuer_id TEXT NOT NULL,
            note TEXT NULL
        )
        """
    )
    return MigrationResult(added_columns=added_columns, issuer_alias_created=True)


def compile_patterns(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(p, flags=re.IGNORECASE) for p in patterns]


def classify_instrument_name(
    name: Optional[str],
    type_patterns: dict[str, list[re.Pattern[str]]],
    leverage_patterns: list[re.Pattern[str]],
    inverse_patterns: list[re.Pattern[str]],
) -> tuple[str, int, int]:
    text = normalize_space(name)
    if not text:
        return ("UNKNOWN", 0, 0)

    instrument_type = "ETF"
    if any(p.search(text) for p in type_patterns["ETC"]):
        instrument_type = "ETC"
    elif any(p.search(text) for p in type_patterns["ETN"]):
        instrument_type = "ETN"
    elif any(p.search(text) for p in type_patterns["OTHER"]):
        instrument_type = "OTHER"

    leverage_flag = 1 if any(p.search(text) for p in leverage_patterns) else 0
    inverse_flag = 1 if any(p.search(text) for p in inverse_patterns) else 0
    return (instrument_type, leverage_flag, inverse_flag)


def update_instrument_classification(conn: sqlite3.Connection) -> dict[str, int]:
    type_patterns = {
        key: compile_patterns(value)
        for key, value in CONFIG["instrument_type_patterns"].items()
    }
    leverage_patterns = compile_patterns(CONFIG["leverage_patterns"])
    inverse_patterns = compile_patterns(CONFIG["inverse_patterns"])

    rows = conn.execute(
        """
        SELECT instrument_id, instrument_name, instrument_type, leverage_flag, inverse_flag
        FROM instrument
        """
    ).fetchall()

    updates: list[tuple[str, int, int, int]] = []
    field_changes = {"instrument_type": 0, "leverage_flag": 0, "inverse_flag": 0}
    for row in rows:
        new_type, new_lev, new_inv = classify_instrument_name(
            row["instrument_name"], type_patterns, leverage_patterns, inverse_patterns
        )
        old_type = row["instrument_type"]
        old_lev = row["leverage_flag"]
        old_inv = row["inverse_flag"]

        changed = False
        if old_type != new_type:
            field_changes["instrument_type"] += 1
            changed = True
        if old_lev != new_lev:
            field_changes["leverage_flag"] += 1
            changed = True
        if old_inv != new_inv:
            field_changes["inverse_flag"] += 1
            changed = True
        if changed:
            updates.append((new_type, new_lev, new_inv, row["instrument_id"]))

    if updates:
        conn.executemany(
            """
            UPDATE instrument
            SET instrument_type = ?, leverage_flag = ?, inverse_flag = ?
            WHERE instrument_id = ?
            """,
            updates,
        )

    field_changes["instrument_rows_updated"] = len(updates)
    return field_changes


def cleanup_issuer_name(name: Optional[str]) -> Optional[str]:
    text = normalize_space(name)
    if not text:
        return None

    out = text.strip(" ,.;")
    suffixes = CONFIG["issuer_cleanup_suffixes"]
    suffix_re = re.compile(
        r"(?:\s+|,\s*)(?:" + "|".join(re.escape(s) for s in suffixes) + r")$",
        flags=re.IGNORECASE,
    )
    while True:
        candidate = suffix_re.sub("", out).strip(" ,.;")
        if candidate == out:
            break
        out = candidate
    return out or text


def bucket_for_issuer_name(issuer_name: str) -> Optional[dict]:
    upper_name = issuer_name.upper()
    for bucket in CONFIG["issuer_buckets"]:
        if any(token in upper_name for token in bucket["match_any"]):
            return bucket
    return None


def upsert_issuer_aliases(conn: sqlite3.Connection) -> dict[str, int]:
    issuer_rows = conn.execute("SELECT issuer_id, issuer_name FROM issuer").fetchall()
    by_bucket: dict[str, list[sqlite3.Row]] = {}
    bucket_meta: dict[str, dict] = {}

    for row in issuer_rows:
        bucket = bucket_for_issuer_name(row["issuer_name"])
        if not bucket:
            continue
        canonical = bucket["canonical"]
        by_bucket.setdefault(canonical, []).append(row)
        bucket_meta[canonical] = bucket

    intended: dict[str, tuple[str, str]] = {}
    for canonical, matches in by_bucket.items():
        anchor_id = min(match["issuer_id"] for match in matches)
        for match in matches:
            alias = match["issuer_name"]
            note = f"canonical={canonical}"
            intended[alias] = (str(anchor_id), note)

    existing = {
        row["alias"]: (row["issuer_id"], row["note"])
        for row in conn.execute("SELECT alias, issuer_id, note FROM issuer_alias")
    }

    inserted = 0
    updated = 0
    for alias, (issuer_id, note) in intended.items():
        prev = existing.get(alias)
        if prev is None:
            inserted += 1
        elif prev != (issuer_id, note):
            updated += 1
        conn.execute(
            """
            INSERT INTO issuer_alias(alias, issuer_id, note)
            VALUES(?, ?, ?)
            ON CONFLICT(alias) DO UPDATE SET
                issuer_id = excluded.issuer_id,
                note = excluded.note
            """,
            (alias, issuer_id, note),
        )

    return {
        "issuer_alias_targeted": len(intended),
        "issuer_alias_inserted": inserted,
        "issuer_alias_updated": updated,
    }


def canonical_from_note(note: Optional[str]) -> Optional[str]:
    if not note:
        return None
    if note.startswith("canonical="):
        return note.split("=", 1)[1].strip() or None
    return None


def normalize_issuers(conn: sqlite3.Connection) -> dict[str, int]:
    alias_rows = conn.execute("SELECT alias, issuer_id, note FROM issuer_alias").fetchall()
    alias_lookup = {row["alias"]: (row["issuer_id"], row["note"]) for row in alias_rows}
    canonical_domain = {
        bucket["canonical"]: bucket.get("domain")
        for bucket in CONFIG["issuer_buckets"]
        if bucket.get("domain")
    }

    issuer_rows = conn.execute("SELECT issuer_id, issuer_name, normalized_name, domain FROM issuer").fetchall()
    updates: list[tuple[Optional[str], Optional[str], int]] = []
    normalized_name_updates = 0
    domain_updates = 0

    for row in issuer_rows:
        issuer_name = row["issuer_name"]
        alias_rec = alias_lookup.get(issuer_name)
        if alias_rec:
            canonical = canonical_from_note(alias_rec[1])
            normalized = canonical or cleanup_issuer_name(issuer_name)
            domain = canonical_domain.get(canonical)
        else:
            normalized = cleanup_issuer_name(issuer_name)
            domain = None

        if row["normalized_name"] != normalized:
            normalized_name_updates += 1
        if row["domain"] != domain:
            domain_updates += 1

        if row["normalized_name"] != normalized or row["domain"] != domain:
            updates.append((normalized, domain, row["issuer_id"]))

    if updates:
        conn.executemany(
            """
            UPDATE issuer
            SET normalized_name = ?, domain = ?
            WHERE issuer_id = ?
            """,
            updates,
        )

    return {
        "issuer_rows_updated": len(updates),
        "normalized_name_updated": normalized_name_updates,
        "domain_updated": domain_updates,
    }


def _venue_rank(venue: Optional[str]) -> tuple[int, str]:
    venue_upper = (venue or "").upper()
    try:
        return (CONFIG["venue_priority"].index(venue_upper), venue_upper)
    except ValueError:
        return (len(CONFIG["venue_priority"]), venue_upper)


def _currency_rank(currency: Optional[str]) -> tuple[int, str]:
    curr_upper = (currency or "").upper()
    try:
        return (CONFIG["currency_priority"].index(curr_upper), curr_upper)
    except ValueError:
        return (len(CONFIG["currency_priority"]), curr_upper)


def choose_primary_listings(conn: sqlite3.Connection) -> dict[str, int]:
    reset_cur = conn.execute("UPDATE listing SET primary_flag = 0 WHERE primary_flag <> 0")
    reset_updates = reset_cur.rowcount

    listing_rows = conn.execute(
        """
        SELECT listing_id, instrument_id, venue_mic, ticker, trading_currency
        FROM listing
        ORDER BY instrument_id, listing_id
        """
    ).fetchall()

    grouped: dict[int, list[sqlite3.Row]] = {}
    for row in listing_rows:
        grouped.setdefault(row["instrument_id"], []).append(row)

    chosen_listing_ids: list[int] = []
    for _, rows in grouped.items():
        best = min(
            rows,
            key=lambda r: (
                _venue_rank(r["venue_mic"]),
                _currency_rank(r["trading_currency"]),
                (r["ticker"] or "").upper(),
                r["listing_id"],
            ),
        )
        chosen_listing_ids.append(best["listing_id"])

    if chosen_listing_ids:
        conn.executemany(
            "UPDATE listing SET primary_flag = 1 WHERE listing_id = ?",
            [(lid,) for lid in chosen_listing_ids],
        )

    zero_primary = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT instrument_id, SUM(COALESCE(primary_flag, 0)) AS primary_count
            FROM listing
            GROUP BY instrument_id
            HAVING primary_count = 0
        ) t
        """
    ).fetchone()[0]
    multi_primary = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT instrument_id, SUM(COALESCE(primary_flag, 0)) AS primary_count
            FROM listing
            GROUP BY instrument_id
            HAVING primary_count > 1
        ) t
        """
    ).fetchone()[0]

    if zero_primary > 0 or multi_primary > 0:
        raise RuntimeError(
            "Primary listing validation failed: "
            f"instruments_with_0_primary={zero_primary}, instruments_with_gt1_primary={multi_primary}"
        )

    return {
        "primary_reset_updated": reset_updates,
        "primary_set_to_1": len(chosen_listing_ids),
        "zero_primary": zero_primary,
        "multi_primary": multi_primary,
    }


def print_acceptance_outputs(conn: sqlite3.Connection) -> None:
    print("\n=== Stage 1.1 Counts ===")
    print("instruments by instrument_type:")
    for row in conn.execute(
        """
        SELECT COALESCE(instrument_type, 'NULL') AS instrument_type, COUNT(*) AS c
        FROM instrument
        GROUP BY COALESCE(instrument_type, 'NULL')
        ORDER BY c DESC, instrument_type
        """
    ):
        print(f"  {row['instrument_type']}: {row['c']}")

    lev_count = conn.execute(
        "SELECT COUNT(*) FROM instrument WHERE COALESCE(leverage_flag, 0) = 1"
    ).fetchone()[0]
    inv_count = conn.execute(
        "SELECT COUNT(*) FROM instrument WHERE COALESCE(inverse_flag, 0) = 1"
    ).fetchone()[0]
    print(f"# leverage_flag=1: {lev_count}")
    print(f"# inverse_flag=1: {inv_count}")

    zero_primary = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT instrument_id, SUM(COALESCE(primary_flag, 0)) AS primary_count
            FROM listing
            GROUP BY instrument_id
            HAVING primary_count = 0
        ) t
        """
    ).fetchone()[0]
    multi_primary = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT instrument_id, SUM(COALESCE(primary_flag, 0)) AS primary_count
            FROM listing
            GROUP BY instrument_id
            HAVING primary_count > 1
        ) t
        """
    ).fetchone()[0]
    print("\nPrimary listing sanity:")
    print(f"# instruments with 0 primary listings: {zero_primary}")
    print(f"# instruments with >1 primary listings: {multi_primary}")

    print("\n=== Sample (30) ===")
    sample_rows = conn.execute(
        """
        SELECT
            i.isin,
            i.instrument_name,
            i.instrument_type,
            iss.normalized_name AS normalized_issuer,
            l.venue_mic,
            l.ticker,
            l.trading_currency
        FROM instrument i
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN listing l
            ON l.instrument_id = i.instrument_id
           AND COALESCE(l.primary_flag, 0) = 1
        ORDER BY i.isin
        LIMIT 30
        """
    ).fetchall()
    for row in sample_rows:
        print(
            f"{row['isin']} | {row['instrument_name'] or ''} | {row['instrument_type'] or ''} | "
            f"{row['normalized_issuer'] or ''} | {row['venue_mic'] or ''} | "
            f"{row['ticker'] or ''} | {row['trading_currency'] or ''}"
        )


def export_primary_csv(conn: sqlite3.Connection, output_csv: Path) -> int:
    rows = conn.execute(
        """
        SELECT
            i.isin,
            i.instrument_name,
            i.instrument_type,
            iss.normalized_name AS normalized_issuer,
            l.venue_mic AS primary_venue_mic,
            l.ticker AS primary_ticker,
            l.trading_currency AS primary_currency
        FROM instrument i
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN listing l
            ON l.instrument_id = i.instrument_id
           AND COALESCE(l.primary_flag, 0) = 1
        ORDER BY i.isin
        """
    ).fetchall()

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "isin",
                "instrument_name",
                "instrument_type",
                "normalized_issuer",
                "primary_venue_mic",
                "primary_ticker",
                "primary_currency",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["isin"],
                    row["instrument_name"],
                    row["instrument_type"],
                    row["normalized_issuer"],
                    row["primary_venue_mic"],
                    row["primary_ticker"],
                    row["primary_currency"],
                ]
            )
    return len(rows)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Primary listing hygiene updater.")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to stage1 SQLite DB")
    parser.add_argument(
        "--output-csv",
        default="artifacts/primary_listings.csv",
        help="Primary listing export path",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)
    output_csv = Path(args.output_csv)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("BEGIN")
        migration = apply_migration(conn)
        log(
            "migration complete; added columns: "
            + (", ".join(migration.added_columns) if migration.added_columns else "none")
        )

        classify_stats = update_instrument_classification(conn)
        log(
            "instrument updates: "
            f"rows={classify_stats['instrument_rows_updated']}, "
            f"instrument_type={classify_stats['instrument_type']}, "
            f"leverage_flag={classify_stats['leverage_flag']}, "
            f"inverse_flag={classify_stats['inverse_flag']}"
        )

        alias_stats = upsert_issuer_aliases(conn)
        log(
            "issuer_alias upsert: "
            f"targeted={alias_stats['issuer_alias_targeted']}, "
            f"inserted={alias_stats['issuer_alias_inserted']}, "
            f"updated={alias_stats['issuer_alias_updated']}"
        )

        issuer_stats = normalize_issuers(conn)
        log(
            "issuer normalization: "
            f"rows={issuer_stats['issuer_rows_updated']}, "
            f"normalized_name={issuer_stats['normalized_name_updated']}, "
            f"domain={issuer_stats['domain_updated']}"
        )

        profile_stats = refresh_product_profile(conn)
        log(
            "product profile refresh: "
            f"profile_rows={profile_stats.product_profile_rows_upserted}, "
            f"ucits_from_snapshot={profile_stats.ucits_from_snapshot}, "
            f"ucits_from_name={profile_stats.ucits_from_name}, "
            f"instrument_ucits_updated={profile_stats.instruments_ucits_updated}, "
            f"distribution_synced={profile_stats.distributions_synced}, "
            f"costs_synced={profile_stats.costs_synced}"
        )

        primary_stats = choose_primary_listings(conn)
        log(
            "primary listing selection: "
            f"reset={primary_stats['primary_reset_updated']}, "
            f"set_primary={primary_stats['primary_set_to_1']}"
        )

        exported = export_primary_csv(conn, output_csv)
        log(f"exported CSV rows: {exported} -> {output_csv}")

        print_acceptance_outputs(conn)
        conn.commit()
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
