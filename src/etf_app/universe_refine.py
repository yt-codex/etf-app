#!/usr/bin/env python3
"""
Stage 1.2 refinements on top of stage1_etf.db + stage1.1 outputs.

Goals:
1) Repair missing primary listing currency conservatively.
2) Build deterministic MVP-supported universe subset.
3) Optional conservative issuer backfill for obvious issuer keywords.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sqlite3
import sys
from pathlib import Path
from typing import Optional

from etf_app.profile import refresh_product_profile


CONFIG = {
    "mvp_primary_venues": ["XLON", "XETR"],
    "mvp_primary_currencies": ["USD", "EUR", "GBP"],
    "currency_rank": ["USD", "EUR", "GBP"],
    "issuer_keyword_rules": [
        {"pattern": r"\bISHARES\b", "canonical": "BlackRock / iShares", "domain": "ishares.com"},
        {"pattern": r"\bVANGUARD\b", "canonical": "Vanguard", "domain": "vanguard.com"},
        {"pattern": r"\bSPDR\b", "canonical": "State Street / SPDR", "domain": "ssga.com"},
        {"pattern": r"\bINVESCO\b", "canonical": "Invesco", "domain": "invesco.com"},
        {"pattern": r"\bAMUNDI\b", "canonical": "Amundi", "domain": "amundietf.com"},
        {"pattern": r"\bXTRACKERS\b", "canonical": "Xtrackers / DWS", "domain": "xtrackers.com"},
        {"pattern": r"\bWISDOMTREE\b", "canonical": "WisdomTree", "domain": "wisdomtree.eu"},
        {"pattern": r"\bVANECK\b", "canonical": "VanEck", "domain": "vaneck.com"},
        {"pattern": r"\bHSBC\b", "canonical": "HSBC", "domain": "assetmanagement.hsbc.com"},
    ],
}


def log(message: str) -> None:
    print(f"[stage1.2] {message}")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    col_type: str,
    default_sql: Optional[str] = None,
) -> bool:
    if column_name in table_columns(conn, table_name):
        return False
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type}{default_clause}")
    return True


def apply_migration(conn: sqlite3.Connection) -> dict[str, object]:
    added = []
    if ensure_column(conn, "instrument", "universe_mvp_flag", "INTEGER", default_sql="0"):
        added.append("instrument.universe_mvp_flag")
    if ensure_column(conn, "instrument", "issuer_source", "TEXT"):
        added.append("instrument.issuer_source")
    if ensure_column(conn, "listing", "currency_quality", "TEXT"):
        added.append("listing.currency_quality")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_mvp(
            instrument_id TEXT PRIMARY KEY,
            isin TEXT,
            instrument_name TEXT,
            instrument_type TEXT,
            leverage_flag INTEGER,
            inverse_flag INTEGER,
            ucits_flag INTEGER,
            ucits_source TEXT,
            issuer_normalized TEXT,
            distribution_policy TEXT,
            primary_venue_mic TEXT,
            primary_ticker TEXT,
            primary_currency TEXT,
            ongoing_charges REAL,
            ongoing_charges_asof TEXT,
            updated_at TEXT
        )
        """
    )
    ensure_column(conn, "universe_mvp", "ucits_flag", "INTEGER")
    ensure_column(conn, "universe_mvp", "ucits_source", "TEXT")
    ensure_column(conn, "universe_mvp", "distribution_policy", "TEXT")
    ensure_column(conn, "universe_mvp", "ongoing_charges", "REAL")
    ensure_column(conn, "universe_mvp", "ongoing_charges_asof", "TEXT")

    return {
        "added_columns": added,
        "created_universe_mvp": True,
    }


def _currency_missing_sql() -> str:
    return "(trading_currency IS NULL OR TRIM(trading_currency) = '')"


def refine_currency(conn: sqlite3.Connection) -> dict[str, int]:
    before_null = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM listing
        WHERE primary_flag = 1 AND {_currency_missing_sql()}
        """
    ).fetchone()[0]

    # Existing non-missing primary listings are "ok" unless they were explicitly imputed before.
    ok_updated = conn.execute(
        """
        UPDATE listing
        SET currency_quality = 'ok'
        WHERE primary_flag = 1
          AND trading_currency IS NOT NULL
          AND TRIM(trading_currency) <> ''
          AND COALESCE(currency_quality, '') <> 'imputed'
        """
    ).rowcount

    primary_missing_rows = conn.execute(
        f"""
        SELECT listing_id, instrument_id, venue_mic
        FROM listing
        WHERE primary_flag = 1 AND {_currency_missing_sql()}
        ORDER BY instrument_id, listing_id
        """
    ).fetchall()

    imputed = 0
    still_missing = 0

    for row in primary_missing_rows:
        choice = conn.execute(
            """
            SELECT trading_currency, COUNT(*) AS cnt
            FROM listing
            WHERE instrument_id = ?
              AND venue_mic = ?
              AND listing_id <> ?
              AND trading_currency IS NOT NULL
              AND TRIM(trading_currency) <> ''
            GROUP BY trading_currency
            ORDER BY
              cnt DESC,
              CASE UPPER(trading_currency)
                  WHEN 'USD' THEN 0
                  WHEN 'EUR' THEN 1
                  WHEN 'GBP' THEN 2
                  ELSE 3
              END,
              UPPER(trading_currency) ASC
            LIMIT 1
            """,
            (row["instrument_id"], row["venue_mic"], row["listing_id"]),
        ).fetchone()

        if choice is not None:
            conn.execute(
                """
                UPDATE listing
                SET trading_currency = ?, currency_quality = 'imputed'
                WHERE listing_id = ?
                """,
                (choice["trading_currency"], row["listing_id"]),
            )
            imputed += 1
        else:
            conn.execute(
                "UPDATE listing SET currency_quality = 'missing' WHERE listing_id = ?",
                (row["listing_id"],),
            )
            still_missing += 1

    after_null = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM listing
        WHERE primary_flag = 1 AND {_currency_missing_sql()}
        """
    ).fetchone()[0]

    return {
        "before_null": before_null,
        "after_null": after_null,
        "imputed": imputed,
        "still_missing": still_missing,
        "ok_marked": ok_updated,
    }


def _match_issuer_keyword(instrument_name: Optional[str]) -> Optional[dict[str, str]]:
    if not instrument_name:
        return None
    name = instrument_name.upper()
    for rule in CONFIG["issuer_keyword_rules"]:
        if re.search(rule["pattern"], name):
            return rule
    return None


def _find_or_create_issuer(conn: sqlite3.Connection, canonical: str, domain: Optional[str]) -> int:
    row = conn.execute(
        """
        SELECT issuer_id
        FROM issuer
        WHERE normalized_name = ?
        ORDER BY issuer_id
        LIMIT 1
        """,
        (canonical,),
    ).fetchone()
    if row:
        issuer_id = int(row["issuer_id"])
        if domain:
            conn.execute(
                """
                UPDATE issuer
                SET domain = COALESCE(domain, ?), normalized_name = COALESCE(normalized_name, ?)
                WHERE issuer_id = ?
                """,
                (domain, canonical, issuer_id),
            )
        return issuer_id

    row = conn.execute(
        """
        SELECT issuer_id
        FROM issuer
        WHERE issuer_name = ?
        ORDER BY issuer_id
        LIMIT 1
        """,
        (canonical,),
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
            (canonical, domain, issuer_id),
        )
        return issuer_id

    created_at = now_utc_iso()
    cur = conn.execute(
        """
        INSERT INTO issuer(issuer_name, website, created_at, normalized_name, domain)
        VALUES (?, NULL, ?, ?, ?)
        """,
        (canonical, created_at, canonical, domain),
    )
    return int(cur.lastrowid)


def issuer_backfill(conn: sqlite3.Connection) -> dict[str, int]:
    stage1_tagged = conn.execute(
        """
        UPDATE instrument
        SET issuer_source = 'stage1'
        WHERE issuer_id IS NOT NULL AND issuer_source IS NULL
        """
    ).rowcount

    candidates = conn.execute(
        """
        SELECT instrument_id, instrument_name
        FROM instrument
        WHERE issuer_id IS NULL
        ORDER BY instrument_id
        """
    ).fetchall()

    filled = 0
    new_issuers_created = 0

    issuer_ids_before = {
        row["issuer_id"]
        for row in conn.execute("SELECT issuer_id FROM issuer")
    }

    for row in candidates:
        match = _match_issuer_keyword(row["instrument_name"])
        if not match:
            continue
        canonical = match["canonical"]
        domain = match.get("domain")
        issuer_id = _find_or_create_issuer(conn, canonical, domain)
        if issuer_id not in issuer_ids_before:
            issuer_ids_before.add(issuer_id)
            new_issuers_created += 1

        updated = conn.execute(
            """
            UPDATE instrument
            SET issuer_id = ?, issuer_source = 'name_heuristic'
            WHERE instrument_id = ? AND issuer_id IS NULL
            """,
            (issuer_id, row["instrument_id"]),
        ).rowcount
        if updated:
            filled += 1

    return {
        "issuer_source_stage1_tagged": stage1_tagged,
        "issuer_backfilled": filled,
        "new_issuers_created": new_issuers_created,
    }


def build_universe(conn: sqlite3.Connection) -> dict[str, int]:
    conn.execute("UPDATE instrument SET universe_mvp_flag = 0")

    placeholders_venues = ",".join("?" for _ in CONFIG["mvp_primary_venues"])
    placeholders_ccy = ",".join("?" for _ in CONFIG["mvp_primary_currencies"])

    eligible_ids = conn.execute(
        f"""
        SELECT i.instrument_id
        FROM instrument i
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        WHERE i.instrument_type = 'ETF'
          AND COALESCE(i.ucits_flag, 0) = 1
          AND COALESCE(i.leverage_flag, 0) = 0
          AND COALESCE(i.inverse_flag, 0) = 0
          AND l.venue_mic IN ({placeholders_venues})
          AND COALESCE(l.status, 'active') = 'active'
          AND (
                l.trading_currency IS NULL
                OR TRIM(l.trading_currency) = ''
                OR UPPER(l.trading_currency) IN ({placeholders_ccy})
              )
        """,
        tuple(CONFIG["mvp_primary_venues"] + CONFIG["mvp_primary_currencies"]),
    ).fetchall()

    if eligible_ids:
        conn.executemany(
            "UPDATE instrument SET universe_mvp_flag = 1 WHERE instrument_id = ?",
            [(row["instrument_id"],) for row in eligible_ids],
        )

    refreshed_at = now_utc_iso()
    conn.execute("DELETE FROM universe_mvp")
    inserted = conn.execute(
        """
        INSERT INTO universe_mvp(
            instrument_id,
            isin,
            instrument_name,
            instrument_type,
            leverage_flag,
            inverse_flag,
            ucits_flag,
            ucits_source,
            issuer_normalized,
            distribution_policy,
            primary_venue_mic,
            primary_ticker,
            primary_currency,
            ongoing_charges,
            ongoing_charges_asof,
            updated_at
        )
        SELECT
            CAST(i.instrument_id AS TEXT),
            i.isin,
            i.instrument_name,
            i.instrument_type,
            i.leverage_flag,
            i.inverse_flag,
            i.ucits_flag,
            i.ucits_source,
            iss.normalized_name,
            pp.distribution_policy,
            l.venue_mic,
            l.ticker,
            NULLIF(TRIM(l.trading_currency), ''),
            c.ongoing_charges,
            c.asof_date,
            ?
        FROM instrument i
        LEFT JOIN issuer iss
               ON iss.issuer_id = i.issuer_id
        LEFT JOIN product_profile pp
               ON pp.instrument_id = i.instrument_id
        JOIN listing l
             ON l.instrument_id = i.instrument_id
            AND COALESCE(l.primary_flag, 0) = 1
        LEFT JOIN instrument_cost_current c
               ON c.instrument_id = i.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
        ORDER BY i.isin
        """,
        (refreshed_at,),
    ).rowcount

    return {
        "universe_flagged": len(eligible_ids),
        "universe_rows_inserted": inserted,
    }


def export_csv(conn: sqlite3.Connection, output_path: Path) -> int:
    rows = conn.execute(
        """
        SELECT
            instrument_id,
            isin,
            instrument_name,
            instrument_type,
            leverage_flag,
            inverse_flag,
            ucits_flag,
            ucits_source,
            issuer_normalized,
            distribution_policy,
            primary_venue_mic,
            primary_ticker,
            primary_currency,
            ongoing_charges,
            ongoing_charges_asof,
            updated_at
        FROM universe_mvp
        ORDER BY isin
        """
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "instrument_id",
                "isin",
                "instrument_name",
                "instrument_type",
                "leverage_flag",
                "inverse_flag",
                "ucits_flag",
                "ucits_source",
                "issuer_normalized",
                "distribution_policy",
                "primary_venue_mic",
                "primary_ticker",
                "primary_currency",
                "ongoing_charges",
                "ongoing_charges_asof",
                "updated_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["instrument_id"],
                    row["isin"],
                    row["instrument_name"],
                    row["instrument_type"],
                    row["leverage_flag"],
                    row["inverse_flag"],
                    row["ucits_flag"],
                    row["ucits_source"],
                    row["issuer_normalized"],
                    row["distribution_policy"],
                    row["primary_venue_mic"],
                    row["primary_ticker"],
                    row["primary_currency"],
                    row["ongoing_charges"],
                    row["ongoing_charges_asof"],
                    row["updated_at"],
                ]
            )
    return len(rows)


def print_outputs(
    conn: sqlite3.Connection,
    currency_stats: dict[str, int],
    universe_stats: dict[str, int],
    issuer_stats: dict[str, int],
    export_rows: int,
    export_path: Path,
) -> None:
    print("\n=== Stage 1.2 Primary Currency ===")
    print(f"primary currency NULL before: {currency_stats['before_null']}")
    print(f"primary currency NULL after:  {currency_stats['after_null']}")
    print(f"imputed count:                {currency_stats['imputed']}")
    print(f"still missing count:          {currency_stats['still_missing']}")

    print("\n=== Stage 1.2 Universe ===")
    print(f"instruments universe_mvp_flag=1: {universe_stats['universe_flagged']}")
    ucits_counts = conn.execute(
        """
        SELECT COALESCE(ucits_flag, 'NULL') AS ucits_flag, COUNT(*) AS c
        FROM universe_mvp
        GROUP BY COALESCE(ucits_flag, 'NULL')
        ORDER BY c DESC
        """
    ).fetchall()
    print("breakdown by ucits_flag:")
    for row in ucits_counts:
        print(f"  {row['ucits_flag']}: {row['c']}")
    print("breakdown by primary_venue_mic and primary_currency:")
    for row in conn.execute(
        """
        SELECT
            primary_venue_mic,
            COALESCE(primary_currency, 'NULL') AS primary_currency,
            COUNT(*) AS c
        FROM universe_mvp
        GROUP BY primary_venue_mic, COALESCE(primary_currency, 'NULL')
        ORDER BY primary_venue_mic, primary_currency
        """
    ):
        print(f"  {row['primary_venue_mic']} | {row['primary_currency']} | {row['c']}")

    print("\n=== Stage 1.2 Issuer Backfill ===")
    print(f"instruments issuer_id filled by heuristic: {issuer_stats['issuer_backfilled']}")
    print("top 15 issuers in universe_mvp:")
    for row in conn.execute(
        """
        SELECT COALESCE(issuer_normalized, 'NULL') AS issuer_name, COUNT(*) AS c
        FROM universe_mvp
        GROUP BY COALESCE(issuer_normalized, 'NULL')
        ORDER BY c DESC, issuer_name
        LIMIT 15
        """
    ):
        print(f"  {row['issuer_name']}: {row['c']}")

    print("\n=== Stage 1.2 Export ===")
    print(f"rows exported: {export_rows}")
    print(f"path: {export_path}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 1.2 universe refine.")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument(
        "--output-csv",
        default="artifacts/universe_mvp.csv",
        help="Output CSV path for universe_mvp export",
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
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        conn.execute("BEGIN")
        migration_stats = apply_migration(conn)
        log(
            "migration complete; added columns: "
            + (", ".join(migration_stats["added_columns"]) if migration_stats["added_columns"] else "none")
        )

        profile_stats = refresh_product_profile(conn)
        log(
            "product profile refresh: "
            f"profile_rows={profile_stats.product_profile_rows_upserted}, "
            f"ucits_from_snapshot={profile_stats.ucits_from_snapshot}, "
            f"ucits_from_name={profile_stats.ucits_from_name}, "
            f"instrument_ucits_updated={profile_stats.instruments_ucits_updated}"
        )

        currency_stats = refine_currency(conn)
        log(
            "currency refinement: "
            f"before_null={currency_stats['before_null']}, "
            f"after_null={currency_stats['after_null']}, "
            f"imputed={currency_stats['imputed']}, "
            f"still_missing={currency_stats['still_missing']}"
        )

        issuer_stats = issuer_backfill(conn)
        log(
            "issuer backfill: "
            f"filled={issuer_stats['issuer_backfilled']}, "
            f"new_issuers={issuer_stats['new_issuers_created']}, "
            f"stage1_tagged={issuer_stats['issuer_source_stage1_tagged']}"
        )

        universe_stats = build_universe(conn)
        log(
            "universe build: "
            f"flagged={universe_stats['universe_flagged']}, "
            f"inserted={universe_stats['universe_rows_inserted']}"
        )

        export_rows = export_csv(conn, output_csv)
        log(f"csv export written: {output_csv} ({export_rows} rows)")

        print_outputs(conn, currency_stats, universe_stats, issuer_stats, export_rows, output_csv)

        conn.commit()
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
