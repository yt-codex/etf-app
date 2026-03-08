from __future__ import annotations

import sqlite3

from etf_app.completeness import collect_gap_rows, collect_gap_summary
from etf_app.profile import ensure_product_profile_schema
from etf_app.taxonomy import ensure_taxonomy_schema


def make_db(tmp_path) -> sqlite3.Connection:
    db_path = tmp_path / "completeness.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY,
            issuer_name TEXT,
            normalized_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT NOT NULL,
            instrument_name TEXT NOT NULL,
            instrument_type TEXT,
            issuer_id INTEGER,
            universe_mvp_flag INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE listing(
            listing_id INTEGER PRIMARY KEY,
            instrument_id INTEGER NOT NULL,
            venue_mic TEXT,
            ticker TEXT,
            trading_currency TEXT,
            primary_flag INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE issuer_metadata_snapshot(
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
    ensure_product_profile_schema(conn)
    ensure_taxonomy_schema(conn)
    return conn


def test_collect_gap_rows_includes_missing_fields_and_ft_status(tmp_path) -> None:
    conn = make_db(tmp_path)
    conn.execute("INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'Amundi', 'Amundi')")
    conn.execute(
        """
        INSERT INTO instrument(
            instrument_id, isin, instrument_name, instrument_type, issuer_id, universe_mvp_flag, status
        ) VALUES (1, 'LU2089238203', 'Amundi Prime Global UCITS ETF', 'ETF', 1, 1, 'active')
        """
    )
    conn.execute(
        """
        INSERT INTO listing(
            listing_id, instrument_id, venue_mic, ticker, trading_currency, primary_flag, status
        ) VALUES (1, 1, 'XLON', 'PRWU', 'USD', 1, 'active')
        """
    )
    conn.execute(
        """
        INSERT INTO product_profile(
            instrument_id, distribution_policy, ongoing_charges, domicile_country, updated_at
        ) VALUES (1, 'Accumulating', 0.05, 'Luxembourg', '2026-03-08T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO instrument_taxonomy(
            instrument_id, asset_class, geography_region, updated_at
        ) VALUES (1, 'equity', 'global', '2026-03-08T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(
            instrument_id, asof_date, source, source_url, quality_flag, raw_json
        ) VALUES (
            1,
            '2026-03-08',
            'ft_tearsheet',
            'https://markets.ft.com/data/search?query=LU2089238203',
            'ft_tearsheet_unresolved',
            '{"parser_version":"ft_tearsheet_v2","resolution_failed":true}'
        )
        """
    )
    conn.commit()

    rows = collect_gap_rows(conn)
    conn.close()

    assert len(rows) == 1
    assert rows[0]["ft_status"] == "ft_tearsheet_unresolved"
    assert rows[0]["missing_fields"] == "benchmark_name,fund_size,replication_method,equity_size,equity_style,sector"


def test_collect_gap_summary_counts_missing_fields_and_statuses(tmp_path) -> None:
    conn = make_db(tmp_path)
    conn.execute("INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'Amundi', 'Amundi')")
    conn.execute(
        """
        INSERT INTO instrument(
            instrument_id, isin, instrument_name, instrument_type, issuer_id, universe_mvp_flag, status
        ) VALUES (1, 'LU2089238203', 'Amundi Prime Global UCITS ETF', 'ETF', 1, 1, 'active')
        """
    )
    conn.execute(
        """
        INSERT INTO listing(
            listing_id, instrument_id, venue_mic, ticker, trading_currency, primary_flag, status
        ) VALUES (1, 1, 'XLON', 'PRWU', 'USD', 1, 'active')
        """
    )
    conn.execute(
        """
        INSERT INTO instrument_taxonomy(
            instrument_id, asset_class, geography_region, updated_at
        ) VALUES (1, 'equity', 'global', '2026-03-08T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(
            instrument_id, asof_date, source, source_url, quality_flag, raw_json
        ) VALUES (
            1,
            '2026-03-08',
            'ft_tearsheet',
            'https://markets.ft.com/data/search?query=LU2089238203',
            'ft_tearsheet_unresolved',
            '{"parser_version":"ft_tearsheet_v2","resolution_failed":true}'
        )
        """
    )
    conn.commit()

    summary = collect_gap_summary(collect_gap_rows(conn))
    conn.close()

    assert summary["rows_with_gaps"] == 1
    assert summary["ft_status_counts"] == {"ft_tearsheet_unresolved": 1}
    assert summary["missing_field_counts"]["benchmark_name"] == 1
