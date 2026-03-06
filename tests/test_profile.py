from __future__ import annotations

import json
import sqlite3

from etf_app.profile import detect_ucits_flag, ensure_instrument_cost_current_view, refresh_product_profile


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT,
            instrument_name TEXT,
            ucits_flag INTEGER,
            issuer_id INTEGER,
            status TEXT,
            created_at TEXT,
            updated_at TEXT
        );
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
        );
        CREATE TABLE cost_snapshot(
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
        );
        """
    )
    return conn


def test_detect_ucits_flag_handles_exchange_abbreviations() -> None:
    assert detect_ucits_flag("ISH.S.EU.SEL.DIV.30 U.ETF") == 1
    assert detect_ucits_flag("AMUNDI-A.DAX50ESG2 UE DIS") == 1
    assert detect_ucits_flag("SPDR S&P 500 ETF TRUST") is None


def test_refresh_product_profile_backfills_current_fields() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
        VALUES (1, 'IE00TEST0001', 'ISH.S.EU.SEL.DIV.30 U.ETF', NULL, NULL, 'active', '2026-01-01', '2026-01-01')
        """
    )
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(instrument_id, asof_date, source, source_url, ter, use_of_income, ucits_compliant, quality_flag, raw_json)
        VALUES (?, '2026-02-15', 'ishares_product_page', 'https://example.com/old', 0.12, NULL, NULL, 'ok', ?)
        """,
        (
            1,
            json.dumps(
                {
                    "parsed": {
                        "benchmark_name": "MSCI World Index",
                        "asset_class_hint": "Equity",
                        "domicile_country": "Ireland",
                        "replication_method": "physical",
                        "hedged_flag": 1,
                        "hedged_target": "GBP",
                    }
                },
                ensure_ascii=True,
            ),
        ),
    )
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(instrument_id, asof_date, source, source_url, ter, use_of_income, ucits_compliant, quality_flag, raw_json)
        VALUES (1, '2026-03-01', 'issuer', 'https://example.com', 0.12, 'Accumulating', 1, 'ok', '{}')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (1, '2026-03-02', 0.12, 'ok', '{}')
        """
    )
    ensure_instrument_cost_current_view(conn)

    stats = refresh_product_profile(conn)

    instrument = conn.execute(
        "SELECT ucits_flag, ucits_source FROM instrument WHERE instrument_id = 1"
    ).fetchone()
    profile = conn.execute(
        """
        SELECT
            distribution_policy,
            ucits_flag,
            ucits_source,
            ongoing_charges,
            ongoing_charges_asof,
            benchmark_name,
            asset_class_hint,
            domicile_country,
            replication_method,
            hedged_flag,
            hedged_target
        FROM product_profile
        WHERE instrument_id = 1
        """
    ).fetchone()

    assert stats.instruments_ucits_updated == 1
    assert instrument["ucits_flag"] == 1
    assert instrument["ucits_source"] == "issuer_metadata_snapshot"
    assert profile["distribution_policy"] == "Accumulating"
    assert profile["ucits_flag"] == 1
    assert profile["ongoing_charges"] == 0.12
    assert profile["ongoing_charges_asof"] == "2026-03-02"
    assert profile["benchmark_name"] == "MSCI World Index"
    assert profile["asset_class_hint"] == "Equity"
    assert profile["domicile_country"] == "Ireland"
    assert profile["replication_method"] == "physical"
    assert profile["hedged_flag"] == 1
    assert profile["hedged_target"] == "GBP"
