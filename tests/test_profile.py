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
                        "fund_size_value": 123456789.0,
                        "fund_size_currency": "USD",
                        "fund_size_asof": "2026-03-01",
                        "fund_size_scope": "fund",
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
            fund_size_value,
            fund_size_currency,
            fund_size_asof,
            fund_size_scope,
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
    assert profile["fund_size_value"] == 123456789.0
    assert profile["fund_size_currency"] == "USD"
    assert profile["fund_size_asof"] == "2026-03-01"
    assert profile["fund_size_scope"] == "fund"
    assert profile["replication_method"] == "physical"
    assert profile["hedged_flag"] == 1
    assert profile["hedged_target"] == "GBP"


def test_refresh_product_profile_reads_metadata_from_cost_snapshot_raw_json() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
        VALUES (2, 'IE00TEST0002', 'XTRACKERS MSCI WORLD INF TECH UCITS ETF', 1, NULL, 'active', '2026-01-01', '2026-01-01')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (2, '2026-03-02', 0.19, 'ok', ?)
        """,
        (
            json.dumps(
                {
                    "parse": {
                        "benchmark_name": "MSCI World Information Technology 20/35 Custom Index",
                        "asset_class_hint": "Equity",
                        "domicile_country": "Ireland",
                        "fund_size_value": 555000000.0,
                        "fund_size_currency": "USD",
                        "fund_size_asof": "2026-03-02",
                        "fund_size_scope": "share_class",
                        "replication_method": "physical",
                        "hedged_flag": 1,
                        "hedged_target": "EUR",
                    }
                },
                ensure_ascii=True,
            ),
        ),
    )
    ensure_instrument_cost_current_view(conn)

    stats = refresh_product_profile(conn)
    profile = conn.execute(
        """
        SELECT benchmark_name, asset_class_hint, domicile_country, fund_size_value, fund_size_currency, fund_size_asof, fund_size_scope, replication_method, hedged_flag, hedged_target
        FROM product_profile
        WHERE instrument_id = 2
        """
    ).fetchone()

    assert stats.metadata_synced == 1
    assert profile["benchmark_name"] == "MSCI World Information Technology 20/35 Custom Index"
    assert profile["asset_class_hint"] == "Equity"
    assert profile["domicile_country"] == "Ireland"
    assert profile["fund_size_value"] == 555000000.0
    assert profile["fund_size_currency"] == "USD"
    assert profile["fund_size_asof"] == "2026-03-02"
    assert profile["fund_size_scope"] == "share_class"
    assert profile["replication_method"] == "physical"
    assert profile["hedged_flag"] == 1
    assert profile["hedged_target"] == "EUR"


def test_refresh_product_profile_reads_metadata_from_parse_ongoing_payload() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
        VALUES (3, 'IE00TEST0003', 'AVANTIS GLOBAL EQUITY UCITS ETF', 1, NULL, 'active', '2026-01-01', '2026-01-01')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (3, '2026-03-02', 0.22, 'ok', ?)
        """,
        (
            json.dumps(
                {
                    "source": "generic_kid",
                    "parse_ongoing": {
                        "benchmark_name": "MSCI World Index",
                        "asset_class_hint": "Equity",
                        "domicile_country": "Ireland",
                        "fund_size_value": 250000000.0,
                        "fund_size_currency": "EUR",
                        "fund_size_asof": "03/03/2026",
                        "fund_size_scope": "fund",
                        "replication_method": "Synthetic replication",
                        "hedged_target": "USD",
                    },
                },
                ensure_ascii=True,
            ),
        ),
    )
    ensure_instrument_cost_current_view(conn)

    refresh_product_profile(conn)
    profile = conn.execute(
        """
        SELECT benchmark_name, asset_class_hint, domicile_country, fund_size_value, fund_size_currency, fund_size_asof, fund_size_scope, replication_method, hedged_flag, hedged_target
        FROM product_profile
        WHERE instrument_id = 3
        """
    ).fetchone()

    assert profile["benchmark_name"] == "MSCI World Index"
    assert profile["asset_class_hint"] == "Equity"
    assert profile["domicile_country"] == "Ireland"
    assert profile["fund_size_value"] == 250000000.0
    assert profile["fund_size_currency"] == "EUR"
    assert profile["fund_size_asof"] == "2026-03-03"
    assert profile["fund_size_scope"] == "fund"
    assert profile["replication_method"] == "synthetic"
    assert profile["hedged_flag"] == 1
    assert profile["hedged_target"] == "USD"


def test_refresh_product_profile_infers_domicile_from_isin_prefix_when_missing() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
        VALUES (4, 'DE000TEST0004', 'CORE DAX UCITS ETF', 1, NULL, 'active', '2026-01-01', '2026-01-01')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (4, '2026-03-02', 0.09, 'ok', '{}')
        """
    )
    ensure_instrument_cost_current_view(conn)

    refresh_product_profile(conn)
    profile = conn.execute(
        "SELECT domicile_country FROM product_profile WHERE instrument_id = 4"
    ).fetchone()

    assert profile["domicile_country"] == "Germany"


def test_refresh_product_profile_normalizes_domicile_aliases() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
        VALUES (5, 'FR000TEST0005', 'AMUNDI FRANCE UCITS ETF', 1, NULL, 'active', '2026-01-01', '2026-01-01')
        """
    )
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(instrument_id, asof_date, source, source_url, ter, use_of_income, ucits_compliant, quality_flag, raw_json)
        VALUES (?, '2026-03-02', 'issuer', 'https://example.com', 0.15, 'Distributing', 1, 'ok', ?)
        """,
        (
            5,
            json.dumps({"parsed": {"domicile_country": "French"}}, ensure_ascii=True),
        ),
    )
    ensure_instrument_cost_current_view(conn)

    refresh_product_profile(conn)
    profile = conn.execute(
        "SELECT domicile_country FROM product_profile WHERE instrument_id = 5"
    ).fetchone()

    assert profile["domicile_country"] == "France"
