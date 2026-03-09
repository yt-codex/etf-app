from __future__ import annotations

import json
import sqlite3

from etf_app.api import get_completeness_snapshot, get_custom_strategy_snapshot, get_strategy_snapshot, list_filter_options, list_funds, open_read_conn
from etf_app.deploy_db import build_deploy_db
from etf_app.profile import ensure_instrument_cost_current_view, ensure_product_profile_schema
from etf_app.taxonomy import ensure_taxonomy_schema


def make_source_db(tmp_path) -> str:
    db_path = tmp_path / "source.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT NOT NULL,
            instrument_name TEXT NOT NULL,
            instrument_type TEXT NOT NULL,
            issuer_id INTEGER NULL,
            universe_mvp_flag INTEGER DEFAULT 0,
            leverage_flag INTEGER DEFAULT 0,
            inverse_flag INTEGER DEFAULT 0,
            ucits_flag INTEGER NULL,
            ucits_source TEXT NULL,
            status TEXT DEFAULT 'active'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE listing(
            listing_id INTEGER PRIMARY KEY,
            instrument_id INTEGER NOT NULL,
            primary_flag INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            venue_mic TEXT NULL,
            ticker TEXT NULL,
            trading_currency TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY,
            issuer_name TEXT NOT NULL,
            normalized_name TEXT NULL
        )
        """
    )
    ensure_product_profile_schema(conn)
    ensure_instrument_cost_current_view(conn)
    ensure_taxonomy_schema(conn)

    conn.executemany(
        "INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (?, ?, ?)",
        [
            (1, "Vanguard", "Vanguard"),
            (2, "State Street", "State Street / SPDR"),
            (3, "WisdomTree", "WisdomTree"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO instrument(
            instrument_id, isin, instrument_name, instrument_type, issuer_id,
            universe_mvp_flag, leverage_flag, inverse_flag, ucits_flag, ucits_source, status
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, 0, 1, 'issuer_metadata_snapshot', 'active')
        """,
        [
            (1, "IE000WORLD01", "World Equity UCITS ETF", "ETF", 1, 1),
            (2, "IE000SCVAL01", "Global Small Cap Value UCITS ETF", "ETF", 1, 1),
            (3, "IE000SHBND01", "USD Treasury 1-3 UCITS ETF", "ETF", 2, 1),
            (4, "IE000EXTRA01", "Excluded Fund UCITS ETF", "ETF", 2, 0),
            (5, "JE00WGLD0001", "WisdomTree Core Physical Gold", "ETC", 3, 0),
        ],
    )
    conn.executemany(
        """
        INSERT INTO listing(
            listing_id, instrument_id, primary_flag, status, venue_mic, ticker, trading_currency
        )
        VALUES (?, ?, 1, 'active', ?, ?, ?)
        """,
        [
            (1, 1, "XLON", "VWLD", "USD"),
            (2, 2, "XETR", "GSCV", "USD"),
            (3, 3, "XLON", "USTS", "USD"),
            (4, 4, "XLON", "XTRA", "USD"),
            (5, 5, "XLON", "WGLD", "USD"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO product_profile(
            instrument_id, distribution_policy, ucits_flag, ucits_source, ucits_updated_at,
            ongoing_charges, ongoing_charges_asof, benchmark_name, asset_class_hint, domicile_country,
            fund_size_value, fund_size_currency, fund_size_asof, fund_size_scope,
            replication_method, hedged_flag, hedged_target, updated_at
        )
        VALUES (?, ?, 1, 'issuer_metadata_snapshot', '2026-03-08', ?, '2026-03-08', ?, ?, ?, ?, ?, ?, 'fund', 'physical', NULL, NULL, '2026-03-08T00:00:00Z')
        """,
        [
            (1, "Accumulating", 0.12, "MSCI World", "equity", "Ireland", 1500000000.0, "USD", "2026-03-08"),
            (2, "Accumulating", 0.25, "MSCI World Small Cap Value", "equity", "Ireland", 420000000.0, "USD", "2026-03-08"),
            (3, "Accumulating", 0.10, "US Treasury 1-3", "bond", "Ireland", 650000000.0, "USD", "2026-03-08"),
            (4, "Distributing", 0.35, "Excluded Index", "equity", "Ireland", 100000000.0, "USD", "2026-03-08"),
            (5, None, 0.15, "Physical Gold", "commodity", "Jersey", 250000000.0, "USD", "2026-03-08"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cost_snapshot(
            cost_id, instrument_id, asof_date, ongoing_charges, quality_flag, raw_json
        )
        VALUES (?, ?, ?, ?, ?, '{}')
        """,
        [
            (1, 1, "2026-01-01", 0.50, "ok"),
            (2, 1, "2026-03-08", 0.12, "ok"),
            (3, 2, "2026-03-08", 0.25, "ok"),
            (4, 3, "2026-03-08", 0.10, "ok"),
            (5, 4, "2026-03-08", 0.35, "ok"),
            (6, 5, "2026-03-08", 0.15, "ok"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO instrument_taxonomy(
            instrument_id, asset_class, geography_scope, geography_region, geography_country,
            equity_size, equity_style, factor, sector, theme, bond_type, duration_bucket,
            duration_years_low, duration_years_high, commodity_type, cash_proxy_flag, gold_flag,
            cash_flag, govt_bond_flag, hedged_flag, hedged_target, domicile_country,
            distribution_policy, taxonomy_version, evidence_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, 'unknown', 0, 0, 0, ?, 0, NULL, ?, ?, 'taxonomy_v3', ?, '2026-03-08T00:00:00Z')
        """,
        [
            (1, "equity", "global", "global", None, "large", "blend", None, None, None, None, 0, "Ireland", "Accumulating", json.dumps({"rules": ["equity_global"]})),
            (2, "equity", "global", "global", None, "small", "value", None, None, None, None, 0, "Ireland", "Accumulating", json.dumps({"rules": ["small_value"]})),
            (3, "bond", "regional", "us", "United States", None, None, "govt", "short", 1.0, 3.0, 1, "Ireland", "Accumulating", json.dumps({"rules": ["bond_short"]})),
            (4, "equity", "global", "global", None, "large", "blend", None, None, None, None, 0, "Ireland", "Distributing", json.dumps({"rules": ["excluded"]})),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_build_deploy_db_preserves_runtime_queries(tmp_path) -> None:
    source_db = make_source_db(tmp_path)
    output_db = tmp_path / "deploy.sqlite"

    stats = build_deploy_db(source_db_path=source_db, output_db_path=str(output_db))

    assert stats.instrument_rows == 4
    assert stats.issuer_rows == 3
    assert stats.listing_rows == 4
    assert stats.product_profile_rows == 4
    assert stats.instrument_taxonomy_rows == 3
    assert stats.cost_snapshot_rows == 4
    assert stats.output_size_bytes <= stats.source_size_bytes

    source_conn = open_read_conn(source_db)
    deploy_conn = open_read_conn(str(output_db))
    try:
        params = {"limit": "50", "offset": "0", "sort": "name", "direction": "asc"}
        assert list_funds(source_conn, params=params) == list_funds(deploy_conn, params=params)
        assert list_filter_options(source_conn) == list_filter_options(deploy_conn)

        source_strategy = get_custom_strategy_snapshot(
            source_conn,
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
            buckets=[
                {"bucket_name": "equity_global", "target_weight": 60.0},
                {"bucket_name": "short_bonds", "target_weight": 40.0},
            ],
        )
        deploy_strategy = get_custom_strategy_snapshot(
            deploy_conn,
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
            buckets=[
                {"bucket_name": "equity_global", "target_weight": 60.0},
                {"bucket_name": "short_bonds", "target_weight": 40.0},
            ],
        )
        assert source_strategy == deploy_strategy

        source_gold_strategy = get_strategy_snapshot(
            source_conn,
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
            strategy_name="Harry Browne Permanent Portfolio",
        )
        deploy_gold_strategy = get_strategy_snapshot(
            deploy_conn,
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
            strategy_name="Harry Browne Permanent Portfolio",
        )
        assert source_gold_strategy == deploy_gold_strategy
        gold_rows = [row for row in deploy_gold_strategy["strategies"][0]["rows"] if row["bucket_name"] == "gold"]
        assert [row["ticker"] for row in gold_rows] == ["WGLD"]

        source_coverage = get_completeness_snapshot(
            source_conn,
            db_path=source_db,
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
        )
        deploy_coverage = get_completeness_snapshot(
            deploy_conn,
            db_path=str(output_db),
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
        )
        assert source_coverage["product_profile"] == deploy_coverage["product_profile"]
        assert source_coverage["taxonomy"] == deploy_coverage["taxonomy"]
        assert source_coverage["fee_gaps"] == deploy_coverage["fee_gaps"]
        assert source_coverage["strategy_readiness"] == deploy_coverage["strategy_readiness"]
    finally:
        source_conn.close()
        deploy_conn.close()
