from __future__ import annotations

import io
import json
import sqlite3

from etf_app.api import create_app, get_fund_detail, list_filter_options, list_funds
from etf_app.profile import ensure_instrument_cost_current_view, ensure_product_profile_schema
from etf_app.taxonomy import ensure_taxonomy_schema


def make_api_db(tmp_path) -> str:
    db_path = tmp_path / "api.sqlite"
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
            (2, "SPDR", "State Street / SPDR"),
            (3, "WisdomTree", "WisdomTree"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO instrument(
            instrument_id, isin, instrument_name, instrument_type, issuer_id,
            universe_mvp_flag, leverage_flag, inverse_flag, ucits_flag, ucits_source, status
        )
        VALUES (?, ?, ?, ?, ?, 1, 0, 0, ?, ?, 'active')
        """,
        [
            (1, "IE000WORLD01", "World Equity UCITS ETF GBP Hedged", "ETF", 1, 1, "issuer_metadata_snapshot"),
            (2, "IE000SCVAL01", "Global Small Cap Value UCITS ETF", "ETF", 1, 1, "issuer_metadata_snapshot"),
            (3, "IE000LGBND01", "Euro Government Bond 15+ UCITS ETF", "ETF", 2, 1, "issuer_metadata_snapshot"),
            (4, "IE000SHBND01", "USD Treasury 1-3 UCITS ETF", "ETF", 2, 1, "issuer_metadata_snapshot"),
            (5, "JE00GOLD001", "Physical Gold ETC", "ETC", 3, 0, "issuer_metadata_snapshot"),
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
            (1, 1, "XLON", "VWLD", "GBP"),
            (2, 2, "XETR", "GSCV", "USD"),
            (3, 3, "XLON", "EGLB", "EUR"),
            (4, 4, "XETR", "USTS", "USD"),
            (5, 5, "XLON", "PHGL", "USD"),
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
        VALUES (?, ?, ?, ?, '2026-03-07', ?, '2026-03-07', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-03-07T00:00:00Z')
        """,
        [
            (1, "Accumulating", 1, "issuer_metadata_snapshot", 0.12, "MSCI World", "equity", "Ireland", 1500000000.0, "USD", "2026-03-06", "fund", "physical", 1, "GBP"),
            (2, "Accumulating", 1, "issuer_metadata_snapshot", 0.25, "MSCI World Small Cap Value", "equity", "Ireland", 420000000.0, "USD", "2026-03-06", "fund", "physical", None, None),
            (3, "Distributing", 1, "issuer_metadata_snapshot", 0.20, "Euro Government Bond 15+", "bond", "Ireland", 800000000.0, "EUR", "2026-03-06", "fund", "physical", None, None),
            (4, "Accumulating", 1, "issuer_metadata_snapshot", 0.10, "US Treasury 1-3", "bond", "Ireland", 650000000.0, "USD", "2026-03-06", "fund", "physical", None, None),
            (5, None, 0, "issuer_metadata_snapshot", 0.39, "Physical Gold", "commodity", "Jersey", 210000000.0, "USD", "2026-03-06", "fund", "physical", None, None),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cost_snapshot(
            instrument_id, asof_date, ongoing_charges, quality_flag, raw_json
        )
        VALUES (?, '2026-03-07', ?, 'ok', '{}')
        """,
        [
            (1, 0.12),
            (2, 0.25),
            (3, 0.20),
            (4, 0.10),
            (5, 0.39),
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'taxonomy_v2', ?, '2026-03-07T00:00:00Z')
        """,
        [
            (1, "equity", "global", "global", None, None, None, None, None, None, None, None, None, None, "unknown", 0, 0, 0, 0, 1, "GBP", "Ireland", "Accumulating", json.dumps({"rules": ["profile:benchmark_name", "hedged:GBP"]})),
            (2, "equity", "global", "global", None, "small", "value", None, None, None, None, None, None, None, "unknown", 0, 0, 0, 0, 0, None, "Ireland", "Accumulating", json.dumps({"rules": ["size:small", "style:value"]})),
            (3, "bond", "regional", "europe", None, None, None, None, None, None, "govt", "long", 15.0, None, "unknown", 0, 0, 0, 1, 0, None, "Ireland", "Distributing", json.dumps({"rules": ["bond_type:govt", "duration:long"]})),
            (4, "bond", "regional", "us", "United States", None, None, None, None, None, "govt", "short", 1.0, 3.0, "unknown", 1, 0, 0, 1, 0, None, "Ireland", "Accumulating", json.dumps({"rules": ["bond_type:govt", "duration:short"]})),
            (5, "commodity", "global", "global", None, None, None, None, None, None, None, None, None, None, "gold", 0, 1, 0, 0, 0, None, "Jersey", None, json.dumps({"rules": ["commodity:gold"]})),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def call_json(app, path: str, query: str = "") -> tuple[str, dict[str, str], dict[str, object]]:
    captured: dict[str, object] = {}

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        captured["status"] = status
        captured["headers"] = {key: value for key, value in headers}

    body = b"".join(
        app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": path,
                "QUERY_STRING": query,
                "wsgi.input": io.BytesIO(b""),
            },
            start_response,
        )
    )
    return str(captured["status"]), dict(captured["headers"]), json.loads(body.decode("utf-8"))


def test_list_funds_supports_filters_sort_and_pagination(tmp_path) -> None:
    db_path = make_api_db(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    payload = list_funds(
        conn,
        params={"asset_class": "equity", "sort": "isin", "limit": "1", "offset": "0"},
    )

    assert payload["total"] == 2
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert len(payload["items"]) == 1
    assert payload["items"][0]["isin"] == "IE000SCVAL01"
    assert payload["items"][0]["asset_class"] == "equity"


def test_get_fund_detail_includes_taxonomy_evidence(tmp_path) -> None:
    db_path = make_api_db(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    payload = get_fund_detail(conn, "IE000WORLD01")

    assert payload is not None
    assert payload["hedged_flag"] == 1
    assert payload["fund_size_value"] == 1500000000.0
    assert payload["fund_size_currency"] == "USD"
    assert payload["taxonomy_evidence"]["rules"] == ["profile:benchmark_name", "hedged:GBP"]


def test_api_endpoints_expose_filters_completeness_and_strategies(tmp_path) -> None:
    db_path = make_api_db(tmp_path)
    app = create_app(db_path)

    status, _headers, filters_payload = call_json(app, "/api/filters")
    assert status == "200 OK"
    assert filters_payload["asset_class"][0] == {"value": "bond", "count": 2}
    assert filters_payload["hedged_flag"]["true"] == 1

    status, _headers, completeness_payload = call_json(app, "/api/completeness")
    assert status == "200 OK"
    assert completeness_payload["product_profile"]["fields"]["ongoing_charges"]["known"] == 5
    assert completeness_payload["product_profile"]["fields"]["fund_size_value"]["known"] == 5
    assert completeness_payload["taxonomy"]["equity"]["geography_known"]["known"] == 2

    status, _headers, strategy_payload = call_json(app, "/api/strategies", "venue=ALL&top_n=1")
    assert status == "200 OK"
    all_weather = next(item for item in strategy_payload["strategies"] if item["name"] == "Ray Dalio All Weather Portfolio")
    assert all_weather["rows"]
    assert isinstance(all_weather["rows"][0]["selection_reason"], dict)

    status, _headers, filtered_strategy_payload = call_json(
        app,
        "/api/strategies",
        "venue=ALL&top_n=1&strategy_name=Ray+Dalio+All+Weather+Portfolio",
    )
    assert status == "200 OK"
    assert [strategy["name"] for strategy in filtered_strategy_payload["strategies"]] == ["Ray Dalio All Weather Portfolio"]
    assert filtered_strategy_payload["strategies"][0]["detail"]
    assert filtered_strategy_payload["strategies"][0]["implementation_note"]


def test_health_and_fund_detail_routes(tmp_path) -> None:
    db_path = make_api_db(tmp_path)
    app = create_app(db_path)

    status, _headers, payload = call_json(app, "/health")
    assert status == "200 OK"
    assert payload == {"status": "ok", "mvp_instruments": 5}

    status, _headers, payload = call_json(app, "/api/funds/IE000WORLD01")
    assert status == "200 OK"
    assert payload["benchmark_name"] == "MSCI World"
    assert payload["fund_size_scope"] == "fund"

    status, _headers, payload = call_json(app, "/api/funds/UNKNOWN")
    assert status == "404 Not Found"
    assert payload["error"] == "fund_not_found"
