from __future__ import annotations

import sqlite3

import etf_app.ft_enrich as ft_enrich
from etf_app.profile import ensure_product_profile_schema
from etf_app.taxonomy import ensure_taxonomy_schema


SUMMARY_HTML = """
<div class="mod-tearsheet-overview__header__name">iShares Core S&P 500 UCITS ETF USD (Acc)</div>
<table class="mod-ui-table mod-ui-table--two-column mod-profile-and-investment-app__table--profile">
  <tr><th>Investment style (stocks)</th><td>Market Cap: Large<br/>Investment Style: Blend</td></tr>
  <tr><th>Income treatment</th><td>Accumulation</td></tr>
  <tr><th>Domicile</th><td>Ireland</td></tr>
  <tr><th>ISIN</th><td>IE00B5BMR087</td></tr>
</table>
<table class="mod-ui-table mod-ui-table--two-column mod-profile-and-investment-app__table--invest">
  <tr><th>Fund size</th><td><div>102.95bn <span class="mod-format__currency">GBP</span><span class="disclaimer"><br/>As of Feb 28 2026</span></div></td></tr>
  <tr><th>Ongoing charge</th><td>0.07%</td></tr>
</table>
"""


HOLDINGS_HTML = """
<div role="tabpanel" id="sectors-panel" class="mod-ui-tab-content" aria-hidden="false">
  <div>
    <div class="mod-weightings__sectors" aria-hidden="false">
      <div class="mod-weightings__sectors__table">
        <table class="mod-ui-table mod-ui-table--colored">
          <thead><th class="mod-ui-table__header--text">Sector</th><th>% Net assets</th><th>Category average</th></thead>
          <tbody>
            <tr><td>Technology</td><td>99.97%</td><td>88.72%</td></tr>
            <tr><td>Industrials</td><td>0.00%</td><td>1.71%</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>
"""


SHARE_CLASS_SIZE_SUMMARY_HTML = """
<div class="mod-tearsheet-overview__header__name">Example Equity UCITS ETF</div>
<table class="mod-ui-table mod-ui-table--two-column">
  <tr><th>Investment style</th><td>Large Blend</td></tr>
  <tr><th>Use of income</th><td>Distribution</td></tr>
  <tr><th>Domicile</th><td>Ireland</td></tr>
  <tr><th>ISIN</th><td>IE00TEST0001</td></tr>
  <tr><th>Share class size</th><td>2.5 billion USD As of Feb 28, 2026</td></tr>
  <tr><th>Ongoing charge</th><td>0.12%</td></tr>
</table>
"""


SEARCH_HTML = """
<div class="search-results">
  <a href="/data/etfs/tearsheet/summary?s=SXR8:GER:EUR">iShares Core S&amp;P 500 UCITS ETF USD (Acc)</a>
  <a href="/data/etfs/tearsheet/summary?s=CSPX:LSE:USD">iShares Core S&amp;P 500 UCITS ETF USD (Acc)</a>
  <a href="/data/equities/tearsheet/summary?s=NOTETF">Ignore non ETF result</a>
</div>
"""


def make_db(tmp_path) -> str:
    db_path = tmp_path / "ft.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT NOT NULL,
            instrument_name TEXT NOT NULL,
            instrument_type TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            universe_mvp_flag INTEGER DEFAULT 0,
            ucits_flag INTEGER NULL,
            ucits_source TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE listing(
            listing_id INTEGER PRIMARY KEY,
            instrument_id INTEGER NOT NULL,
            venue_mic TEXT NOT NULL,
            ticker TEXT NOT NULL,
            trading_currency TEXT NOT NULL,
            primary_flag INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
        """
    )
    ensure_product_profile_schema(conn)
    ensure_taxonomy_schema(conn)
    conn.execute(
        """
        INSERT INTO instrument(
            instrument_id, isin, instrument_name, instrument_type, status, universe_mvp_flag, ucits_flag, ucits_source
        ) VALUES (1, 'IE00B5BMR087', 'iShares Core S&P 500 UCITS ETF USD (Acc)', 'ETF', 'active', 1, 1, 'legacy_seed')
        """
    )
    conn.execute(
        """
        INSERT INTO listing(
            listing_id, instrument_id, venue_mic, ticker, trading_currency, primary_flag, status
        ) VALUES (1, 1, 'XLON', 'CSPX', 'USD', 1, 'active')
        """
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_parse_ft_summary_html_extracts_profile_metadata() -> None:
    parsed = ft_enrich.parse_ft_summary_html(SUMMARY_HTML)

    assert parsed["isin"] == "IE00B5BMR087"
    assert parsed["use_of_income"] == "Accumulating"
    assert parsed["ucits_compliant"] == 1
    assert parsed["asset_class_hint"] == "Equity"
    assert parsed["domicile_country"] == "Ireland"
    assert parsed["fund_size_value"] == 102_950_000_000.0
    assert parsed["fund_size_currency"] == "GBP"
    assert parsed["fund_size_asof"] == "2026-02-28"
    assert parsed["fund_size_scope"] == "fund"
    assert parsed["equity_size_hint"] == "large"
    assert parsed["equity_style_hint"] == "blend"


def test_parse_ft_summary_html_falls_back_to_share_class_size_and_variant_labels() -> None:
    parsed = ft_enrich.parse_ft_summary_html(SHARE_CLASS_SIZE_SUMMARY_HTML)

    assert parsed["isin"] == "IE00TEST0001"
    assert parsed["use_of_income"] == "Distributing"
    assert parsed["fund_size_value"] == 2_500_000_000.0
    assert parsed["fund_size_currency"] == "USD"
    assert parsed["fund_size_asof"] == "2026-02-28"
    assert parsed["fund_size_scope"] == "share_class"
    assert parsed["equity_size_hint"] == "large"
    assert parsed["equity_style_hint"] == "blend"


def test_parse_ft_holdings_html_extracts_dominant_sector() -> None:
    parsed = ft_enrich.parse_ft_holdings_html(HOLDINGS_HTML)

    assert parsed["sector_hint"] == "technology"
    assert parsed["sector_weight"] == 99.97
    assert parsed["sector_weights"] == [
        {"label": "Technology", "sector": "technology", "weight": 99.97},
        {"label": "Industrials", "sector": "industrials", "weight": 0.0},
    ]


def test_extract_ft_search_symbols_keeps_unique_etf_summary_symbols() -> None:
    assert ft_enrich.extract_ft_search_symbols(SEARCH_HTML) == [
        "SXR8:GER:EUR",
        "CSPX:LSE:USD",
    ]


def test_resolve_symbol_uses_search_fallback_when_direct_symbols_fail(tmp_path, monkeypatch) -> None:
    db_path = make_db(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    monkeypatch.setattr(ft_enrich, "build_session", lambda: object())

    def fake_fetch_html(_session, url: str) -> str | None:
        if url == ft_enrich.summary_url("CSPX:LSE:USD"):
            return None
        if url == ft_enrich.search_url("IE00B5BMR087"):
            return SEARCH_HTML
        if url == ft_enrich.summary_url("SXR8:GER:EUR"):
            return SUMMARY_HTML
        return None

    monkeypatch.setattr(ft_enrich, "fetch_html", fake_fetch_html)

    symbol, summary_html, parsed = ft_enrich.resolve_symbol(
        conn,
        object(),
        instrument_id=1,
        expected_isin="IE00B5BMR087",
        venue="ALL",
    )
    conn.close()

    assert symbol == "SXR8:GER:EUR"
    assert summary_html == SUMMARY_HTML
    assert parsed["isin"] == "IE00B5BMR087"


def test_run_ft_metadata_backfill_updates_profile_and_taxonomy(tmp_path, monkeypatch) -> None:
    db_path = make_db(tmp_path)

    monkeypatch.setattr(ft_enrich, "build_session", lambda: object())

    def fake_fetch_html(_session, url: str) -> str | None:
        if "/summary?" in url:
            return SUMMARY_HTML
        if "/holdings?" in url:
            return HOLDINGS_HTML
        return None

    monkeypatch.setattr(ft_enrich, "fetch_html", fake_fetch_html)

    stats = ft_enrich.run_ft_metadata_backfill(
        db_path=db_path,
        limit=10,
        venue="ALL",
        sleep_seconds=0.0,
    )

    assert stats.attempted == 1
    assert stats.resolved == 1
    assert stats.summary_parsed == 1
    assert stats.holdings_parsed == 1
    assert stats.snapshots_inserted == 1
    assert stats.profile_rows_upserted == 1
    assert stats.taxonomy_rows_updated == 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    profile_row = conn.execute(
        """
        SELECT distribution_policy, fund_size_value, fund_size_currency, equity_size_hint, equity_style_hint, sector_hint, sector_weight
        FROM product_profile
        WHERE instrument_id = 1
        """
    ).fetchone()
    taxonomy_row = conn.execute(
        """
        SELECT asset_class, equity_size, equity_style, sector
        FROM instrument_taxonomy
        WHERE instrument_id = 1
        """
    ).fetchone()
    conn.close()

    assert dict(profile_row) == {
        "distribution_policy": "Accumulating",
        "fund_size_value": 102_950_000_000.0,
        "fund_size_currency": "GBP",
        "equity_size_hint": "large",
        "equity_style_hint": "blend",
        "sector_hint": "technology",
        "sector_weight": 99.97,
    }
    assert dict(taxonomy_row) == {
        "asset_class": "equity",
        "equity_size": "large",
        "equity_style": "blend",
        "sector": "technology",
    }
