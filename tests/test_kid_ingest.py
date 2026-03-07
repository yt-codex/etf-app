from __future__ import annotations

import sqlite3

from etf_app.kid_ingest import (
    extract_profile_metadata_from_text,
    find_ongoing_charges_windowed,
    load_universe_rows,
    parse_issuer_filters,
)


def test_extract_profile_metadata_from_kid_text() -> None:
    text = (
        "Product Xtrackers MSCI World Information Technology UCITS ETF "
        "The fund is an Irish based UCITS (Undertakings for Collective Investment in Transferable Securities). "
        "INVESTMENT OBJECTIVE: The aim is for your investment to reflect the performance, before fees and expenses, "
        "of the MSCI World Information Technology 20/35 Custom Index (index) which is designed to reflect the "
        "performance of the listed shares of certain companies from various developed countries. "
        "INVESTMENT POLICY: To achieve the aim, the fund will attempt to replicate the index, before fees and expenses, "
        "by buying all or a substantial number of the securities in the index. "
    )

    parsed = extract_profile_metadata_from_text(text)

    assert parsed["benchmark_name"] == "MSCI World Information Technology 20/35 Custom Index"
    assert parsed["asset_class_hint"] == "Equity"
    assert parsed["domicile_country"] == "Ireland"
    assert parsed["replication_method"] == "physical"
    assert parsed["hedged_flag"] is None
    assert parsed["hedged_target"] is None


def test_extract_profile_metadata_from_kid_text_detects_hedged_share_class() -> None:
    parsed = extract_profile_metadata_from_text(
        "Product iShares Nasdaq 100 UCITS ETF EUR Hedged (Acc) authorised in Ireland."
    )

    assert parsed["domicile_country"] == "Ireland"
    assert parsed["hedged_flag"] == 1
    assert parsed["hedged_target"] == "EUR"


def test_extract_profile_metadata_from_benchmarkless_avantis_style_kid_text() -> None:
    text = (
        "Product Avantis Emerging Markets Equity UCITS ETF USD ACC ETF (IE000K975W13) "
        "Avantis Emerging Markets Equity UCITS ETF is authorised in Ireland and regulated by the Central Bank of Ireland. "
        "What is this product? Type This is an investment fund. "
        "The fund invests primarily in equity securities of companies in emerging markets. "
    )

    parsed = extract_profile_metadata_from_text(text)

    assert parsed["asset_class_hint"] == "Equity"
    assert parsed["domicile_country"] == "Ireland"
    assert parsed["benchmark_name"] is None


def test_find_ongoing_charges_windowed_prefers_annual_cost_impact() -> None:
    text = (
        "Ongoing costs USD -5.1 % Moderate What you might get back after costs "
        "Average return each year 10.300 USD 3.0 % 14.980 USD 5.9 % "
        "Favourable What you might get back after costs Average return each year "
        "15.760 USD 57.6 % 20.510 USD 10.8 % If you exit after 1 year If you exit after 7 years "
        "Total costs 37 USD 323 USD Annual cost impact (*) 0.4 % 0.4 % each year "
    )

    ongoing, attempts, _ = find_ongoing_charges_windowed(text)

    assert ongoing == 0.4
    assert attempts[0]["preferred_plausible"] == 0.4


def test_find_ongoing_charges_windowed_rejects_implausible_only_values() -> None:
    ongoing, attempts, _ = find_ongoing_charges_windowed(
        "Ongoing costs 34.9 % 57.6 % 10.8 %"
    )

    assert ongoing is None
    assert attempts[0]["plausible_0_3"] == []


def test_parse_issuer_filters_dedupes_and_splits_csv() -> None:
    filters = parse_issuer_filters(["Invesco, JPMorgan", "invesco", "  State Street / SPDR  "])

    assert filters == ["INVESCO", "JPMORGAN", "STATE STREET / SPDR"]


def test_load_universe_rows_can_filter_by_issuer() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE universe_mvp(
            instrument_id TEXT,
            isin TEXT,
            instrument_name TEXT,
            instrument_type TEXT,
            primary_venue_mic TEXT,
            issuer_normalized TEXT
        );
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            issuer_id INTEGER,
            issuer_source TEXT
        );
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY,
            normalized_name TEXT,
            domain TEXT
        );
        INSERT INTO issuer(issuer_id, normalized_name, domain) VALUES
            (1, 'Invesco', 'invesco.com'),
            (2, 'JPMorgan', 'jpmorgan.com');
        INSERT INTO instrument(instrument_id, issuer_id, issuer_source) VALUES
            (101, 1, 'manual'),
            (202, 2, 'manual');
        INSERT INTO universe_mvp(instrument_id, isin, instrument_name, instrument_type, primary_venue_mic, issuer_normalized) VALUES
            ('101', 'IE00INVESCO1', 'Invesco MSCI World UCITS ETF', 'ETF', 'XLON', 'Invesco'),
            ('202', 'IE00JPMORGN2', 'JPM US Equity Premium Income UCITS ETF', 'ETF', 'XLON', 'JPMorgan');
        """
    )

    rows = load_universe_rows(
        conn,
        limit=None,
        venue="ALL",
        priority_mode=False,
        mode="search",
        issuer_filters=["INVESCO"],
    )

    assert len(rows) == 1
    assert rows[0]["isin"] == "IE00INVESCO1"
