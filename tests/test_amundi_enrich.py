from __future__ import annotations

import json
import sqlite3

import pytest

import etf_app.amundi_enrich as amundi_enrich
from etf_app.amundi_enrich import (
    DownloadResult,
    FactsheetCandidate,
    KidFallbackResult,
    ProbeResult,
    build_amundi_kid_candidate_urls,
    build_factsheet_candidates,
    ensure_tables_and_view,
    extract_distribution,
    extract_fee,
    extract_profile_metadata_from_factsheet,
    insert_cost_snapshot_from_ter,
    load_targets,
    merge_profile_metadata,
    select_monthly_factsheet_document,
)
from etf_app.profile import ensure_product_profile_schema


def test_extract_profile_metadata_from_factsheet() -> None:
    text = (
        "BOND "
        "Amundi US Treasury Bond Long Dated UCITS ETF Acc "
        "Benchmark :100% BLOOMBERG BARCLAYS US LONG TREASURY TOTAL RETURN INDEX VALUE "
        "Date of the first NAV :26/07/2021 "
        "AUM as of :27/02/2026"
        "Assets Under Management (AUM) :5,814.79 ( million EUR ) "
        "Replication type :Physical "
        "Asset class :Bond "
        "Fund structure SICAV under Luxembourg law "
    )
    lines = [
        "BOND",
        "Amundi US Treasury Bond Long Dated UCITS ETF Acc",
        "Replication type :Physical",
        "Asset class :Bond",
        "Fund structure SICAV under Luxembourg law",
    ]

    parsed = extract_profile_metadata_from_factsheet(text, lines)

    assert parsed["benchmark_name"] == "BLOOMBERG BARCLAYS US LONG TREASURY TOTAL RETURN INDEX VALUE"
    assert parsed["asset_class_hint"] == "Bond"
    assert parsed["domicile_country"] == "Luxembourg"
    assert parsed["fund_size_value"] == 5814790000.0
    assert parsed["fund_size_currency"] == "EUR"
    assert parsed["fund_size_asof"] == "2026-02-27"
    assert parsed["fund_size_scope"] == "fund"
    assert parsed["replication_method"] == "physical"
    assert parsed["hedged_flag"] is None
    assert parsed["hedged_target"] is None


def test_extract_profile_metadata_from_factsheet_handles_legacy_total_assets_format() -> None:
    text = (
        "Total asset 4,730.45 ( M EUR ) "
        "Fund Net asset Value 13,940.05 ( M EUR ) "
        "Type of shares Capitalisation "
    )

    parsed = extract_profile_metadata_from_factsheet(text, text.splitlines())

    assert parsed["fund_size_value"] == 4730450000.0
    assert parsed["fund_size_currency"] == "EUR"
    assert parsed["fund_size_scope"] == "fund"


def test_extract_fee_parses_total_expense_ratio_line_without_percent_symbol() -> None:
    lines = [
        "Fund structure SICAV under Luxembourg law",
        "ISIN code LU1248511575",
        "Replication type Synthtique",
        "Total Expense Ratio p.a 0.1",
        "Type of shares Capitalisation",
    ]

    fee, matched_line = extract_fee("\n".join(lines), lines)

    assert fee == 0.1
    assert matched_line == "Total Expense Ratio p.a 0.1"


def test_extract_distribution_maps_capitalisation() -> None:
    lines = [
        "Income treatment Distribution",
        "Type of shares Capitalisation",
    ]

    distribution, matched_line = extract_distribution("\n".join(lines), lines)

    assert distribution == "Distributing"
    assert matched_line == "Income treatment Distribution"


def test_extract_profile_metadata_normalizes_synthetic_replication() -> None:
    text = (
        "Benchmark :100% UK SONIA "
        "Replication type :Synthtique "
        "Asset class :Bond "
        "Fund structure SICAV under Luxembourg law "
    )
    lines = [
        "Replication type :Synthtique",
        "Asset class :Bond",
        "Fund structure SICAV under Luxembourg law",
    ]

    parsed = extract_profile_metadata_from_factsheet(text, lines)

    assert parsed["replication_method"] == "synthetic"


def test_select_monthly_factsheet_document_prefers_english_latest_etf_document() -> None:
    docs = [
        {
            "language": "French",
            "recordDate": 1772236800000,
            "url": "/pdfDocuments/download/french.pdf",
            "appliedAlias": "/pdfDocuments/monthly-factsheet/LU1407888996/FRA/LUX/RETAIL/ETF/20260228",
            "name": "French latest",
            "documentType": {"name": "monthlyfactsheet"},
        },
        {
            "language": "English",
            "recordDate": 1770000000000,
            "url": "/pdfDocuments/download/english-old.pdf",
            "appliedAlias": "/pdfDocuments/monthly-factsheet/LU1407888996/ENG/LUX/RETAIL/AMUNDI/20260131",
            "name": "English older",
            "documentType": {"name": "monthlyfactsheet"},
        },
        {
            "language": "English",
            "recordDate": 1772236800000,
            "url": "/pdfDocuments/download/english-new.pdf",
            "appliedAlias": "/pdfDocuments/monthly-factsheet/LU1407888996/ENG/LUX/RETAIL/ETF/20260228",
            "name": "English latest",
            "documentType": {"name": "monthlyfactsheet"},
        },
    ]

    selected = select_monthly_factsheet_document(docs)

    assert selected is not None
    assert selected["name"] == "English latest"


def test_build_factsheet_candidates_prefers_discovered_then_known_then_legacy() -> None:
    from etf_app.amundi_enrich import DiscoveredFactsheet

    discovered = DiscoveredFactsheet(
        url="https://www.amundietf.com/pdfDocuments/download/live.pdf",
        context_country="SGP",
        user_profile="RETAIL",
        language="English",
        record_date=1772236800000,
        document_name="MonthlyFactsheet.pdf",
        applied_alias="/pdfDocuments/monthly-factsheet/LU1407888996/ENG/SGP/RETAIL/ETF/20260228",
    )

    candidates = build_factsheet_candidates(
        "LU1407888996",
        discovered=discovered,
        known_url="https://www.amundietf.com/pdfDocuments/download/known.pdf",
    )

    assert [candidate.source for candidate in candidates] == [
        "document_api:SGP:RETAIL:English",
        "instrument_url_map",
        "legacy_template",
    ]


def test_build_amundi_kid_candidate_urls_includes_priority_contexts() -> None:
    urls = build_amundi_kid_candidate_urls("LU1686830909")

    assert urls[0] == "https://www.amundietf.lu/pdfDocuments/kid-priips/LU1686830909/ENG/LUX"
    assert "https://www.amundietf.lu/pdfDocuments/kid-priips/LU1686830909/FRA/FRA" in urls
    assert "https://www.amundietf.lu/pdfDocuments/kid-priips/LU1686830909/DEU/DEU" in urls


def test_merge_profile_metadata_backfills_missing_fields_from_kid_payload() -> None:
    primary = {
        "ter": None,
        "benchmark_name": None,
        "asset_class_hint": "Bond",
        "domicile_country": None,
        "fund_size_value": None,
        "fund_size_currency": None,
        "fund_size_asof": None,
        "fund_size_scope": None,
        "replication_method": None,
        "hedged_flag": None,
        "hedged_target": None,
    }
    secondary = {
        "ongoing_charges": 0.10,
        "benchmark_name": "J.P. Morgan EMBI Global Diversified Index",
        "domicile_country": "Luxembourg",
        "fund_size_value": 250000000.0,
        "fund_size_currency": "USD",
        "fund_size_asof": "2026-03-01",
        "fund_size_scope": "fund",
        "replication_method": "synthetic",
        "hedged_target": "USD",
    }

    merged = merge_profile_metadata(primary, secondary, ter_field="ongoing_charges")

    assert merged["ter"] == 0.10
    assert merged["asset_class_hint"] == "Bond"
    assert merged["benchmark_name"] == "J.P. Morgan EMBI Global Diversified Index"
    assert merged["domicile_country"] == "Luxembourg"
    assert merged["fund_size_value"] == 250000000.0
    assert merged["fund_size_currency"] == "USD"
    assert merged["replication_method"] == "synthetic"
    assert merged["hedged_target"] == "USD"


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT,
            instrument_name TEXT,
            issuer_id INTEGER,
            universe_mvp_flag INTEGER,
            ucits_flag INTEGER
        );
        CREATE TABLE listing(
            listing_id INTEGER PRIMARY KEY,
            instrument_id INTEGER,
            ticker TEXT,
            venue_mic TEXT,
            primary_flag INTEGER,
            status TEXT,
            trading_currency TEXT
        );
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY,
            issuer_name TEXT,
            normalized_name TEXT
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
    ensure_product_profile_schema(conn)
    ensure_tables_and_view(conn)
    return conn


def test_load_targets_includes_fee_complete_rows_with_missing_profile_metadata() -> None:
    conn = make_conn()
    conn.execute("INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'Amundi', 'Amundi')")
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, universe_mvp_flag, ucits_flag)
        VALUES (1, 'LU0000000001', 'Amundi MSCI World UCITS ETF', 1, 1, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO listing(listing_id, instrument_id, ticker, venue_mic, primary_flag, status, trading_currency)
        VALUES (1, 1, 'CW8', 'XETR', 1, 'active', 'EUR')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (1, '2026-03-07', 0.12, 'amundi_factsheet_ok', '{}')
        """
    )
    conn.execute(
        """
        INSERT INTO product_profile(
            instrument_id,
            ongoing_charges,
            ongoing_charges_asof,
            benchmark_name,
            asset_class_hint,
            domicile_country,
            replication_method,
            hedged_flag,
            updated_at
        ) VALUES (1, 0.12, '2026-03-07', NULL, NULL, NULL, NULL, NULL, '2026-03-07T00:00:00Z')
        """
    )

    rows = load_targets(conn, limit=10, venue="ALL")

    assert [int(row["instrument_id"]) for row in rows] == [1]


def test_load_targets_prioritizes_missing_fee_rows_before_metadata_only_rows() -> None:
    conn = make_conn()
    conn.execute("INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'Amundi', 'Amundi')")
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, universe_mvp_flag, ucits_flag)
        VALUES
            (1, 'LU0000000001', 'Amundi Missing Fee UCITS ETF', 1, 1, 1),
            (2, 'LU0000000002', 'Amundi Metadata Gap UCITS ETF', 1, 1, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO listing(listing_id, instrument_id, ticker, venue_mic, primary_flag, status, trading_currency)
        VALUES
            (1, 1, 'AAA', 'XETR', 1, 'active', 'EUR'),
            (2, 2, 'BBB', 'XETR', 1, 'active', 'EUR')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (2, '2026-03-07', 0.12, 'amundi_factsheet_ok', '{}')
        """
    )
    conn.execute(
        """
        INSERT INTO product_profile(
            instrument_id,
            ongoing_charges,
            ongoing_charges_asof,
            benchmark_name,
            asset_class_hint,
            domicile_country,
            replication_method,
            hedged_flag,
            updated_at
        ) VALUES (2, 0.12, '2026-03-07', NULL, NULL, NULL, NULL, NULL, '2026-03-07T00:00:00Z')
        """
    )

    rows = load_targets(conn, limit=2, venue="ALL")

    assert [int(row["instrument_id"]) for row in rows] == [1, 2]


def test_load_targets_prioritizes_reusable_urls_then_fund_size_gaps() -> None:
    conn = make_conn()
    conn.execute("INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'Amundi', 'Amundi')")
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, universe_mvp_flag, ucits_flag)
        VALUES
            (1, 'LU0000000101', 'Amundi Fund Size Gap UCITS ETF', 1, 1, 1),
            (2, 'LU0000000102', 'Amundi Benchmark Gap UCITS ETF', 1, 1, 1),
            (3, 'LU0000000103', 'Amundi No URL Gap UCITS ETF', 1, 1, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO listing(listing_id, instrument_id, ticker, venue_mic, primary_flag, status, trading_currency)
        VALUES
            (1, 1, 'AAA', 'XETR', 1, 'active', 'EUR'),
            (2, 2, 'BBB', 'XETR', 1, 'active', 'EUR'),
            (3, 3, 'CCC', 'XETR', 1, 'active', 'EUR')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES
            (1, '2026-03-07', 0.12, 'amundi_factsheet_ok', '{}'),
            (2, '2026-03-07', 0.12, 'amundi_factsheet_ok', '{}'),
            (3, '2026-03-07', 0.12, 'amundi_factsheet_ok', '{}')
        """
    )
    conn.executemany(
        """
        INSERT INTO product_profile(
            instrument_id,
            ongoing_charges,
            ongoing_charges_asof,
            benchmark_name,
            asset_class_hint,
            domicile_country,
            fund_size_value,
            replication_method,
            hedged_flag,
            updated_at
        ) VALUES (?, 0.12, '2026-03-07', ?, 'Equity', 'Luxembourg', ?, 'physical', 0, '2026-03-07T00:00:00Z')
        """,
        [
            (1, "MSCI World", None),
            (2, None, 100000000.0),
            (3, "MSCI World", None),
        ],
    )
    conn.executemany(
        """
        INSERT INTO instrument_url_map(instrument_id, url_type, url)
        VALUES (?, 'amundi_monthly_factsheet', ?)
        """,
        [
            (1, "https://example.com/fund-1"),
            (2, "https://example.com/fund-2"),
        ],
    )

    rows = load_targets(conn, limit=3, venue="ALL")

    assert [int(row["instrument_id"]) for row in rows] == [1, 2, 3]


def test_main_commits_successful_rows_before_later_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "amundi.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
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
    ensure_tables_and_view(conn)
    conn.close()

    rows = [
        {"instrument_id": 1, "isin": "LU0000000001", "ticker": "AAA", "instrument_name": "Amundi AAA"},
        {"instrument_id": 2, "isin": "LU0000000002", "ticker": "BBB", "instrument_name": "Amundi BBB"},
    ]

    monkeypatch.setattr(amundi_enrich, "load_targets", lambda conn, limit, venue: rows)
    monkeypatch.setattr(amundi_enrich, "load_existing_factsheet_urls", lambda conn, instrument_ids: {})
    monkeypatch.setattr(amundi_enrich, "discover_monthly_factsheet_urls", lambda client, isins: {})
    monkeypatch.setattr(
        amundi_enrich,
        "build_factsheet_candidates",
        lambda isin, discovered=None, known_url=None: [
            FactsheetCandidate(url=f"https://example.com/{isin}.pdf", source="test")
        ],
    )
    monkeypatch.setattr(
        amundi_enrich,
        "probe_pdf_url",
        lambda client, url: ProbeResult(
            accepted=True,
            reason="ok",
            status_code=200,
            content_type="application/pdf",
            final_url=url,
            method="HEAD",
            first_bytes_hex=None,
        ),
    )
    monkeypatch.setattr(
        amundi_enrich,
        "download_pdf",
        lambda client, url, cache_dir: DownloadResult(
            success=True,
            pdf_bytes=url.encode("utf-8"),
            final_url=url,
            from_cache=False,
            error=None,
            cache_path=None,
            http_status=200,
            content_type="application/pdf",
        ),
    )

    def fake_parse(pdf_bytes: bytes) -> dict[str, object]:
        text = pdf_bytes.decode("utf-8")
        if "LU0000000002" in text:
            raise RuntimeError("boom")
        return {
            "ter": 0.12,
            "use_of_income": "Accumulating",
            "ucits_compliant": 1,
            "benchmark_name": None,
            "asset_class_hint": None,
            "domicile_country": None,
            "replication_method": None,
            "hedged_flag": None,
            "hedged_target": None,
        }

    monkeypatch.setattr(amundi_enrich, "parse_factsheet_pdf", fake_parse)
    monkeypatch.setattr(
        amundi_enrich,
        "try_amundi_kid_fallback",
        lambda client, cache_dir, isin: KidFallbackResult(False, None, None, []),
    )
    monkeypatch.setattr(amundi_enrich, "maybe_update_product_profile", lambda conn, instrument_id, use_of_income: False)

    with pytest.raises(RuntimeError):
        amundi_enrich.main(["--db-path", str(db_path), "--limit", "2"])

    verify = sqlite3.connect(db_path)
    assert verify.execute("SELECT COUNT(*) FROM cost_snapshot").fetchone()[0] == 1
    assert verify.execute("SELECT COUNT(*) FROM issuer_metadata_snapshot").fetchone()[0] == 1
    assert verify.execute("SELECT COUNT(*) FROM instrument_url_map").fetchone()[0] == 1
    verify.close()


def test_insert_cost_snapshot_from_ter_stores_profile_metadata() -> None:
    conn = make_conn()

    insert_cost_snapshot_from_ter(
        conn,
        instrument_id=1,
        asof_date="2026-03-07",
        ter=0.1,
        source_url="https://example.com/amundi",
        use_of_income="Accumulating",
        ucits_compliant=1,
        profile_metadata={
            "benchmark_name": "MSCI World Index",
            "asset_class_hint": "Equity",
            "domicile_country": "Luxembourg",
            "fund_size_value": 300000000.0,
            "fund_size_currency": "EUR",
            "fund_size_asof": "2026-03-07",
            "fund_size_scope": "fund",
            "replication_method": "synthetic",
            "hedged_flag": 1,
            "hedged_target": "USD",
        },
    )
    row = conn.execute("SELECT raw_json FROM cost_snapshot").fetchone()
    payload = json.loads(str(row["raw_json"]))

    assert payload["profile_metadata"]["benchmark_name"] == "MSCI World Index"
    assert payload["profile_metadata"]["fund_size_value"] == 300000000.0
    assert payload["profile_metadata"]["fund_size_currency"] == "EUR"
    assert payload["profile_metadata"]["replication_method"] == "synthetic"
    assert payload["profile_metadata"]["hedged_target"] == "USD"
