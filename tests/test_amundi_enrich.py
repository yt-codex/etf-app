from __future__ import annotations

from etf_app.amundi_enrich import (
    build_factsheet_candidates,
    extract_distribution,
    extract_fee,
    extract_profile_metadata_from_factsheet,
    select_monthly_factsheet_document,
)


def test_extract_profile_metadata_from_factsheet() -> None:
    text = (
        "BOND "
        "Amundi US Treasury Bond Long Dated UCITS ETF Acc "
        "Benchmark :100% BLOOMBERG BARCLAYS US LONG TREASURY TOTAL RETURN INDEX VALUE "
        "Date of the first NAV :26/07/2021 "
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
    assert parsed["replication_method"] == "physical"
    assert parsed["hedged_flag"] is None
    assert parsed["hedged_target"] is None


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
