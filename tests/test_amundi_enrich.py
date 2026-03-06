from __future__ import annotations

from etf_app.amundi_enrich import extract_profile_metadata_from_factsheet


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
