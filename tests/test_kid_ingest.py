from __future__ import annotations

from etf_app.kid_ingest import extract_profile_metadata_from_text


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
