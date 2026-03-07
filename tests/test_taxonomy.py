from __future__ import annotations

import json

from etf_app.taxonomy import classify_instrument


def test_classifies_bulgaria_sofix_as_equity_country() -> None:
    result = classify_instrument(
        isin="BG9000011163",
        instrument_name="EXPAT BULGARIA SOFIX UCITS ETF",
        instrument_type="ETF",
        distribution_policy=None,
    )
    assert result.asset_class == "equity"
    assert result.geography_country == "Bulgaria"
    assert result.geography_region == "europe"
    assert result.equity_region == "europe"


def test_classifies_pfandbriefe_as_bond() -> None:
    result = classify_instrument(
        isin="DE0002635265",
        instrument_name="ISHARES PFANDBRIEFE U.ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.distribution_policy == "Distributing"


def test_classifies_avantis_small_cap_value_from_abbreviation() -> None:
    result = classify_instrument(
        isin="IE0003R87OG3",
        instrument_name="AMC.AV.GLSMCV DLA",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "equity"
    assert result.geography_region == "global"
    assert result.equity_size == "small"
    assert result.equity_style == "value"


def test_classifies_treasury_duration() -> None:
    result = classify_instrument(
        isin="LU1407888053",
        instrument_name="AMUNDI US TREASURY BOND 7-10Y UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "intermediate"


def test_classifies_jpm_global_eq_abbreviation() -> None:
    result = classify_instrument(
        isin="IE0000UW95D6",
        instrument_name="JPM-GLB REI EQ UE EOAH",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "equity"
    assert result.geography_region == "global"


def test_classifies_sp500_as_us_equity() -> None:
    result = classify_instrument(
        isin="LU1681049018",
        instrument_name="AMUNDI S&P 500",
        instrument_type="ETF",
        distribution_policy=None,
    )
    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.geography_region == "us"


def test_classifies_us_with_punctuated_abbreviation() -> None:
    result = classify_instrument(
        isin="IE00BF2B0P08",
        instrument_name="FRK U.S. EQUITY UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.geography_region == "us"


def test_classifies_avantis_global_equity_abbreviation() -> None:
    result = classify_instrument(
        isin="IE000RJECXS5",
        instrument_name="AMC.AV.GL.EQ. DLA",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "equity"
    assert result.geography_scope == "global"
    assert result.geography_region == "global"


def test_classifies_short_duration_corporate_bond_abbreviation() -> None:
    result = classify_instrument(
        isin="IE00BZ17CN18",
        instrument_name="ISH $ ST DUR HY CRP BND ETF USD ACC",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "corp"
    assert result.duration_bucket == "short"


def test_classifies_zero_to_six_month_govt_bond_as_short() -> None:
    result = classify_instrument(
        isin="FR0010754200",
        instrument_name="AMUNDI ETF-GOV.0-6M EO IG",
        instrument_type="ETF",
        distribution_policy=None,
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"
    assert result.duration_years_low == 0.0
    assert result.duration_years_high == 0.5


def test_classifies_short_treasury_bond_as_bond_and_cash_proxy() -> None:
    result = classify_instrument(
        isin="IE00BLRPPV00",
        instrument_name="VANGUARD U.S. TSY 0-1 YR BOND UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"
    assert result.cash_proxy_flag == 1


def test_classifies_zero_to_three_month_treasury_as_short() -> None:
    result = classify_instrument(
        isin="IE00BMD8KM66",
        instrument_name="JPM US TREASURY BOND 0-3MTH UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"
    assert result.duration_years_low == 0.0
    assert result.duration_years_high == 0.25


def test_classifies_abbreviated_high_yield_bd_as_bond() -> None:
    result = classify_instrument(
        isin="IE000IEOQSJ3",
        instrument_name="JPM-EUR HY BD UE EOA",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "corp"


def test_classifies_jtegbi_ten_plus_as_long_govt_bond() -> None:
    result = classify_instrument(
        isin="LU2742533636",
        instrument_name="BPPE-JTEGBI10+ U.ETFEOA",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "long"


def test_classifies_physical_gold_etc_as_gold_commodity() -> None:
    result = classify_instrument(
        isin="IE00B4ND3602",
        instrument_name="ISHARES PHYSICAL GOLD ETC",
        instrument_type="ETC",
        distribution_policy=None,
    )
    assert result.asset_class == "commodity"
    assert result.commodity_type == "gold"
    assert result.gold_flag == 1


def test_gold_miner_proxy_records_proxy_exclusion_evidence() -> None:
    result = classify_instrument(
        isin="IE00B6R52036",
        instrument_name="ISHRS GOLD PRODUCERS ETF USD (ACC)",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    evidence = json.loads(result.evidence_json)
    assert result.asset_class == "equity"
    assert result.gold_flag == 0
    assert "commodity:gold_proxy_equity_excluded" in evidence["rules"]


def test_classifies_abbreviated_world_equity_from_profile_metadata() -> None:
    result = classify_instrument(
        isin="LU1781541179",
        instrument_name="AIS-ACMSCIWS U.ETFDLA",
        instrument_type="ETF",
        distribution_policy="Accumulating",
        benchmark_name="MSCI World SRI Screened Index",
        asset_class_hint="Equity",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "global"


def test_classifies_duration_and_hedge_from_profile_metadata() -> None:
    result = classify_instrument(
        isin="LU0000000001",
        instrument_name="AMUNDI PRIME US TREAS UCITS ETF DR (D)",
        instrument_type="ETF",
        distribution_policy="Distributing",
        benchmark_name="Solactive US Treasury Bond 1-3 Year Index",
        asset_class_hint="Bond",
        domicile_country="Luxembourg",
        hedged_flag=1,
        hedged_target="GBP",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"
    assert result.domicile_country == "Luxembourg"
    assert result.hedged_flag == 1
    assert result.hedged_target == "GBP"


def test_decimal_duration_ranges_are_supported() -> None:
    result = classify_instrument(
        isin="DE0006289473",
        instrument_name="I.EB.R.G.G.1.5-2.5Y UEEOD",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"
    assert result.duration_years_low == 1.5
    assert result.duration_years_high == 2.5


def test_decimal_plus_duration_is_long() -> None:
    result = classify_instrument(
        isin="DE000A0D8Q31",
        instrument_name="I.EB.R.GOV.GE.10.5+ U.ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "long"
    assert result.duration_years_low == 10.5
    assert result.duration_years_high is None


def test_covered_call_is_not_misclassified_as_bond() -> None:
    result = classify_instrument(
        isin="IE0002L5QB31",
        instrument_name="GLOBAL X S&P 500 COVERED CALL UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.factor == "dividend_income"


def test_classifies_europe_large_cap_abbreviation() -> None:
    result = classify_instrument(
        isin="DE0005933980",
        instrument_name="ISH.S.EUR.LARGE 200 U.ETF",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "europe"
    assert result.equity_size == "large"


def test_classifies_apac_ex_japan_as_asia() -> None:
    result = classify_instrument(
        isin="IE000P1WR081",
        instrument_name="HSBC APAC EX JP SUS EQ UC ETF USD (DIST)",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "asia"


def test_classifies_pac_ex_jp_as_asia() -> None:
    result = classify_instrument(
        isin="IE000SGVQIZ9",
        instrument_name="HSBC MSCI PAC EX JP UCITS ETF USD (ACC)",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "asia"


def test_classifies_epra_real_estate_as_sector_equity() -> None:
    result = classify_instrument(
        isin="IE00B5L01S80",
        instrument_name="HSBC FTSE EPRA/NAREIT DEV. UCITS ETF $",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "equity"
    assert result.sector == "real_estate"
    assert result.geography_scope == "sector"
    assert result.geography_region == "global"


def test_classifies_us_equity_compound_name_as_us() -> None:
    result = classify_instrument(
        isin="IE00BYTH6238",
        instrument_name="FIRSTTRUST USEQUITYOPPORTUNITIESUCITS A",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.geography_region == "us"


def test_classifies_hydrogen_theme_as_global_thematic_equity() -> None:
    result = classify_instrument(
        isin="IE00BMDH1538",
        instrument_name="VANECK HYDROGEN ECONOMY UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.theme == "hydrogen"
    assert result.geography_scope == "thematic"
    assert result.geography_region == "global"


def test_worldwide_multi_factor_equity_is_global() -> None:
    result = classify_instrument(
        isin="IE00BKZGB098",
        instrument_name="HSBC MULTI FACTOR WORLDWIDE EQ UCITS ETF",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "global"
    assert result.factor == "multi_factor"


def test_classifies_nasdaq_cybersecurity_as_us_digital() -> None:
    result = classify_instrument(
        isin="IE00BF16M727",
        instrument_name="FIRSTTRUSTNASDAQCYBERSECURITYUCITS",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.theme == "digital"


def test_classifies_sp_smallcap_600_as_us_small_cap() -> None:
    result = classify_instrument(
        isin="IE00B2QWCY14",
        instrument_name="ISHRS S&P SMLL CAP 600 ETF USD (DIST)",
        instrument_type="ETF",
        distribution_policy="Distributing",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "United States"
    assert result.equity_size == "small"


def test_classifies_compressed_stoxx_600_insurance_as_europe_financials() -> None:
    result = classify_instrument(
        isin="LU1834987973",
        instrument_name="MUL-AMUN ST600 INSU ETF A",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "europe"
    assert result.sector == "financials"


def test_classifies_e_g_b_abbreviation_as_govt_bond() -> None:
    result = classify_instrument(
        isin="LU3138596492",
        instrument_name="OSS E.G.B.7-10Y U.ETF 1CE",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )

    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "intermediate"


def test_classifies_canada_from_benchmark_metadata() -> None:
    result = classify_instrument(
        isin="IE00B52SF786",
        instrument_name="ISHRS MSCI CANADA UCITS ETF USD (ACC)",
        instrument_type="ETF",
        distribution_policy="Accumulating",
        benchmark_name="MSCI Developed - Canada in USD - NET TR",
        asset_class_hint="Equity",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "Canada"
    assert result.geography_region == "north_america"


def test_classifies_turkey_from_benchmark_metadata() -> None:
    result = classify_instrument(
        isin="IE00B1FZS574",
        instrument_name="ISHARES MSCI TURKEY UCITS ETF USD (DIST)",
        instrument_type="ETF",
        distribution_policy="Distributing",
        benchmark_name="MSCI Turkey - Net Returns",
        asset_class_hint="Equity",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "Turkey"
    assert result.geography_region == "em"


def test_classifies_emerging_market_bond_from_benchmark_metadata() -> None:
    result = classify_instrument(
        isin="LU1686830909",
        instrument_name="AMUNDI GLOBAL EMERGING BOND",
        instrument_type="ETF",
        distribution_policy="Distributing",
        benchmark_name="J.P. Morgan EMBI Global Diversified Index",
        asset_class_hint="Bond",
    )

    assert result.asset_class == "bond"
    assert result.geography_region == "em"
    assert result.bond_type == "govt"


def test_classifies_sector_and_region_from_benchmark_metadata() -> None:
    result = classify_instrument(
        isin="FR0010688176",
        instrument_name="AMUNDI ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
        benchmark_name="MSCI Europe Banks Index",
        asset_class_hint="Equity",
    )

    assert result.asset_class == "equity"
    assert result.geography_region == "europe"
    assert result.sector == "financials"


def test_classifies_esg_theme_from_benchmark_metadata() -> None:
    result = classify_instrument(
        isin="LU2469335025",
        instrument_name="AMUNDI JAPAN ETF",
        instrument_type="ETF",
        distribution_policy="Distributing",
        benchmark_name="MSCI Japan SRI PAB Index",
        asset_class_hint="Equity",
    )

    assert result.asset_class == "equity"
    assert result.geography_country == "Japan"
    assert result.theme == "esg"
