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
