from __future__ import annotations

from etf_app.recommend import match_bucket, summarize_gold_policy
from etf_app.taxonomy import classify_instrument


def make_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "isin": "IE00TEST0001",
        "instrument_name": "MSCI WORLD UCITS ETF",
        "asset_class": "equity",
        "geography_scope": "global",
        "geography_region": "global",
        "geography_country": None,
        "equity_size": None,
        "equity_style": None,
        "factor": None,
        "sector": None,
        "theme": None,
        "bond_type": "unknown",
        "duration_bucket": "unknown",
        "commodity_type": "unknown",
        "cash_flag": 0,
        "cash_proxy_flag": 0,
        "govt_bond_flag": 0,
    }
    row.update(overrides)
    return row


def test_equity_global_rejects_single_country_equity() -> None:
    ok, _reasons = match_bucket(
        "equity_global",
        make_row(geography_scope="country", geography_region="us", geography_country="United States"),
    )
    assert ok is False


def test_equity_global_rejects_thematic_world_fund() -> None:
    ok, _reasons = match_bucket(
        "equity_global",
        make_row(instrument_name="LY WORLD WATER (DR) UCITS ETF"),
    )
    assert ok is False


def test_short_bonds_rejects_intermediate_duration() -> None:
    ok, _reasons = match_bucket(
        "short_bonds",
        make_row(asset_class="bond", duration_bucket="intermediate"),
    )
    assert ok is False


def test_gold_producers_classifies_as_equity_not_gold() -> None:
    result = classify_instrument(
        isin="IE00B6R52036",
        instrument_name="ISHRS GOLD PRODUCERS ETF USD (ACC)",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "equity"
    assert result.commodity_type is None
    assert result.gold_flag == 0


def test_trsy_abbreviation_classifies_as_govt_bond() -> None:
    result = classify_instrument(
        isin="IE00B3VWN179",
        instrument_name="ISHRS USD TRSY BOND 1-3YR ETF USD (ACC)",
        instrument_type="ETF",
        distribution_policy="Accumulating",
    )
    assert result.asset_class == "bond"
    assert result.bond_type == "govt"
    assert result.duration_bucket == "short"


def test_gold_bucket_accepts_physical_gold_commodity() -> None:
    ok, reasons = match_bucket(
        "gold",
        make_row(
            instrument_name="ISHARES PHYSICAL GOLD ETC",
            asset_class="commodity",
            commodity_type="gold",
        ),
    )
    assert ok is True
    assert reasons == ["asset_class=commodity", "commodity_type=gold"]


def test_gold_policy_summary_explains_strict_ucits_gap() -> None:
    summary = summarize_gold_policy(
        eligible_ucits_gold_count=0,
        excluded_non_ucits_gold_count=3,
        ignored_gold_equity_proxy_count=2,
    )
    assert summary.policy_name == "strict_ucits_only"
    assert summary.eligible_ucits_gold_count == 0
    assert "No eligible UCITS gold commodity instrument was found" in summary.note
    assert "3 non-UCITS physical gold instrument(s) were excluded" in summary.note
    assert "2 gold miner/producer equity proxy instrument(s) were ignored" in summary.note
