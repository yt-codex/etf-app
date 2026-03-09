from __future__ import annotations

import json

from etf_app.recommend import BUCKET_OPTIONS, STRATEGIES, build_strategy_rows, match_bucket, summarize_gold_policy
from etf_app.taxonomy import classify_instrument


def make_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "isin": "IE00TEST0001",
        "instrument_name": "MSCI WORLD UCITS ETF",
        "instrument_type": "ETF",
        "leverage_flag": 0,
        "inverse_flag": 0,
        "issuer_normalized": "Issuer",
        "primary_venue": "XLON",
        "ticker": "TST",
        "currency": "USD",
        "distribution_policy": "Accumulating",
        "ongoing_charges": 0.2,
        "ongoing_charges_asof": "2026-03-07",
        "fund_size_value": 250000000.0,
        "fund_size_currency": "USD",
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
        "gold_policy_exception_flag": 0,
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


def test_generic_equity_bucket_matches_plain_vanilla_us_large_value() -> None:
    ok, reasons = match_bucket(
        "equity_us_large_value",
        make_row(
            geography_scope="country",
            geography_region="us",
            equity_size="large",
            equity_style="value",
        ),
    )

    assert ok is True
    assert reasons == [
        "asset_class=equity",
        "geography_region=us",
        "equity_size=large",
        "equity_style=value",
    ]


def test_equity_global_accepts_all_cap_blend_core_market_fund() -> None:
    ok, reasons = match_bucket(
        "equity_global",
        make_row(
            instrument_name="Avantis America Equity UCITS ETF",
            geography_scope="global",
            geography_region="global",
            equity_size="all_cap",
            equity_style="blend",
        ),
    )

    assert ok is True
    assert reasons == ["asset_class=equity", "geography_region=global", "core_scope=global"]


def test_generic_commodity_bucket_allows_broad_commodities() -> None:
    ok, reasons = match_bucket(
        "broad_commodities",
        make_row(
            instrument_type="ETC",
            asset_class="commodity",
            commodity_type="broad_commodities",
        ),
    )

    assert ok is True
    assert reasons == ["asset_class=commodity", "commodity_type=broad_commodities"]


def test_gold_policy_summary_explains_disclosed_exception_gap() -> None:
    summary = summarize_gold_policy(
        eligible_ucits_gold_count=0,
        eligible_non_ucits_exception_gold_count=3,
        ignored_gold_equity_proxy_count=2,
    )
    assert summary.policy_name == "disclosed_non_ucits_physical_gold_exception"
    assert summary.eligible_ucits_gold_count == 0
    assert summary.eligible_non_ucits_exception_gold_count == 3
    assert "No eligible UCITS gold commodity instrument was found" in summary.note
    assert "3 non-UCITS physical gold instrument(s) are available under the disclosed exception" in summary.note
    assert "2 gold miner/producer equity proxy instrument(s) were ignored" in summary.note


def test_build_strategy_rows_can_use_disclosed_non_ucits_gold_exception() -> None:
    strategy = {
        "name": "Test Gold",
        "buckets": (("gold", 20.0),),
    }
    gold_exception_row = make_row(
        isin="JE00BN2CJ301",
        instrument_name="WISDOMTREE CORE PHYSICAL GOLD",
        issuer_normalized="WisdomTree",
        ticker="WGLD",
        asset_class="commodity",
        commodity_type="gold",
        ongoing_charges=0.12,
        gold_policy_exception_flag=1,
    )
    gold_policy = summarize_gold_policy(
        eligible_ucits_gold_count=0,
        eligible_non_ucits_exception_gold_count=1,
        ignored_gold_equity_proxy_count=0,
    )

    rows, emitted, diagnostics = build_strategy_rows(
        strategy,
        [],
        selected_venues=["XLON"],
        top_n=1,
        currency_order=["USD", "EUR", "GBP"],
        allow_missing_fees=False,
        allow_missing_currency=False,
        gold_policy=gold_policy,
        gold_exception_rows=[gold_exception_row],
    )

    assert emitted["gold"] == 1
    assert diagnostics["gold"]["eligible_non_ucits_exception_gold_count"] == 1
    reason = json.loads(rows[0]["selection_reason"])
    assert reason["bucket_policy"] == "disclosed_non_ucits_physical_gold_exception"
    assert reason["bucket_policy_exception"] == "non_ucits_physical_gold"
    assert "non_ucits_gold_exception_disclosed" in reason["filters"]


def test_strategy_catalog_is_expanded_and_unique() -> None:
    assert len(STRATEGIES) >= 20
    names = [str(strategy["name"]) for strategy in STRATEGIES]
    filenames = [str(strategy["filename"]) for strategy in STRATEGIES]
    assert len(names) == len(set(names))
    assert len(filenames) == len(set(filenames))
    assert len(BUCKET_OPTIONS) >= 20
