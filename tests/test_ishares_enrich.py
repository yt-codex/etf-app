from __future__ import annotations

from etf_app.ishares_enrich import (
    build_discovery_search_terms,
    parse_ishares_product_page,
    score_autocomplete_candidate,
)


def test_parse_ishares_product_page_extracts_profile_metadata() -> None:
    html = """
    <div class="product-data-item">
      <div class="caption">Benchmark Index</div>
      <div class="data">MSCI World Index</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Asset Class</div>
      <div class="data">Equity</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Fund Domicile</div>
      <div class="data">Ireland</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Replication Method</div>
      <div class="data">Physical</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Currency Hedged</div>
      <div class="data">GBP Hedged</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Use of Income</div>
      <div class="data">Accumulating</div>
    </div>
    <div class="product-data-item">
      <div class="caption">UCITS Compliant</div>
      <div class="data">Yes</div>
    </div>
    <div class="product-data-item">
      <div class="caption">Total Expense Ratio</div>
      <div class="data">0.20%</div>
    </div>
    """

    parsed = parse_ishares_product_page(html)

    assert parsed["ter"] == 0.2
    assert parsed["use_of_income"] == "Accumulating"
    assert parsed["ucits_compliant"] == 1
    assert parsed["benchmark_name"] == "MSCI World Index"
    assert parsed["asset_class_hint"] == "Equity"
    assert parsed["domicile_country"] == "Ireland"
    assert parsed["replication_method"] == "physical"
    assert parsed["hedged_flag"] == 1
    assert parsed["hedged_target"] == "GBP"


def test_build_discovery_search_terms_includes_ticker_and_benchmark_fragment() -> None:
    terms = build_discovery_search_terms(
        isin="IE0031442068",
        ticker="IDUS",
        instrument_name="ISHARES S&P 500 UCITS ETF USD (DIST)",
    )

    assert terms[0] == "IE0031442068"
    assert "IDUS" in terms
    assert any("S&P 500" in term for term in terms)


def test_score_autocomplete_candidate_prefers_matching_share_class() -> None:
    dist_score = score_autocomplete_candidate(
        label="iShares Core S&P 500 UCITS ETF USD (Dist)",
        instrument_name="ISHARES S&P 500 UCITS ETF USD (DIST)",
        ticker="IDUS",
        search_term="S&P 500",
    )
    acc_score = score_autocomplete_candidate(
        label="iShares Core S&P 500 UCITS ETF USD (Acc)",
        instrument_name="ISHARES S&P 500 UCITS ETF USD (DIST)",
        ticker="IDUS",
        search_term="S&P 500",
    )

    assert dist_score > acc_score
