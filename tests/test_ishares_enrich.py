from __future__ import annotations

import json
import sqlite3

import pytest

import etf_app.ishares_enrich as ishares_enrich
from etf_app.ishares_enrich import (
    HttpResult,
    ensure_tables_and_view,
    insert_cost_snapshot_from_ter,
    load_targets,
)
from etf_app.profile import ensure_product_profile_schema
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
    conn.execute(
        "INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'BlackRock / iShares', 'BlackRock / iShares')"
    )
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, universe_mvp_flag, ucits_flag)
        VALUES (1, 'IE00TEST0001', 'iShares Core MSCI World UCITS ETF', 1, 1, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO listing(listing_id, instrument_id, ticker, venue_mic, primary_flag, status, trading_currency)
        VALUES (1, 1, 'IWDA', 'XLON', 1, 'active', 'USD')
        """
    )
    conn.execute(
        """
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (1, '2026-03-07', 0.20, 'issuer_page_ok', '{}')
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
        ) VALUES (1, 0.20, '2026-03-07', NULL, NULL, NULL, NULL, NULL, '2026-03-07T00:00:00Z')
        """
    )

    rows = load_targets(conn, limit=10, venue="ALL")

    assert [int(row["instrument_id"]) for row in rows] == [1]


def test_load_targets_prioritizes_missing_fee_rows_before_metadata_only_rows() -> None:
    conn = make_conn()
    conn.execute(
        "INSERT INTO issuer(issuer_id, issuer_name, normalized_name) VALUES (1, 'BlackRock iShares', 'BlackRock / iShares')"
    )
    conn.execute(
        """
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, universe_mvp_flag, ucits_flag)
        VALUES
            (1, 'IE0000000001', 'iShares Missing Fee UCITS ETF', 1, 1, 1),
            (2, 'IE0000000002', 'iShares Metadata Gap UCITS ETF', 1, 1, 1)
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
        VALUES (2, '2026-03-07', 0.12, 'issuer_page_ok', '{}')
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


def test_insert_cost_snapshot_from_ter_stores_profile_metadata() -> None:
    conn = make_conn()

    insert_cost_snapshot_from_ter(
        conn,
        instrument_id=1,
        asof_date="2026-03-07",
        ter=0.2,
        source_url="https://example.com/ishares",
        use_of_income="Accumulating",
        ucits_compliant=1,
        profile_metadata={
            "benchmark_name": "MSCI World Index",
            "asset_class_hint": "Equity",
            "domicile_country": "Ireland",
            "replication_method": "physical",
            "hedged_flag": 0,
        },
    )
    row = conn.execute("SELECT raw_json FROM cost_snapshot").fetchone()
    payload = json.loads(str(row["raw_json"]))

    assert payload["profile_metadata"]["benchmark_name"] == "MSCI World Index"
    assert payload["profile_metadata"]["replication_method"] == "physical"
    assert payload["profile_metadata"]["hedged_flag"] == 0


def test_main_commits_successful_rows_before_later_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "ishares.sqlite"
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
        {"instrument_id": 1, "isin": "IE00TEST0001", "ticker": "AAA", "instrument_name": "iShares AAA"},
        {"instrument_id": 2, "isin": "IE00TEST0002", "ticker": "BBB", "instrument_name": "iShares BBB"},
    ]

    class FakeClient:
        def __init__(self, rate_limit: float, timeout: int, max_retries: int) -> None:
            pass

        def get(self, url: str, *, params=None) -> HttpResult:
            return HttpResult(
                ok=True,
                status_code=200,
                content_type="text/html",
                text=url,
                final_url=url,
                error=None,
            )

    def fake_parse(html: str) -> dict[str, object]:
        if "products/2" in html:
            raise RuntimeError("boom")
        return {
            "ter": 0.2,
            "use_of_income": "Accumulating",
            "ucits_compliant": 1,
            "benchmark_name": None,
            "asset_class_hint": None,
            "domicile_country": None,
            "replication_method": None,
            "hedged_flag": None,
            "hedged_target": None,
            "facts": {},
        }

    monkeypatch.setattr(ishares_enrich, "HttpClient", FakeClient)
    monkeypatch.setattr(ishares_enrich, "load_targets", lambda conn, limit, venue: rows)
    monkeypatch.setattr(
        ishares_enrich,
        "find_existing_ishares_product_url",
        lambda conn, instrument_id: f"https://example.com/products/{instrument_id}",
    )
    monkeypatch.setattr(ishares_enrich, "maybe_update_product_profile", lambda conn, instrument_id, use_of_income: False)
    monkeypatch.setattr(ishares_enrich, "parse_ishares_product_page", fake_parse)

    with pytest.raises(RuntimeError):
        ishares_enrich.main(["--db-path", str(db_path), "--limit", "2", "--reuse-only"])

    verify = sqlite3.connect(db_path)
    assert verify.execute("SELECT COUNT(*) FROM cost_snapshot").fetchone()[0] == 1
    assert verify.execute("SELECT COUNT(*) FROM issuer_metadata_snapshot").fetchone()[0] == 1
    assert verify.execute("SELECT COUNT(*) FROM instrument_url_map").fetchone()[0] == 1
    verify.close()
