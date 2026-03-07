from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from etf_app.api import (
    get_completeness_snapshot,
    get_fund_detail,
    get_strategy_snapshot,
    list_filter_options,
    list_funds,
    open_read_conn,
)


st.set_page_config(
    page_title="ETF Atlas",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

    html, body, [class*="css"]  {
        font-family: 'IBM Plex Sans', sans-serif;
    }

    [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top left, rgba(228, 201, 147, 0.34), transparent 28%),
            radial-gradient(circle at top right, rgba(62, 125, 111, 0.18), transparent 30%),
            linear-gradient(180deg, #f5f0e5 0%, #f8f7f3 46%, #eef2ec 100%);
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #183a37 0%, #102826 100%);
    }

    [data-testid="stSidebar"] * {
        color: #f5f0e5;
    }

    .hero-panel {
        background: linear-gradient(135deg, rgba(15, 58, 55, 0.96), rgba(34, 87, 81, 0.92));
        border: 1px solid rgba(224, 198, 153, 0.28);
        border-radius: 22px;
        padding: 1.6rem 1.8rem;
        color: #f7f3e8;
        box-shadow: 0 20px 60px rgba(15, 42, 40, 0.16);
        margin-bottom: 1rem;
    }

    .hero-panel h1 {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2.2rem;
        line-height: 1.05;
        margin: 0 0 0.55rem 0;
        letter-spacing: -0.03em;
    }

    .hero-panel p {
        margin: 0;
        max-width: 58rem;
        color: rgba(247, 243, 232, 0.84);
        font-size: 1rem;
    }

    .section-kicker {
        font-family: 'Space Grotesk', sans-serif;
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 0.76rem;
        color: #9bb7b0;
        margin-bottom: 0.5rem;
    }

    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.82);
        border: 1px solid rgba(17, 32, 43, 0.08);
        border-radius: 18px;
        padding: 0.85rem 1rem;
        box-shadow: 0 12px 30px rgba(17, 32, 43, 0.06);
    }

    .detail-card {
        background: rgba(255, 255, 255, 0.84);
        border: 1px solid rgba(17, 32, 43, 0.08);
        border-radius: 18px;
        padding: 1rem 1.1rem;
        box-shadow: 0 12px 30px rgba(17, 32, 43, 0.06);
    }

    .badge-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
        margin: 0.75rem 0 1rem 0;
    }

    .badge {
        display: inline-block;
        border-radius: 999px;
        background: #e6efe8;
        color: #183a37;
        padding: 0.22rem 0.65rem;
        font-size: 0.78rem;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _db_path() -> str:
    secret_path: Optional[str] = None
    try:
        secret_path = st.secrets.get("db_path")
    except Exception:
        secret_path = None
    raw = secret_path or os.getenv("ETF_APP_DB_PATH", "stage1_etf.db")
    return str(Path(raw))


def _with_conn(sqlite_path: str, fn, *args, **kwargs):
    conn = open_read_conn(sqlite_path)
    try:
        return fn(conn, *args, **kwargs)
    finally:
        conn.close()


@st.cache_data(show_spinner=False, ttl=120)
def load_filters(db_path: str) -> dict[str, object]:
    return _with_conn(db_path, list_filter_options)


@st.cache_data(show_spinner=False, ttl=120)
def load_funds_payload(db_path: str, params_items: tuple[tuple[str, str], ...]) -> dict[str, object]:
    return _with_conn(db_path, list_funds, params=dict(params_items))


@st.cache_data(show_spinner=False, ttl=120)
def load_fund_detail(db_path: str, isin: str) -> Optional[dict[str, object]]:
    return _with_conn(db_path, get_fund_detail, isin)


@st.cache_data(show_spinner=False, ttl=120)
def load_strategy_payload(
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    return _with_conn(
        db_path,
        get_strategy_snapshot,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
    )


@st.cache_data(show_spinner=False, ttl=120)
def load_completeness_payload(db_path: str, venue: str) -> dict[str, object]:
    return _with_conn(
        db_path,
        get_completeness_snapshot,
        db_path=db_path,
        venue=venue,
        preferred_currency_order="USD,EUR,GBP",
        top_n=5,
        allow_missing_fees=False,
        allow_missing_currency=False,
    )


def _selectbox_options(rows: list[dict[str, object]], label: str) -> list[str]:
    values = [str(row["value"]) for row in rows if row.get("value") not in (None, "", "unknown")]
    return [f"Any {label}"] + values


def _selected_filter(value: str, placeholder: str) -> Optional[str]:
    return None if value == placeholder else value


def _fund_table(items: list[dict[str, object]]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(
            columns=[
                "isin",
                "instrument_name",
                "issuer_name",
                "primary_venue",
                "ticker",
                "currency",
                "ongoing_charges",
                "asset_class",
                "geography_region",
                "equity_size",
                "equity_style",
                "factor",
                "sector",
                "theme",
                "bond_type",
                "duration_bucket",
            ]
        )
    df = pd.DataFrame(items)
    columns = [
        "isin",
        "instrument_name",
        "issuer_name",
        "primary_venue",
        "ticker",
        "currency",
        "ongoing_charges",
        "asset_class",
        "geography_region",
        "equity_size",
        "equity_style",
        "factor",
        "sector",
        "theme",
        "bond_type",
        "duration_bucket",
    ]
    out = df[columns].copy()
    out.rename(
        columns={
            "isin": "ISIN",
            "instrument_name": "Fund",
            "issuer_name": "Issuer",
            "primary_venue": "Venue",
            "ticker": "Ticker",
            "currency": "CCY",
            "ongoing_charges": "TER",
            "asset_class": "Asset",
            "geography_region": "Region",
            "equity_size": "Size",
            "equity_style": "Style",
            "factor": "Factor",
            "sector": "Sector",
            "theme": "Theme",
            "bond_type": "Bond Type",
            "duration_bucket": "Duration",
        },
        inplace=True,
    )
    return out


def _coverage_metric(field: dict[str, object]) -> str:
    return f"{field['known']}/{field['total']}"


def _render_fund_detail(detail: dict[str, object]) -> None:
    badges = [
        detail["asset_class"],
        detail.get("geography_region"),
        detail.get("equity_size"),
        detail.get("equity_style"),
        detail.get("factor"),
        detail.get("sector"),
        detail.get("theme"),
        detail.get("bond_type"),
        detail.get("duration_bucket"),
    ]
    badge_html = "".join(
        f"<span class='badge'>{value}</span>"
        for value in badges
        if value not in (None, "", "unknown")
    )
    st.markdown(
        f"""
        <div class="detail-card">
            <div class="section-kicker">Fund Detail</div>
            <h3 style="margin:0 0 0.25rem 0;">{detail['instrument_name']}</h3>
            <div style="color:#50616b; font-size:0.95rem;">{detail['isin']} | {detail['issuer_name']} | {detail['primary_venue']} {detail['ticker'] or ''}</div>
            <div class="badge-row">{badge_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    left, right = st.columns(2)
    with left:
        st.write(
            {
                "currency": detail["currency"],
                "ongoing_charges": detail["ongoing_charges"],
                "distribution_policy": detail["distribution_policy"],
                "benchmark_name": detail["benchmark_name"],
                "domicile_country": detail["domicile_country"],
                "replication_method": detail["replication_method"],
                "hedged_flag": detail["hedged_flag"],
                "hedged_target": detail["hedged_target"],
            }
        )
    with right:
        if detail.get("taxonomy_evidence"):
            st.json(detail["taxonomy_evidence"], expanded=False)


def _strategy_rows_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["bucket_name", "ISIN", "instrument_name"])
    df = pd.DataFrame(rows)
    keep = [
        "bucket_name",
        "ISIN",
        "instrument_name",
        "issuer_normalized",
        "primary_venue",
        "currency",
        "ongoing_charges",
        "asset_class",
        "geography_region",
        "equity_size",
        "equity_style",
        "factor",
        "bond_type",
        "duration_bucket",
    ]
    out = df[keep].copy()
    out.rename(
        columns={
            "bucket_name": "Bucket",
            "ISIN": "ISIN",
            "instrument_name": "Fund",
            "issuer_normalized": "Issuer",
            "primary_venue": "Venue",
            "currency": "CCY",
            "ongoing_charges": "TER",
            "asset_class": "Asset",
            "geography_region": "Region",
            "equity_size": "Size",
            "equity_style": "Style",
            "factor": "Factor",
            "bond_type": "Bond Type",
            "duration_bucket": "Duration",
        },
        inplace=True,
    )
    return out


db_path = _db_path()
db_exists = Path(db_path).exists()

st.markdown(
    """
    <div class="hero-panel">
        <div class="section-kicker">Singapore UCITS ETF Workbench</div>
        <h1>Browse the universe, stress-test taxonomy, and inspect strategy picks in one place.</h1>
        <p>The app is backed by the same normalized SQLite dataset that powers the CLI, completeness report, and predefined recommender.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if not db_exists:
    st.error(f"Database not found at `{db_path}`. Set `ETF_APP_DB_PATH` or add `db_path` to Streamlit secrets.")
    st.stop()

filters_payload = load_filters(db_path)
completeness = load_completeness_payload(db_path, "ALL")

overview = completeness["universe"]["overview"]
profile_fields = completeness["product_profile"]["fields"]
taxonomy = completeness["taxonomy"]

metric_cols = st.columns(4)
metric_cols[0].metric("MVP Universe", f"{overview['total_instruments']}")
metric_cols[1].metric("Fee Coverage", _coverage_metric(profile_fields["ongoing_charges"]), f"{profile_fields['ongoing_charges']['pct']:.2f}%")
metric_cols[2].metric("Equity Geography", _coverage_metric(taxonomy["equity"]["geography_known"]), f"{taxonomy['equity']['geography_known']['pct']:.2f}%")
metric_cols[3].metric("Bond Duration", _coverage_metric(taxonomy["bond"]["duration_bucket_known"]), f"{taxonomy['bond']['duration_bucket_known']['pct']:.2f}%")

st.sidebar.markdown("## Universe Filters")
search = st.sidebar.text_input("Search", placeholder="ISIN, name, ticker, issuer, benchmark")
venue = st.sidebar.selectbox("Venue", ["Any venue"] + [str(row["value"]) for row in filters_payload["venue"]])
asset_class = st.sidebar.selectbox("Asset class", _selectbox_options(filters_payload["asset_class"], "asset class"))
geography_region = st.sidebar.selectbox("Region", _selectbox_options(filters_payload["geography_region"], "region"))
factor = st.sidebar.selectbox("Factor", _selectbox_options(filters_payload["factor"], "factor"))
theme = st.sidebar.selectbox("Theme", _selectbox_options(filters_payload["theme"], "theme"))
bond_type = st.sidebar.selectbox("Bond type", _selectbox_options(filters_payload["bond_type"], "bond type"))
currency = st.sidebar.selectbox("Trading currency", _selectbox_options(filters_payload["currency"], "currency"))
distribution = st.sidebar.selectbox(
    "Distribution",
    _selectbox_options(filters_payload["distribution_policy"], "distribution"),
)
issuer = st.sidebar.selectbox("Top issuer", _selectbox_options(filters_payload["issuer_top"], "issuer"))
hedged = st.sidebar.selectbox("Hedged", ["Any hedge state", "Yes", "No"])
sort = st.sidebar.selectbox(
    "Sort",
    [
        ("name", "Fund name"),
        ("fee", "TER"),
        ("issuer", "Issuer"),
        ("isin", "ISIN"),
        ("venue", "Venue"),
    ],
    format_func=lambda item: item[1],
)
sort_direction = st.sidebar.radio("Sort direction", ["asc", "desc"], horizontal=True)
page_size = st.sidebar.slider("Rows per page", min_value=25, max_value=100, value=50, step=25)

browse_tab, strategies_tab, coverage_tab = st.tabs(["Explorer", "Strategies", "Coverage"])

with browse_tab:
    st.markdown("### Fund Explorer")
    fund_params = {
        "limit": str(page_size),
        "offset": "0",
        "sort": sort[0],
        "direction": sort_direction,
    }
    if search:
        fund_params["q"] = search
    if selected := _selected_filter(asset_class, "Any asset class"):
        fund_params["asset_class"] = selected
    if selected := _selected_filter(geography_region, "Any region"):
        fund_params["geography_region"] = selected
    if selected := _selected_filter(factor, "Any factor"):
        fund_params["factor"] = selected
    if selected := _selected_filter(theme, "Any theme"):
        fund_params["theme"] = selected
    if selected := _selected_filter(bond_type, "Any bond type"):
        fund_params["bond_type"] = selected
    if selected := _selected_filter(currency, "Any currency"):
        fund_params["currency"] = selected
    if selected := _selected_filter(distribution, "Any distribution"):
        fund_params["distribution_policy"] = selected
    if selected := _selected_filter(issuer, "Any issuer"):
        fund_params["issuer"] = selected
    if venue != "Any venue":
        fund_params["venue"] = venue
    if hedged == "Yes":
        fund_params["hedged"] = "true"
    elif hedged == "No":
        fund_params["hedged"] = "false"

    preview_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))
    total = int(preview_payload["total"])
    max_pages = max(1, math.ceil(total / page_size))
    page = st.select_slider("Page", options=list(range(1, max_pages + 1)), value=1 if max_pages == 1 else min(st.session_state.get("browse_page", 1), max_pages), key="browse_page")
    fund_params["offset"] = str((page - 1) * page_size)
    funds_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))

    st.caption(f"{funds_payload['total']} funds matched the current filter set.")
    left, right = st.columns([1.7, 1.0])
    with left:
        table = _fund_table(funds_payload["items"])
        st.dataframe(
            table,
            use_container_width=True,
            hide_index=True,
            column_config={"TER": st.column_config.NumberColumn(format="%.2f")},
        )
    with right:
        options = funds_payload["items"]
        if options:
            labels = {f"{item['isin']} | {item['instrument_name']}": item["isin"] for item in options}
            selected_label = st.selectbox("Inspect current-page fund", list(labels.keys()))
            detail = load_fund_detail(db_path, labels[selected_label])
            if detail:
                _render_fund_detail(detail)
        else:
            st.info("No funds matched the current filters.")

with strategies_tab:
    st.markdown("### Predefined Strategy Engine")
    strat_left, strat_right, strat_extra = st.columns([1.0, 1.0, 1.0])
    strategy_venue = strat_left.selectbox("Venue scope", ["ALL", "XLON", "XETR"])
    top_n = strat_right.slider("Candidates per bucket", min_value=1, max_value=5, value=3)
    preferred_currency_order = strat_extra.text_input("Preferred currencies", value="USD,EUR,GBP")
    allow_missing_fees = st.checkbox("Allow missing fees after strict attempts", value=False)
    allow_missing_currency = st.checkbox("Allow missing currency after strict attempts", value=False)

    strategy_payload = load_strategy_payload(
        db_path,
        strategy_venue,
        preferred_currency_order,
        top_n,
        allow_missing_fees,
        allow_missing_currency,
    )
    st.caption(strategy_payload["gold_policy"]["note"])
    for strategy in strategy_payload["strategies"]:
        st.markdown(f"#### {strategy['name']}")
        st.write(strategy["description"])
        metrics = st.columns(3)
        metrics[0].metric("Rows emitted", str(len(strategy["rows"])))
        metrics[1].metric("Minimum bucket coverage", str(min(strategy["emitted"].values()) if strategy["emitted"] else 0))
        metrics[2].metric("Buckets", str(len(strategy["buckets"])))
        st.dataframe(
            _strategy_rows_table(strategy["rows"]),
            use_container_width=True,
            hide_index=True,
            column_config={"TER": st.column_config.NumberColumn(format="%.2f")},
        )
        with st.expander(f"{strategy['name']} diagnostics", expanded=False):
            st.json(strategy["diagnostics"], expanded=False)
            if strategy["rows"]:
                st.markdown("Selection reason sample")
                st.json(strategy["rows"][0]["selection_reason"], expanded=False)

with coverage_tab:
    st.markdown("### Coverage and Gaps")
    cov_left, cov_right = st.columns(2)
    with cov_left:
        asset_df = pd.DataFrame(completeness["taxonomy"]["asset_class_distribution"])
        asset_df.rename(columns={"asset_class": "Asset class", "count": "Count"}, inplace=True)
        st.markdown("#### Asset class distribution")
        st.bar_chart(asset_df.set_index("Asset class"))
        st.markdown("#### Key profile fields")
        st.write(
            {
                "benchmark_name": profile_fields["benchmark_name"],
                "asset_class_hint": profile_fields["asset_class_hint"],
                "domicile_country": profile_fields["domicile_country"],
                "replication_method": profile_fields["replication_method"],
                "hedged_flag": profile_fields["hedged_flag"],
            }
        )
    with cov_right:
        gap_df = pd.DataFrame(completeness["fee_gaps"]["missing_fees_top_issuers"])
        if not gap_df.empty:
            gap_df.rename(columns={"issuer": "Issuer", "missing_fee_count": "Missing fee rows"}, inplace=True)
            st.markdown("#### Top missing-fee issuers")
            st.bar_chart(gap_df.set_index("Issuer"))
        st.markdown("#### Strategy readiness")
        st.json(completeness["strategy_readiness"]["strict_hard_filters"], expanded=False)

st.caption(f"Database: {db_path}")
