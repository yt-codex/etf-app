from __future__ import annotations

import html
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


st.set_page_config(page_title="ETF Atlas", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
    :root {
        --paper: #f4efe6;
        --paper-soft: #fcf8f1;
        --panel: rgba(255,255,255,0.78);
        --ink: #17251f;
        --muted: #67736f;
        --line: rgba(23,37,31,0.10);
        --accent: #b86540;
        --moss: #294a44;
        --moss-soft: #dbe8e2;
    }
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; color: var(--ink); }
    h1, h2, h3 { font-family: 'Fraunces', serif; color: var(--ink); letter-spacing: -0.02em; }
    [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top left, rgba(208,189,145,0.28), transparent 24%),
            radial-gradient(circle at 88% 0%, rgba(70,118,107,0.12), transparent 28%),
            linear-gradient(180deg, #f4efe6 0%, #f8f4ec 55%, #f1eee8 100%);
    }
    [data-testid="stHeader"] { background: rgba(244,239,230,0.74); backdrop-filter: blur(10px); }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #ece3d4, #f5f1e8); border-right: 1px solid var(--line); }
    [data-testid="stSidebar"] * { color: var(--ink) !important; }
    .sidebar-note, .section-box, .detail-box, .signal-box, .mini-box {
        background: var(--panel); border: 1px solid var(--line); border-radius: 22px; box-shadow: 0 16px 50px rgba(23,37,31,0.06);
    }
    .sidebar-note { padding: 0.9rem 1rem; color: var(--muted); font-size: 0.92rem; line-height: 1.45; margin-bottom: 0.9rem; }
    .eyebrow { font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.16em; color: #7c8b86; margin-bottom: 0.55rem; font-weight: 600; }
    .hero-box {
        background: linear-gradient(135deg, rgba(32,70,64,0.98), rgba(64,112,102,0.94));
        border: 1px solid rgba(223,206,169,0.26);
        border-radius: 30px;
        box-shadow: 0 24px 70px rgba(19,44,40,0.18);
        color: #f8f3ea;
        padding: 1.55rem 1.65rem;
        min-height: 235px;
    }
    .hero-box .eyebrow { color: rgba(240,231,211,0.70); }
    .hero-box h1 { color: #f8f3ea; font-size: 2.95rem; line-height: 1.0; margin: 0 0 0.8rem 0; max-width: 15ch; }
    .hero-box p { color: rgba(248,243,234,0.84); margin: 0; font-size: 1.02rem; line-height: 1.55; max-width: 60ch; }
    .signal-box, .section-box, .detail-box { padding: 1rem 1.1rem; }
    .signal-grid, .facts-grid { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 0.75rem; }
    .signal-tile, .fact-tile { background: rgba(244,239,230,0.86); border: 1px solid var(--line); border-radius: 16px; padding: 0.75rem 0.8rem; }
    .signal-tile span, .fact-tile span { display:block; font-size:0.75rem; letter-spacing:0.11em; text-transform:uppercase; color:#7a8883; margin-bottom:0.35rem; font-weight:600; }
    .signal-tile strong, .fact-tile strong { display:block; color:var(--ink); font-size:1.02rem; line-height:1.4; }
    .metric-grid { display:grid; grid-template-columns: repeat(5, minmax(0,1fr)); gap: 0.9rem; margin: 1.15rem 0 1.3rem 0; }
    .metric-card, .mini-box { background: var(--panel); border: 1px solid var(--line); border-radius: 20px; padding: 0.95rem 1rem; box-shadow: 0 12px 34px rgba(23,37,31,0.05); }
    .metric-card span, .mini-box span { display:block; font-size:0.75rem; letter-spacing:0.12em; text-transform:uppercase; color:#7a8883; margin-bottom:0.35rem; font-weight:600; }
    .metric-card strong, .mini-box strong { display:block; color:var(--ink); font-family:'Fraunces', serif; font-size:1.75rem; line-height:1.05; margin-bottom:0.2rem; }
    .metric-card em { font-style:normal; color:var(--muted); font-size:0.92rem; }
    .chip-row { display:flex; flex-wrap:wrap; gap:0.55rem; margin:0.25rem 0 0.9rem 0; }
    .chip { display:inline-flex; align-items:center; border-radius:999px; background: rgba(41,74,68,0.08); border:1px solid rgba(41,74,68,0.14); color:var(--moss); padding:0.34rem 0.74rem; font-size:0.83rem; font-weight:600; }
    .chip.accent { background: rgba(184,101,64,0.11); border-color: rgba(184,101,64,0.18); color:#8d4d30; }
    .detail-meta, .section-copy, .table-copy { color: var(--muted); font-size: 0.95rem; line-height: 1.5; }
    .badge-row { display:flex; flex-wrap:wrap; gap:0.45rem; margin:0.85rem 0 0.95rem 0; }
    .badge { display:inline-flex; align-items:center; border-radius:999px; background: var(--moss-soft); color: var(--moss); padding: 0.28rem 0.68rem; font-size:0.8rem; font-weight:600; }
    div[data-baseweb="select"] > div, div[data-baseweb="base-input"] > div, [data-testid="stNumberInput"] div[data-baseweb="input"] > div {
        background: rgba(255,255,255,0.88) !important; border:1px solid var(--line) !important; border-radius:15px !important; box-shadow:none !important;
    }
    div[data-baseweb="select"] *, div[data-baseweb="base-input"] input, [data-testid="stNumberInput"] input { color: var(--ink) !important; }
    [data-testid="stDataFrame"], [data-testid="stJson"], [data-testid="stExpander"] { border-radius: 18px; overflow:hidden; border:1px solid var(--line); box-shadow: 0 12px 34px rgba(23,37,31,0.05); }
    .stTabs [data-baseweb="tab-list"] { gap:0.45rem; background: rgba(255,252,248,0.72); border:1px solid var(--line); border-radius:999px; padding:0.32rem; width:fit-content; }
    .stTabs [data-baseweb="tab"] { border-radius:999px; padding:0.6rem 1rem; color:var(--muted); font-weight:600; height:auto; }
    .stTabs [aria-selected="true"] { background: var(--moss); color:#f8f3ea !important; }
    [data-testid="stMetric"] { background: var(--panel); border:1px solid var(--line); border-radius:18px; box-shadow: 0 10px 30px rgba(23,37,31,0.05); }
    [data-testid="stMetric"] label, [data-testid="stMetric"] [data-testid="stMetricValue"], [data-testid="stMetric"] [data-testid="stMetricDelta"] { color: var(--ink) !important; }
    @media (max-width: 1200px) { .metric-grid { grid-template-columns: repeat(2, minmax(0,1fr)); } .signal-grid, .facts-grid { grid-template-columns:1fr; } .hero-box h1 { font-size:2.4rem; } }
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


def _escape(value: object) -> str:
    return html.escape(str(value if value not in (None, "") else "Unknown"))


def _selected_filter(value: str, placeholder: str) -> Optional[str]:
    return None if value == placeholder else value


def _selectbox_options(rows: list[dict[str, object]], label: str) -> list[str]:
    values = [str(row["value"]) for row in rows if row.get("value") not in (None, "", "unknown")]
    return [f"Any {label}"] + values


def _coverage_metric(field: dict[str, object]) -> str:
    return f"{field['known']}/{field['total']}"


def _render_metric_grid(cards: list[dict[str, str]]) -> None:
    out = ["<div class='metric-grid'>"]
    for card in cards:
        out.append(
            f"<div class='metric-card'><span>{_escape(card['label'])}</span><strong>{_escape(card['value'])}</strong><em>{_escape(card['meta'])}</em></div>"
        )
    out.append("</div>")
    st.markdown("".join(out), unsafe_allow_html=True)


def _render_chip_row(values: list[str], *, accent_first: bool = False) -> None:
    if not values:
        return
    chips = []
    for idx, value in enumerate(values):
        cls = "chip accent" if accent_first and idx == 0 else "chip"
        chips.append(f"<span class='{cls}'>{_escape(value)}</span>")
    st.markdown(f"<div class='chip-row'>{''.join(chips)}</div>", unsafe_allow_html=True)


def _fund_table(items: list[dict[str, object]]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=["ISIN", "Fund", "Issuer", "Venue", "Ticker", "CCY", "TER", "Asset", "Region", "Size", "Style", "Factor", "Sector", "Theme", "Bond Type", "Duration"])
    df = pd.DataFrame(items)[[
        "isin", "instrument_name", "issuer_name", "primary_venue", "ticker", "currency", "ongoing_charges",
        "asset_class", "geography_region", "equity_size", "equity_style", "factor", "sector", "theme",
        "bond_type", "duration_bucket",
    ]].copy()
    df.columns = ["ISIN", "Fund", "Issuer", "Venue", "Ticker", "CCY", "TER", "Asset", "Region", "Size", "Style", "Factor", "Sector", "Theme", "Bond Type", "Duration"]
    return df.fillna("")


def _strategy_rows_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Bucket", "ISIN", "Fund"])
    df = pd.DataFrame(rows)[[
        "bucket_name", "ISIN", "instrument_name", "issuer_normalized", "primary_venue", "currency",
        "ongoing_charges", "asset_class", "geography_region", "equity_size", "equity_style", "factor",
        "bond_type", "duration_bucket",
    ]].copy()
    df.columns = ["Bucket", "ISIN", "Fund", "Issuer", "Venue", "CCY", "TER", "Asset", "Region", "Size", "Style", "Factor", "Bond Type", "Duration"]
    return df.fillna("")


def _render_fund_detail(detail: dict[str, object]) -> None:
    badges = [
        detail["asset_class"], detail.get("geography_region"), detail.get("equity_size"), detail.get("equity_style"),
        detail.get("factor"), detail.get("sector"), detail.get("theme"), detail.get("bond_type"), detail.get("duration_bucket"),
    ]
    badge_html = "".join(
        f"<span class='badge'>{_escape(value)}</span>" for value in badges if value not in (None, "", "unknown")
    )
    facts = [
        ("Trading Currency", detail.get("currency")),
        ("Ongoing Charges", detail.get("ongoing_charges")),
        ("Distribution", detail.get("distribution_policy")),
        ("Benchmark", detail.get("benchmark_name")),
        ("Domicile", detail.get("domicile_country")),
        ("Replication", detail.get("replication_method")),
        ("Hedged", detail.get("hedged_flag")),
        ("Hedge Target", detail.get("hedged_target")),
    ]
    fact_html = "".join(
        f"<div class='fact-tile'><span>{_escape(label)}</span><strong>{_escape(value)}</strong></div>" for label, value in facts
    )
    st.markdown(
        f"""
        <div class="detail-box">
            <div class="eyebrow">Selected fund</div>
            <h3>{_escape(detail['instrument_name'])}</h3>
            <div class="detail-meta">{_escape(detail['isin'])} | {_escape(detail['issuer_name'])} | {_escape(detail['primary_venue'])} {_escape(detail.get('ticker') or '')}</div>
            <div class="badge-row">{badge_html}</div>
            <div class="facts-grid">{fact_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if detail.get("taxonomy_evidence"):
        with st.expander("Taxonomy evidence", expanded=False):
            st.json(detail["taxonomy_evidence"], expanded=False)


db_path = _db_path()
if not Path(db_path).exists():
    st.error(f"Database not found at `{db_path}`. Set `ETF_APP_DB_PATH` or add `db_path` to Streamlit secrets.")
    st.stop()

filters_payload = load_filters(db_path)
completeness = load_completeness_payload(db_path, "ALL")

overview = completeness["universe"]["overview"]
profile_fields = completeness["product_profile"]["fields"]
taxonomy = completeness["taxonomy"]
strict_filters = completeness["strategy_readiness"]["strict_hard_filters"]
fee_gaps = completeness["fee_gaps"]["missing_fees_top_issuers"]

hero_left, hero_right = st.columns([1.7, 1.0], gap="large")
with hero_left:
    st.markdown(
        """
        <div class="hero-box">
            <div class="eyebrow">Singapore UCITS ETF Atlas</div>
            <h1>A cleaner command deck for browsing the ETF universe.</h1>
            <p>Use the explorer to slice the normalized UCITS database, inspect fund labels in context, and sanity-check the predefined strategy engine before this turns into a user-facing product.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with hero_right:
    st.markdown(
        f"""
        <div class="signal-box">
            <div class="eyebrow">Live baseline</div>
            <h3 style="margin:0 0 0.5rem 0;">{strict_filters['kept']} strict-ready funds</h3>
            <p class="section-copy">The database is past the coarse cleanup stage. The current bottleneck is label quality, not raw structure.</p>
            <div class="signal-grid">
                <div class="signal-tile"><span>Fee coverage</span><strong>{profile_fields['ongoing_charges']['pct']:.2f}%</strong></div>
                <div class="signal-tile"><span>Equity geography</span><strong>{taxonomy['equity']['geography_known']['pct']:.2f}%</strong></div>
                <div class="signal-tile"><span>Bond duration</span><strong>{taxonomy['bond']['duration_bucket_known']['pct']:.2f}%</strong></div>
                <div class="signal-tile"><span>Database</span><strong>{_escape(Path(db_path).name)}</strong></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

_render_metric_grid(
    [
        {"label": "Universe", "value": f"{overview['total_instruments']}", "meta": "MVP instruments currently in scope"},
        {"label": "Fees", "value": _coverage_metric(profile_fields["ongoing_charges"]), "meta": f"{profile_fields['ongoing_charges']['pct']:.2f}% TER coverage"},
        {"label": "Benchmarks", "value": _coverage_metric(profile_fields["benchmark_name"]), "meta": f"{profile_fields['benchmark_name']['pct']:.2f}% benchmark labels"},
        {"label": "Equity Geography", "value": _coverage_metric(taxonomy["equity"]["geography_known"]), "meta": f"{taxonomy['equity']['geography_known']['pct']:.2f}% classified"},
        {"label": "Bond Duration", "value": _coverage_metric(taxonomy["bond"]["duration_bucket_known"]), "meta": f"{taxonomy['bond']['duration_bucket_known']['pct']:.2f}% bucketed"},
    ]
)

st.sidebar.markdown("## Filter Rail")
st.sidebar.markdown(
    """
    <div class="sidebar-note">
        Keep the left rail for coarse filtering. The main panel should read like an analyst workbench, not a default admin page.
    </div>
    """,
    unsafe_allow_html=True,
)

search = st.sidebar.text_input("Search", placeholder="ISIN, fund name, ticker, issuer, benchmark")
venue = st.sidebar.selectbox("Venue", ["Any venue"] + [str(row["value"]) for row in filters_payload["venue"]])
asset_class = st.sidebar.selectbox("Asset class", _selectbox_options(filters_payload["asset_class"], "asset class"))
geography_region = st.sidebar.selectbox("Region", _selectbox_options(filters_payload["geography_region"], "region"))
factor = st.sidebar.selectbox("Factor", _selectbox_options(filters_payload["factor"], "factor"))
theme = st.sidebar.selectbox("Theme", _selectbox_options(filters_payload["theme"], "theme"))
bond_type = st.sidebar.selectbox("Bond type", _selectbox_options(filters_payload["bond_type"], "bond type"))
currency = st.sidebar.selectbox("Trading currency", _selectbox_options(filters_payload["currency"], "currency"))
distribution = st.sidebar.selectbox("Distribution", _selectbox_options(filters_payload["distribution_policy"], "distribution"))
issuer = st.sidebar.selectbox("Top issuer", _selectbox_options(filters_payload["issuer_top"], "issuer"))
hedged = st.sidebar.selectbox("Hedged", ["Any hedge state", "Yes", "No"])
sort_options = [("name", "Fund name"), ("fee", "TER"), ("issuer", "Issuer"), ("isin", "ISIN"), ("venue", "Venue")]
sort = st.sidebar.selectbox("Sort", sort_options, format_func=lambda item: item[1])
sort_direction = st.sidebar.radio("Direction", ["asc", "desc"], horizontal=True)
page_size = st.sidebar.slider("Rows per page", min_value=25, max_value=100, value=50, step=25)

browse_tab, strategies_tab, coverage_tab = st.tabs(["Explorer", "Strategies", "Coverage"])

with browse_tab:
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Explorer</div>
            <h3 style="margin:0 0 0.35rem 0;">Move from universe filtering to fund inspection.</h3>
            <p class="section-copy">The table is for breadth. The right-hand panel is where you decide whether the labels actually make sense.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    fund_params = {"limit": str(page_size), "offset": "0", "sort": sort[0], "direction": sort_direction}
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

    active_filters = []
    if venue != "Any venue":
        active_filters.append(f"Venue: {venue}")
    for key, label in (
        ("asset_class", "Asset"),
        ("geography_region", "Region"),
        ("factor", "Factor"),
        ("theme", "Theme"),
        ("bond_type", "Bond"),
        ("distribution_policy", "Distribution"),
        ("issuer", "Issuer"),
    ):
        if fund_params.get(key):
            active_filters.append(f"{label}: {fund_params[key]}")
    if fund_params.get("hedged"):
        active_filters.append(f"Hedged: {fund_params['hedged']}")
    if search:
        active_filters.append(f"Search: {search}")
    active_filters.append(f"Sort: {dict(sort_options)[sort[0]]} {sort_direction}")
    _render_chip_row(active_filters or ["No active filters"], accent_first=True)

    preview_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))
    total = int(preview_payload["total"])
    max_pages = max(1, math.ceil(total / page_size))
    default_page = min(max(1, int(st.session_state.get("browse_page_input", 1))), max_pages)

    top_left, top_mid, top_right = st.columns([1.0, 1.0, 1.2], gap="medium")
    with top_left:
        st.markdown(f"<div class='mini-box'><span>Matched funds</span><strong>{total}</strong></div>", unsafe_allow_html=True)
    with top_mid:
        st.markdown(f"<div class='mini-box'><span>Page size</span><strong>{page_size} rows</strong></div>", unsafe_allow_html=True)
    with top_right:
        page = int(st.number_input("Page", min_value=1, max_value=max_pages, value=default_page, step=1, key="browse_page_input"))

    fund_params["offset"] = str((page - 1) * page_size)
    funds_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))
    st.markdown(
        f"<div class='table-copy'>Showing page {page} of {max_pages}. {funds_payload['total']} funds match the current slice.</div>",
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.7, 1.0], gap="large")
    with left:
        st.dataframe(
            _fund_table(funds_payload["items"]),
            use_container_width=True,
            hide_index=True,
            height=600,
            column_config={"TER": st.column_config.NumberColumn("TER", format="%.2f")},
        )
    with right:
        options = funds_payload["items"]
        if options:
            labels = {f"{item['isin']} | {item['instrument_name']}": item["isin"] for item in options}
            selected_label = st.selectbox("Inspect a fund from the current page", list(labels.keys()))
            detail = load_fund_detail(db_path, labels[selected_label])
            if detail:
                _render_fund_detail(detail)
        else:
            st.info("No funds matched the current filters.")

with strategies_tab:
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Predefined strategies</div>
            <h3 style="margin:0 0 0.35rem 0;">Audit the recommender rather than treating it like a black box.</h3>
            <p class="section-copy">Bucket coverage, selected funds, and diagnostics stay visible so strategy outputs remain explainable.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ctrl_left, ctrl_mid, ctrl_right, ctrl_toggle = st.columns([0.9, 1.0, 1.25, 1.1], gap="medium")
    strategy_venue = ctrl_left.selectbox("Venue scope", ["ALL", "XLON", "XETR"])
    top_n = ctrl_mid.slider("Candidates per bucket", min_value=1, max_value=5, value=3)
    preferred_currency_order = ctrl_right.text_input("Preferred currencies", value="USD,EUR,GBP")
    with ctrl_toggle:
        allow_missing_fees = st.checkbox("Allow missing fees", value=False)
        allow_missing_currency = st.checkbox("Allow missing currency", value=False)

    strategy_payload = load_strategy_payload(
        db_path,
        strategy_venue,
        preferred_currency_order,
        top_n,
        allow_missing_fees,
        allow_missing_currency,
    )
    _render_chip_row([strategy_payload["gold_policy"]["note"]], accent_first=True)

    for strategy in strategy_payload["strategies"]:
        emitted = strategy["emitted"]
        min_bucket = min(emitted.values()) if emitted else 0
        st.markdown(
            f"""
            <div class="section-box">
                <div class="eyebrow">Strategy</div>
                <h3 style="margin:0 0 0.35rem 0;">{_escape(strategy['name'])}</h3>
                <p class="section-copy">{_escape(strategy['description'])}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        _render_metric_grid(
            [
                {"label": "Rows emitted", "value": str(len(strategy["rows"])), "meta": "Selected funds across all buckets"},
                {"label": "Minimum bucket", "value": str(min_bucket), "meta": "Lowest sleeve count"},
                {"label": "Buckets", "value": str(len(strategy["buckets"])), "meta": "Distinct sleeves in the template"},
                {"label": "Venue scope", "value": strategy_venue, "meta": "Applied venue filter"},
                {"label": "Top N", "value": str(top_n), "meta": "Candidates emitted per sleeve"},
            ]
        )
        st.dataframe(
            _strategy_rows_table(strategy["rows"]),
            use_container_width=True,
            hide_index=True,
            height=360,
            column_config={"TER": st.column_config.NumberColumn("TER", format="%.2f")},
        )
        with st.expander(f"{strategy['name']} diagnostics", expanded=False):
            st.json(strategy["diagnostics"], expanded=False)
            if strategy["rows"]:
                st.markdown("Selection reason sample")
                st.json(strategy["rows"][0]["selection_reason"], expanded=False)

with coverage_tab:
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Coverage</div>
            <h3 style="margin:0 0 0.35rem 0;">Keep the data-quality story visible while the UI gets richer.</h3>
            <p class="section-copy">This view should make regressions obvious before they leak into recommendations or fund filters.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _render_metric_grid(
        [
            {"label": "Strict candidates", "value": f"{strict_filters['kept']}/{strict_filters['considered']}", "meta": "Plain-vanilla funds surviving hard filters"},
            {"label": "Benchmark labels", "value": _coverage_metric(profile_fields["benchmark_name"]), "meta": f"{profile_fields['benchmark_name']['pct']:.2f}% populated"},
            {"label": "Replication labels", "value": _coverage_metric(profile_fields["replication_method"]), "meta": f"{profile_fields['replication_method']['pct']:.2f}% populated"},
            {"label": "Hedge labels", "value": _coverage_metric(profile_fields["hedged_flag"]), "meta": f"{profile_fields['hedged_flag']['pct']:.2f}% known"},
            {"label": "Domicile labels", "value": _coverage_metric(profile_fields["domicile_country"]), "meta": f"{profile_fields['domicile_country']['pct']:.2f}% populated"},
        ]
    )

    cov_left, cov_right = st.columns([1.05, 0.95], gap="large")
    with cov_left:
        st.markdown("#### Asset class distribution")
        asset_df = pd.DataFrame(completeness["taxonomy"]["asset_class_distribution"]).rename(columns={"asset_class": "Asset class", "count": "Count"})
        st.bar_chart(asset_df.set_index("Asset class"))

        st.markdown("#### Core profile field coverage")
        profile_table = pd.DataFrame(
            [
                {"Field": key, "Known": value["known"], "Total": value["total"], "Pct": value["pct"]}
                for key, value in profile_fields.items()
                if isinstance(value, dict) and {"known", "total", "pct"} <= set(value.keys())
            ]
        )
        st.dataframe(
            profile_table,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config={"Pct": st.column_config.NumberColumn("Pct", format="%.2f")},
        )

    with cov_right:
        st.markdown("#### Top missing-fee issuers")
        gap_df = pd.DataFrame(fee_gaps).rename(columns={"issuer": "Issuer", "missing_fee_count": "Missing fee rows"})
        if gap_df.empty:
            st.info("No issuer fee gaps detected.")
        else:
            st.bar_chart(gap_df.set_index("Issuer"))
            st.dataframe(gap_df, use_container_width=True, hide_index=True, height=280)

        st.markdown("#### Hard-filter exclusions")
        st.json(strict_filters["excluded"], expanded=False)

st.caption(f"Database: {db_path}")
