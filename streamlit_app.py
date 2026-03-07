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


MISSING_DISPLAY = ""
NOT_APPLICABLE_DISPLAY = "N.A."

ASSET_TYPE_LABELS = {
    "equity": "Equity",
    "bond": "Bond",
    "commodity": "Commodity",
    "cash": "Cash",
    "multi": "Multi-asset",
    "multi_asset": "Multi-asset",
}
REGION_LABELS = {
    "us": "US",
    "uk": "UK",
    "em": "Emerging Markets",
    "global": "Global",
    "europe": "Europe",
    "japan": "Japan",
    "asia": "Asia",
    "apac": "Asia-Pacific",
    "china": "China",
    "switzerland": "Switzerland",
    "canada": "Canada",
    "latin_america": "Latin America",
    "north_america": "North America",
}
SIZE_LABELS = {"large": "Large", "mid": "Mid", "small": "Small"}
STYLE_LABELS = {"value": "Value", "growth": "Growth", "blend": "Blend"}
REPLICATION_LABELS = {"physical": "Physical", "synthetic": "Synthetic"}
BOND_TYPE_LABELS = {
    "government": "Government",
    "corporate": "Corporate",
    "aggregate": "Aggregate",
    "inflation_linked": "Inflation-linked",
    "high_yield": "High yield",
    "em_local": "EM local currency",
    "em_hard_currency": "EM hard currency",
    "securitized": "Securitized",
    "floating_rate": "Floating rate",
}
DURATION_LABELS = {
    "ultra_short": "Ultra-short",
    "short": "Short",
    "intermediate": "Intermediate",
    "long": "Long",
}

EXPLORER_COLUMNS = [
    "Asset type",
    "Issuer",
    "ISIN",
    "Ticker",
    "Fund name",
    "Fund size",
    "Domicile",
    "Distribution",
    "Currency",
    "TER",
    "Region",
    "Size",
    "Style",
    "Sector",
    "Replication",
    "Bond type",
    "Duration",
]

STRATEGY_COLUMNS = [
    "Bucket",
    "Asset type",
    "Issuer",
    "ISIN",
    "Ticker",
    "Fund name",
    "Distribution",
    "Currency",
    "TER",
    "Region",
    "Size",
    "Style",
    "Sector",
    "Bond type",
    "Duration",
]


st.set_page_config(page_title="ETF Atlas", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
    :root {
        --paper: #f6f1e8;
        --panel: rgba(255,255,255,0.88);
        --ink: #15231f;
        --muted: #64716d;
        --line: rgba(21,35,31,0.10);
        --accent: #bd6c46;
        --moss: #21453f;
    }
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; color: var(--ink); }
    h1, h2, h3 { font-family: 'Fraunces', serif; color: var(--ink); letter-spacing: -0.02em; }
    [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top left, rgba(206,187,149,0.25), transparent 22%),
            radial-gradient(circle at 90% 0%, rgba(58,102,92,0.10), transparent 24%),
            linear-gradient(180deg, #f6f1e8 0%, #fbf8f2 50%, #f4efe7 100%);
    }
    [data-testid="stHeader"] { background: rgba(246,241,232,0.72); backdrop-filter: blur(12px); }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #f0e7db, #f7f3eb); border-right: 1px solid var(--line); }
    [data-testid="stSidebar"] * { color: var(--ink) !important; }
    .hero-box, .summary-box, .section-box, .detail-box, .table-shell, .stat-card {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        box-shadow: 0 14px 40px rgba(21,35,31,0.06);
    }
    .hero-box {
        padding: 1.5rem 1.65rem;
        background: linear-gradient(135deg, rgba(31,69,63,0.98), rgba(64,112,102,0.94));
        color: #f8f4ed;
        border-color: rgba(222,204,166,0.22);
    }
    .hero-box h1 { color: #f8f4ed; font-size: 2.6rem; line-height: 1.02; margin: 0 0 0.8rem 0; max-width: 14ch; }
    .hero-box p { color: rgba(248,244,237,0.84); margin: 0; font-size: 1rem; line-height: 1.56; max-width: 58ch; }
    .eyebrow { font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.15em; color: #7a8883; margin-bottom: 0.55rem; font-weight: 600; }
    .hero-box .eyebrow { color: rgba(248,244,237,0.66); }
    .summary-box, .section-box, .detail-box { padding: 1rem 1.1rem; }
    .section-copy, .detail-copy, .table-copy { color: var(--muted); font-size: 0.95rem; line-height: 1.52; margin: 0; }
    .metric-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0.85rem; margin: 1.05rem 0 1.2rem 0; }
    .stat-card { padding: 0.95rem 1rem; }
    .stat-card span { display: block; font-size: 0.75rem; letter-spacing: 0.12em; text-transform: uppercase; color: #788682; margin-bottom: 0.35rem; font-weight: 600; }
    .stat-card strong { display: block; color: var(--ink); font-family: 'Fraunces', serif; font-size: 1.6rem; line-height: 1.05; margin-bottom: 0.2rem; }
    .stat-card em { display: block; font-style: normal; color: var(--muted); font-size: 0.9rem; }
    .chip-row { display: flex; flex-wrap: wrap; gap: 0.5rem; margin: 0.35rem 0 0.15rem 0; }
    .chip { display: inline-flex; align-items: center; border-radius: 999px; padding: 0.34rem 0.74rem; font-size: 0.82rem; font-weight: 600; background: rgba(33,69,63,0.08); border: 1px solid rgba(33,69,63,0.14); color: var(--moss); }
    .chip.accent { background: rgba(189,108,70,0.12); border-color: rgba(189,108,70,0.18); color: #955334; }
    .detail-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0.7rem; margin-top: 0.95rem; }
    .detail-tile { background: rgba(246,241,232,0.86); border: 1px solid var(--line); border-radius: 16px; padding: 0.72rem 0.82rem; }
    .detail-tile span { display: block; font-size: 0.74rem; letter-spacing: 0.1em; text-transform: uppercase; color: #7c8a85; margin-bottom: 0.35rem; font-weight: 600; }
    .detail-tile strong { display: block; color: var(--ink); font-size: 1rem; line-height: 1.42; }
    .detail-meta { color: var(--muted); font-size: 0.92rem; line-height: 1.5; margin-top: 0.15rem; }
    .table-shell { overflow: hidden; }
    .table-scroll { overflow: auto; }
    .browser-table, .strategy-table { width: 100%; border-collapse: collapse; border-spacing: 0; }
    .browser-table { min-width: 1600px; }
    .strategy-table { min-width: 1320px; }
    .browser-table thead th, .strategy-table thead th {
        position: sticky; top: 0; z-index: 2; background: #eef2ec; border-bottom: 1px solid var(--line);
        color: #67746f; font-size: 0.72rem; letter-spacing: 0.1em; text-transform: uppercase;
        text-align: left; padding: 0.82rem 0.78rem; white-space: nowrap;
    }
    .browser-table tbody td, .strategy-table tbody td {
        padding: 0.78rem; border-bottom: 1px solid rgba(21,35,31,0.08); font-size: 0.91rem;
        color: var(--ink); background: rgba(255,255,255,0.5); vertical-align: top;
    }
    .browser-table tbody tr:nth-child(even) td, .strategy-table tbody tr:nth-child(even) td { background: rgba(244,239,231,0.78); }
    .browser-table tbody tr:hover td, .strategy-table tbody tr:hover td { background: rgba(33,69,63,0.08); }
    .table-note { display: flex; justify-content: space-between; align-items: center; gap: 1rem; margin: 0.2rem 0 0.9rem 0; color: var(--muted); font-size: 0.92rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 0.45rem; background: rgba(255,252,248,0.72); border: 1px solid var(--line); border-radius: 999px; padding: 0.32rem; width: fit-content; }
    .stTabs [data-baseweb="tab"] { border-radius: 999px; padding: 0.58rem 1rem; color: var(--muted); font-weight: 600; height: auto; }
    .stTabs [aria-selected="true"] { background: var(--moss); color: #f8f4ed !important; }
    div[data-baseweb="select"] > div, div[data-baseweb="base-input"] > div, [data-testid="stNumberInput"] div[data-baseweb="input"] > div {
        background: rgba(255,255,255,0.88) !important; border: 1px solid var(--line) !important; border-radius: 15px !important; box-shadow: none !important;
    }
    [data-testid="stButton"] button { border-radius: 14px; border: 1px solid var(--line); background: rgba(255,255,255,0.9); color: var(--ink); box-shadow: none; }
    @media (max-width: 1280px) {
        .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .detail-grid { grid-template-columns: 1fr; }
        .hero-box h1 { font-size: 2.25rem; }
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
    return str(Path(secret_path or os.getenv("ETF_APP_DB_PATH", "stage1_etf.db")))


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
    return html.escape(str(value))


def _normalized_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return text


def _normalized_known_text(value: object) -> Optional[str]:
    text = _normalized_text(value)
    if text is None or text.lower() == "unknown":
        return None
    return text


def _token_key(value: object) -> Optional[str]:
    text = _normalized_known_text(value)
    if text is None:
        return None
    return text.lower().replace("-", "_").replace(" ", "_")


def _pretty_label(value: object, mapping: Optional[dict[str, str]] = None) -> Optional[str]:
    key = _token_key(value)
    if key is None:
        return None
    if mapping and key in mapping:
        return mapping[key]
    text = _normalized_known_text(value)
    if text is None:
        return None
    if text.isupper() and len(text) <= 6:
        return text
    return text.replace("_", " ").title()


def _format_percentage(value: object) -> Optional[str]:
    text = _normalized_text(value)
    if text is None:
        return None
    try:
        return f"{float(text):.2f}%"
    except ValueError:
        return None


def _format_fund_size(value: object, currency: object) -> Optional[str]:
    if value is None:
        return None
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None
    abs_amount = abs(amount)
    if abs_amount >= 1_000_000_000:
        scaled, suffix = amount / 1_000_000_000, "bn"
    elif abs_amount >= 1_000_000:
        scaled, suffix = amount / 1_000_000, "m"
    elif abs_amount >= 1_000:
        scaled, suffix = amount / 1_000, "k"
    else:
        scaled, suffix = amount, ""
    number = f"{scaled:.2f}".rstrip("0").rstrip(".") if suffix else f"{scaled:.0f}"
    currency_text = _normalized_known_text(currency)
    return f"{currency_text.upper()} {number}{suffix}".strip() if currency_text else f"{number}{suffix}"


def _format_distribution(value: object) -> Optional[str]:
    text = _normalized_known_text(value)
    if text is None:
        return None
    upper = text.upper()
    if "ACCUM" in upper or "CAPITAL" in upper:
        return "Accumulating"
    if "DISTR" in upper or upper in {"DIST", "DIS"}:
        return "Distributing"
    if "NO INCOME" in upper:
        return "No income"
    return text


def _format_yes_no_optional(value: object) -> str:
    if value in (1, True, "1", "true", "True"):
        return "Yes"
    if value in (0, False, "0", "false", "False"):
        return "No"
    return MISSING_DISPLAY


def _display_value(value: Optional[str]) -> str:
    return value if value not in (None, "", "unknown", "Unknown") else MISSING_DISPLAY


def _display_bond_value(value: Optional[str], *, is_bond: bool) -> str:
    if not is_bond:
        return NOT_APPLICABLE_DISPLAY
    return _display_value(value)


def _compact_values(values: list[str]) -> list[str]:
    return [value for value in values if value]


def _asset_key(item: dict[str, object]) -> Optional[str]:
    return _token_key(item.get("asset_class"))


def _asset_label(item: dict[str, object]) -> str:
    asset_key = _asset_key(item)
    if asset_key is None:
        return MISSING_DISPLAY
    return ASSET_TYPE_LABELS.get(asset_key, asset_key.replace("_", " ").title())


def _selected_filter(value: str, placeholder: str) -> Optional[str]:
    return None if value == placeholder else value


def _selectbox_options(rows: list[dict[str, object]], label: str) -> list[str]:
    values = [str(row["value"]) for row in rows if row.get("value") not in (None, "", "unknown")]
    return [f"Any {label}"] + values


def _toggle_choice(label: str, options: list[str], *, default: str, key: str) -> str:
    if default not in options:
        default = options[0]
    segmented = getattr(st, "segmented_control", None)
    if callable(segmented):
        selected = segmented(label, options=options, default=default, selection_mode="single", key=key)
        return str(selected or default)
    return str(st.radio(label, options, index=options.index(default), horizontal=True, key=key))


def _region_label(item: dict[str, object]) -> str:
    return _display_value(_pretty_label(item.get("geography_region"), REGION_LABELS))


def _equity_size_label(item: dict[str, object]) -> str:
    if _asset_key(item) != "equity":
        return MISSING_DISPLAY
    return _display_value(_pretty_label(item.get("equity_size"), SIZE_LABELS))


def _equity_style_label(item: dict[str, object]) -> str:
    if _asset_key(item) != "equity":
        return MISSING_DISPLAY
    return _display_value(_pretty_label(item.get("equity_style"), STYLE_LABELS))


def _sector_label(item: dict[str, object]) -> str:
    if _asset_key(item) != "equity":
        return MISSING_DISPLAY
    return _display_value(_pretty_label(item.get("sector")))


def _replication_label(value: object) -> str:
    return _display_value(_pretty_label(value, REPLICATION_LABELS))


def _bond_type_label(item: dict[str, object]) -> str:
    return _display_bond_value(_pretty_label(item.get("bond_type"), BOND_TYPE_LABELS), is_bond=_asset_key(item) == "bond")


def _duration_label(item: dict[str, object]) -> str:
    return _display_bond_value(_pretty_label(item.get("duration_bucket"), DURATION_LABELS), is_bond=_asset_key(item) == "bond")


def _fund_table(items: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for item in items:
        rows.append(
            {
                "Asset type": _asset_label(item),
                "Issuer": _display_value(_normalized_known_text(item.get("issuer_name"))),
                "ISIN": _display_value(_normalized_known_text(item.get("isin"))),
                "Ticker": _display_value(_normalized_known_text(item.get("ticker"))),
                "Fund name": _display_value(_normalized_known_text(item.get("instrument_name"))),
                "Fund size": _display_value(_format_fund_size(item.get("fund_size_value"), item.get("fund_size_currency"))),
                "Domicile": _display_value(_pretty_label(item.get("domicile_country"))),
                "Distribution": _display_value(_format_distribution(item.get("distribution_policy"))),
                "Currency": _display_value(_normalized_known_text(item.get("currency"))),
                "TER": _display_value(_format_percentage(item.get("ongoing_charges"))),
                "Region": _region_label(item),
                "Size": _equity_size_label(item),
                "Style": _equity_style_label(item),
                "Sector": _sector_label(item),
                "Replication": _replication_label(item.get("replication_method")),
                "Bond type": _bond_type_label(item),
                "Duration": _duration_label(item),
            }
        )
    return pd.DataFrame(rows, columns=EXPLORER_COLUMNS)


def _strategy_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    formatted_rows: list[dict[str, str]] = []
    for row in rows:
        item = {
            "asset_class": row.get("asset_class"),
            "geography_region": row.get("geography_region"),
            "equity_size": row.get("equity_size"),
            "equity_style": row.get("equity_style"),
            "sector": row.get("sector"),
            "bond_type": row.get("bond_type"),
            "duration_bucket": row.get("duration_bucket"),
        }
        formatted_rows.append(
            {
                "Bucket": _display_value(_pretty_label(row.get("bucket_name"))),
                "Asset type": _asset_label(item),
                "Issuer": _display_value(_normalized_known_text(row.get("issuer_normalized"))),
                "ISIN": _display_value(_normalized_known_text(row.get("ISIN"))),
                "Ticker": _display_value(_normalized_known_text(row.get("ticker"))),
                "Fund name": _display_value(_normalized_known_text(row.get("instrument_name"))),
                "Distribution": _display_value(_format_distribution(row.get("distribution_policy"))),
                "Currency": _display_value(_normalized_known_text(row.get("currency"))),
                "TER": _display_value(_format_percentage(row.get("ongoing_charges"))),
                "Region": _region_label(item),
                "Size": _equity_size_label(item),
                "Style": _equity_style_label(item),
                "Sector": _sector_label(item),
                "Bond type": _bond_type_label(item),
                "Duration": _duration_label(item),
            }
        )
    return pd.DataFrame(formatted_rows, columns=STRATEGY_COLUMNS)


def _coverage_metric(field: dict[str, object]) -> str:
    return f"{field['known']}/{field['total']}"


def _render_metric_grid(cards: list[dict[str, str]]) -> None:
    out = ["<div class='metric-grid'>"]
    for card in cards:
        out.append(
            f"<div class='stat-card'><span>{_escape(card['label'])}</span><strong>{_escape(card['value'])}</strong><em>{_escape(card['meta'])}</em></div>"
        )
    out.append("</div>")
    st.markdown("".join(out), unsafe_allow_html=True)


def _render_chip_row(values: list[str], *, accent_first: bool = False) -> None:
    if not values:
        return
    chips: list[str] = []
    for idx, value in enumerate(values):
        css_class = "chip accent" if accent_first and idx == 0 else "chip"
        chips.append(f"<span class='{css_class}'>{_escape(value)}</span>")
    st.markdown(f"<div class='chip-row'>{''.join(chips)}</div>", unsafe_allow_html=True)


def _render_static_table(df: pd.DataFrame, *, table_class: str, height: int) -> None:
    table_html = df.to_html(index=False, classes=table_class, border=0, escape=True)
    st.markdown(
        f"<div class='table-shell'><div class='table-scroll' style='max-height:{height}px'>{table_html}</div></div>",
        unsafe_allow_html=True,
    )


def _render_fund_detail(detail: dict[str, object]) -> None:
    item = detail
    title = _display_value(_normalized_known_text(detail.get("instrument_name")))
    subtitle = " | ".join(
        _compact_values(
            [
                _display_value(_normalized_known_text(detail.get("issuer_name"))),
                _display_value(_normalized_known_text(detail.get("isin"))),
                _display_value(_normalized_known_text(detail.get("primary_venue"))),
            ]
        )
    )
    chips = _compact_values(
        [
        _asset_label(item),
        _region_label(item),
        _display_value(_pretty_label(detail.get("domicile_country"))),
        _display_value(_format_distribution(detail.get("distribution_policy"))),
        ]
    )
    tiles = [
        ("Ticker", _display_value(_normalized_known_text(detail.get("ticker")))),
        ("Fund size", _display_value(_format_fund_size(detail.get("fund_size_value"), detail.get("fund_size_currency")))),
        ("Fund size as of", _display_value(_normalized_known_text(detail.get("fund_size_asof")))),
        ("Trading currency", _display_value(_normalized_known_text(detail.get("currency")))),
        ("TER", _display_value(_format_percentage(detail.get("ongoing_charges")))),
        ("Benchmark", _display_value(_normalized_known_text(detail.get("benchmark_name")))),
        ("Size", _equity_size_label(item)),
        ("Style", _equity_style_label(item)),
        ("Sector", _sector_label(item)),
        ("Replication", _replication_label(detail.get("replication_method"))),
        ("Bond type", _bond_type_label(item)),
        ("Duration", _duration_label(item)),
        ("Hedged", _format_yes_no_optional(detail.get("hedged_flag"))),
        ("Hedge target", _display_value(_pretty_label(detail.get("hedged_target")))),
    ]
    visible_tiles = [(label, value) for label, value in tiles if value]
    chip_html = "".join(f"<span class='chip'>{_escape(value)}</span>" for value in chips)
    tile_html = "".join(
        f"<div class='detail-tile'><span>{_escape(label)}</span><strong>{_escape(value)}</strong></div>"
        for label, value in visible_tiles
    )
    st.markdown(
        f"""
        <div class="detail-box">
            <div class="eyebrow">Fund details</div>
            <h3>{_escape(title)}</h3>
            <p class="detail-meta">{_escape(subtitle)}</p>
            <div class="chip-row">{chip_html}</div>
            <div class="detail-grid">{tile_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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

st.markdown(
    """
    <div class="hero-box">
        <div class="eyebrow">Singapore UCITS ETF Explorer</div>
        <h1>Find UCITS ETFs that fit your portfolio.</h1>
        <p>Screen the UCITS fund universe by exchange, region, income policy, fund size, structure and cost, then review model portfolio ideas built from the same shortlist.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

_render_metric_grid(
    [
        {"label": "Universe", "value": f"{overview['total_instruments']}", "meta": "UCITS ETFs currently in scope"},
        {"label": "TER shown", "value": _coverage_metric(profile_fields["ongoing_charges"]), "meta": f"{profile_fields['ongoing_charges']['pct']:.2f}% available"},
        {"label": "Domicile", "value": _coverage_metric(profile_fields["domicile_country"]), "meta": f"{profile_fields['domicile_country']['pct']:.2f}% available"},
        {"label": "Fund size", "value": _coverage_metric(profile_fields["fund_size_value"]), "meta": f"{profile_fields['fund_size_value']['pct']:.2f}% available"},
    ]
)

active_view = _toggle_choice("View", ["Explorer", "Strategies", "Coverage"], default="Explorer", key="active_view")
if active_view == "Explorer":
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: block; }
        [data-testid="collapsedControl"] { display: flex; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("## Explorer filters")
    st.sidebar.markdown(
        """
        <div class="summary-box" style="padding:0.9rem 1rem;">
            <p class="section-copy">Blank cells mean the value is not currently available. `N.A.` is used only when bond type or duration do not apply.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    search = st.sidebar.text_input("Search", placeholder="ISIN, fund name, ticker, issuer, benchmark")
    exchange = st.sidebar.selectbox("Exchange", ["Any exchange"] + [str(row["value"]) for row in filters_payload["venue"]])
    asset_class = st.sidebar.selectbox("Asset type", _selectbox_options(filters_payload["asset_class"], "asset type"))
    geography_region = st.sidebar.selectbox("Region", _selectbox_options(filters_payload["geography_region"], "region"))
    equity_size = st.sidebar.selectbox("Size", _selectbox_options(filters_payload["equity_size"], "size"))
    equity_style = st.sidebar.selectbox("Style", _selectbox_options(filters_payload["equity_style"], "style"))
    sector = st.sidebar.selectbox("Sector", _selectbox_options(filters_payload["sector"], "sector"))
    bond_type = st.sidebar.selectbox("Bond type", _selectbox_options(filters_payload["bond_type"], "bond type"))
    currency = st.sidebar.selectbox("Currency", _selectbox_options(filters_payload["currency"], "currency"))
    distribution = st.sidebar.selectbox("Distribution", _selectbox_options(filters_payload["distribution_policy"], "distribution"))
    issuer = st.sidebar.selectbox("Issuer", _selectbox_options(filters_payload["issuer_top"], "issuer"))
    hedged = st.sidebar.selectbox("Hedged", ["Any hedge state", "Yes", "No"])
    page_size = st.sidebar.selectbox("Rows per page", [25, 50, 100], index=1)
else:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

if active_view == "Explorer":
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Fund shortlist</div>
            <h3>Screen the universe and compare funds quickly.</h3>
            <p class="section-copy">Use the filters on the left to narrow the list. Blank cells mean the value is not currently available. `N.A.` appears only when bond type or duration do not apply.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    sort_map = {"Fund name": "name", "Issuer": "issuer", "TER": "fee", "Asset type": "asset_class", "ISIN": "isin"}
    sort_col, direction_col = st.columns([1.25, 0.75], gap="large")
    with sort_col:
        sort_label = _toggle_choice("Sort by", list(sort_map.keys()), default="Fund name", key="browse_sort_field")
    with direction_col:
        direction_label = _toggle_choice("Order", ["Ascending", "Descending"], default="Ascending", key="browse_sort_direction")
    sort_direction = "asc" if direction_label == "Ascending" else "desc"

    fund_params = {"limit": str(page_size), "sort": sort_map[sort_label], "direction": sort_direction}
    if search:
        fund_params["q"] = search
    if selected := _selected_filter(asset_class, "Any asset type"):
        fund_params["asset_class"] = selected
    if selected := _selected_filter(geography_region, "Any region"):
        fund_params["geography_region"] = selected
    if selected := _selected_filter(equity_size, "Any size"):
        fund_params["equity_size"] = selected
    if selected := _selected_filter(equity_style, "Any style"):
        fund_params["equity_style"] = selected
    if selected := _selected_filter(sector, "Any sector"):
        fund_params["sector"] = selected
    if selected := _selected_filter(bond_type, "Any bond type"):
        fund_params["bond_type"] = selected
    if selected := _selected_filter(currency, "Any currency"):
        fund_params["currency"] = selected
    if selected := _selected_filter(distribution, "Any distribution"):
        fund_params["distribution_policy"] = selected
    if selected := _selected_filter(issuer, "Any issuer"):
        fund_params["issuer"] = selected
    if exchange != "Any exchange":
        fund_params["venue"] = exchange
    if hedged == "Yes":
        fund_params["hedged"] = "true"
    elif hedged == "No":
        fund_params["hedged"] = "false"

    active_filters: list[str] = []
    if exchange != "Any exchange":
        active_filters.append(f"Exchange: {exchange}")
    for key, label in (
        ("asset_class", "Asset type"),
        ("geography_region", "Region"),
        ("equity_size", "Size"),
        ("equity_style", "Style"),
        ("sector", "Sector"),
        ("bond_type", "Bond type"),
        ("distribution_policy", "Distribution"),
        ("issuer", "Issuer"),
        ("currency", "Currency"),
    ):
        if fund_params.get(key):
            active_filters.append(f"{label}: {fund_params[key]}")
    if fund_params.get("hedged"):
        active_filters.append(f"Hedged: {fund_params['hedged']}")
    if search:
        active_filters.append(f"Search: {search}")
    _render_chip_row(active_filters or ["All active UCITS ETFs"], accent_first=True)
    st.caption("Blank cells indicate data that is not currently available. `N.A.` is used only for bond type and duration on non-bond funds.")

    filter_signature = tuple(sorted(fund_params.items()))
    if st.session_state.get("browse_filter_signature") != filter_signature:
        st.session_state["browse_filter_signature"] = filter_signature
        st.session_state["browse_page"] = 1

    preview_payload = load_funds_payload(db_path, tuple(sorted({**fund_params, "offset": "0"}.items())))
    total = int(preview_payload["total"])
    max_pages = max(1, math.ceil(total / page_size))
    page = max(1, min(int(st.session_state.get("browse_page", 1)), max_pages))
    st.session_state["browse_page"] = page

    nav_left, nav_mid, nav_right, nav_count = st.columns([0.9, 1.0, 0.9, 1.2], gap="medium")
    with nav_left:
        if st.button("Previous page", disabled=page <= 1, use_container_width=True):
            st.session_state["browse_page"] = max(1, page - 1)
            st.rerun()
    with nav_mid:
        st.markdown(f"<div class='stat-card'><span>Page</span><strong>{page}/{max_pages}</strong><em>{total} matched funds</em></div>", unsafe_allow_html=True)
    with nav_right:
        if st.button("Next page", disabled=page >= max_pages, use_container_width=True):
            st.session_state["browse_page"] = min(max_pages, page + 1)
            st.rerun()
    with nav_count:
        st.markdown(f"<div class='stat-card'><span>Order</span><strong>{_escape(sort_label)}</strong><em>{_escape(direction_label)}</em></div>", unsafe_allow_html=True)

    fund_params["offset"] = str((page - 1) * page_size)
    funds_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))
    st.markdown(f"<div class='table-note'><span>Showing page {page} of {max_pages}</span><span>{funds_payload['total']} funds match the current filters</span></div>", unsafe_allow_html=True)

    left, right = st.columns([2.1, 1.0], gap="large")
    with left:
        fund_table = _fund_table(funds_payload["items"])
        if fund_table.empty:
            st.info("No funds matched the current filters.")
        else:
            _render_static_table(fund_table, table_class="browser-table", height=620)
    with right:
        options = funds_payload["items"]
        if options:
            option_labels = [f"{item['instrument_name']} | {item['isin']}" for item in options]
            selected_label = st.selectbox("Inspect fund", option_labels, key="fund_detail_picker")
            detail = load_fund_detail(db_path, options[option_labels.index(selected_label)]["isin"])
            if detail:
                _render_fund_detail(detail)
        else:
            st.info("No fund details available for the current slice.")

elif active_view == "Strategies":
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Model portfolios</div>
            <h3>Review one strategy at a time.</h3>
            <p class="section-copy">Choose a portfolio template to see the ETFs currently filling each sleeve of the strategy.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ctrl_one, ctrl_two, ctrl_three = st.columns([0.95, 0.85, 1.3], gap="medium")
    strategy_exchange_options = {"All exchanges": "ALL", "London": "XLON", "Xetra": "XETR"}
    strategy_exchange_label = ctrl_one.selectbox("Exchange scope", list(strategy_exchange_options.keys()), key="strategy_venue")
    strategy_venue = strategy_exchange_options[strategy_exchange_label]
    top_n = ctrl_two.selectbox("Funds per sleeve", [1, 2, 3, 4, 5], index=2, key="strategy_top_n")
    preferred_currency_order = ctrl_three.text_input("Preferred trading currencies", value="USD,EUR,GBP", key="strategy_currency_order")
    allow_missing_fees = False
    allow_missing_currency = False

    strategy_payload = load_strategy_payload(db_path, strategy_venue, preferred_currency_order, int(top_n), allow_missing_fees, allow_missing_currency)
    strategy_names = [str(strategy["name"]) for strategy in strategy_payload["strategies"]]
    selected_strategy_name = st.selectbox("Strategy", strategy_names, key="strategy_selector")
    selected_strategy = next(strategy for strategy in strategy_payload["strategies"] if strategy["name"] == selected_strategy_name)

    bucket_summary_rows = []
    for bucket in selected_strategy["buckets"]:
        bucket_name = str(bucket["bucket_name"])
        bucket_summary_rows.append(
            {
                "Bucket": bucket_name.replace("_", " ").title(),
                "Target weight": f"{float(bucket['target_weight']):.0f}%",
                "Funds shown": int(selected_strategy["emitted"].get(bucket_name, 0)),
            }
        )
    min_bucket = min(selected_strategy["emitted"].values()) if selected_strategy["emitted"] else 0

    _render_metric_grid(
        [
            {"label": "Strategy", "value": selected_strategy["name"], "meta": selected_strategy["description"]},
            {"label": "Rows shown", "value": str(len(selected_strategy["rows"])), "meta": "Selected funds across all sleeves"},
            {"label": "Smallest sleeve", "value": str(min_bucket), "meta": "Lowest sleeve count in this run"},
            {"label": "Exchange scope", "value": strategy_exchange_label, "meta": f"Top {top_n} fund(s) per sleeve"},
        ]
    )

    summary_left, summary_right = st.columns([0.95, 1.05], gap="large")
    with summary_left:
        st.markdown("#### Sleeve summary")
        st.table(pd.DataFrame(bucket_summary_rows))
    with summary_right:
        if selected_strategy["rows"]:
            st.markdown("#### Strategy selections")
            _render_static_table(_strategy_table(selected_strategy["rows"]), table_class="strategy-table", height=460)
        else:
            st.info("This strategy did not emit any rows under the current constraints.")

else:
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Coverage</div>
            <h3>Keep the data-quality view available while the client UI improves.</h3>
            <p class="section-copy">This tab stays deliberately operational. It is still the fastest way to spot whether a refresh improved or degraded the labels driving the explorer and strategy views.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _render_metric_grid(
        [
            {"label": "Strict candidates", "value": f"{strict_filters['kept']}/{strict_filters['considered']}", "meta": "Funds surviving hard filters"},
            {"label": "Benchmark labels", "value": _coverage_metric(profile_fields["benchmark_name"]), "meta": f"{profile_fields['benchmark_name']['pct']:.2f}% populated"},
            {"label": "Replication labels", "value": _coverage_metric(profile_fields["replication_method"]), "meta": f"{profile_fields['replication_method']['pct']:.2f}% populated"},
            {"label": "Hedge labels", "value": _coverage_metric(profile_fields["hedged_flag"]), "meta": f"{profile_fields['hedged_flag']['pct']:.2f}% known"},
            {"label": "Domicile labels", "value": _coverage_metric(profile_fields["domicile_country"]), "meta": f"{profile_fields['domicile_country']['pct']:.2f}% populated"},
            {"label": "Fund size labels", "value": _coverage_metric(profile_fields["fund_size_value"]), "meta": f"{profile_fields['fund_size_value']['pct']:.2f}% populated"},
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
        st.dataframe(profile_table, use_container_width=True, hide_index=True, height=340, column_config={"Pct": st.column_config.NumberColumn("Pct", format="%.2f")})

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
