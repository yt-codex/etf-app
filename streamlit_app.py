from __future__ import annotations

import html
import math
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from etf_app.api import (
    get_completeness_snapshot,
    get_custom_strategy_snapshot,
    get_strategy_snapshot,
    list_filter_options,
    list_funds,
    open_read_conn,
)
from etf_app.db_bootstrap import resolve_db_path
from etf_app.recommend import BUCKET_OPTIONS, STRATEGIES


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
BUCKET_LABELS = {str(item["bucket_name"]): str(item["label"]) for item in BUCKET_OPTIONS}
BUCKET_PICKER_LABELS = {str(item["bucket_name"]): str(item["picker_label"]) for item in BUCKET_OPTIONS}
CUSTOM_BUCKET_PLACEHOLDER = "__choose_bucket__"
STRATEGY_UI_TOP_N = 5000
MAX_CUSTOM_BUCKETS = 10
VIEW_OPTIONS = ["Explorer", "Strategies", "Custom", "Coverage"]
CUSTOM_BUCKET_IDS_KEY = "custom_bucket_ids"
CUSTOM_BUCKET_NEXT_ID_KEY = "custom_bucket_next_id"

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

st.set_page_config(page_title="UCITS ETF Atlas", layout="wide", initial_sidebar_state="expanded")

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
    .control-label {
        font-size: 0.82rem;
        font-weight: 600;
        color: var(--ink);
        margin: 0 0 0.45rem 0;
    }
    .filter-tag {
        display: flex;
        align-items: center;
        min-height: 2.35rem;
        border-radius: 999px;
        padding: 0.22rem 0.88rem;
        background: rgba(33,69,63,0.06);
        border: 1px solid rgba(33,69,63,0.12);
        color: var(--moss);
        font-size: 0.88rem;
        font-weight: 600;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
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
    .strategy-grid-head {
        font-size: 0.72rem; letter-spacing: 0.1em; text-transform: uppercase; color: #67746f;
        font-weight: 600; padding: 0.2rem 0 0.35rem 0; white-space: pre-line; line-height: 1.2; min-height: 2rem;
        display: flex; align-items: flex-end;
    }
    .strategy-grid-cell {
        min-height: 2.1rem; display: flex; align-items: center; color: var(--ink);
        font-size: 0.9rem; line-height: 1.35; padding: 0.1rem 0;
    }
    .strategy-grid-cell.muted { color: var(--muted); }
    .strategy-grid-divider {
        border-top: 1px solid rgba(21,35,31,0.08);
        margin: 0.15rem 0 0.45rem 0;
    }
    .table-note { display: flex; justify-content: space-between; align-items: center; gap: 1rem; margin: 0.2rem 0 0.9rem 0; color: var(--muted); font-size: 0.92rem; }
    .view-toggle-spacer { height: 1.15rem; }
    .view-toggle-label { font-size: 1.08rem; font-weight: 600; color: var(--ink); margin: 0 0 0.55rem 0; }
    div[role="radiogroup"] { gap: 0.55rem; }
    div[role="radiogroup"] label {
        background: rgba(255,255,255,0.9);
        border: 1px solid var(--line);
        border-radius: 999px;
        padding: 0.3rem 0.95rem;
        margin-right: 0.15rem;
    }
    div[role="radiogroup"] label p {
        font-size: 1rem !important;
        font-weight: 600 !important;
    }
    div[data-testid="stSegmentedControl"] button {
        font-size: 1rem !important;
        font-weight: 600 !important;
        min-height: 2.9rem !important;
        padding: 0.5rem 1rem !important;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 0.45rem; background: rgba(255,252,248,0.72); border: 1px solid var(--line); border-radius: 999px; padding: 0.32rem; width: fit-content; }
    .stTabs [data-baseweb="tab"] { border-radius: 999px; padding: 0.58rem 1rem; color: var(--muted); font-weight: 600; height: auto; }
    .stTabs [aria-selected="true"] { background: var(--moss); color: #f8f4ed !important; }
    div[data-baseweb="select"] > div, div[data-baseweb="base-input"] > div, [data-testid="stNumberInput"] div[data-baseweb="input"] > div {
        background: rgba(255,255,255,0.88) !important; border: 1px solid var(--line) !important; border-radius: 15px !important; box-shadow: none !important;
    }
    [data-testid="stButton"] button {
        border-radius: 14px; border: 1px solid var(--line); background: rgba(255,255,255,0.9);
        color: var(--ink); box-shadow: none; min-height: 2.8rem; padding: 0.58rem 0.9rem; font-size: 0.98rem; font-weight: 600;
    }
    [data-testid="stButton"] button[kind="primary"],
    [data-testid="stButton"] button[data-testid="baseButton-primary"] {
        width: 2.25rem;
        min-width: 2.25rem;
        max-width: 2.25rem;
        min-height: 2.25rem;
        height: 2.25rem;
        padding: 0;
        border-radius: 999px;
        background: #c64d49 !important;
        border-color: #b2423e !important;
        color: #fff !important;
        box-shadow: none !important;
        font-size: 1.02rem !important;
        line-height: 1 !important;
    }
    [data-testid="stButton"] button[kind="primary"]:hover,
    [data-testid="stButton"] button[data-testid="baseButton-primary"]:hover {
        background: #b2423e !important;
        border-color: #a33a36 !important;
    }
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
    try:
        resolved = resolve_db_path(default_path="stage1_etf.db", secrets=st.secrets, env=os.environ)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc
    return str(resolved)


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
def load_strategy_payload(
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    strategy_name: str,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    return _with_conn(
        db_path,
        get_strategy_snapshot,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=STRATEGY_UI_TOP_N,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
        strategy_name=strategy_name,
    )


@st.cache_data(show_spinner=False, ttl=120)
def load_custom_strategy_payload(
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    bucket_items: tuple[tuple[str, float], ...],
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    return _with_conn(
        db_path,
        get_custom_strategy_snapshot,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=STRATEGY_UI_TOP_N,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
        buckets=[
            {"bucket_name": bucket_name, "target_weight": float(target_weight)}
            for bucket_name, target_weight in bucket_items
        ],
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


def _ensure_state_value(key: str, default: object, options: Optional[list[object]] = None) -> None:
    current = st.session_state.get(key, default)
    if options is not None and current not in options:
        st.session_state[key] = default
        return
    if key not in st.session_state:
        st.session_state[key] = default


def _chunked(items: list[object], size: int) -> list[list[object]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def _toggle_choice(label: str, options: list[str], *, default: str, key: str) -> str:
    if default not in options:
        default = options[0]
    segmented = getattr(st, "segmented_control", None)
    if callable(segmented):
        selected = segmented(label, options=options, default=default, selection_mode="single", key=key)
        return str(selected or default)
    return str(st.radio(label, options, index=options.index(default), horizontal=True, key=key))


def _format_filter_value(field: str, value: object) -> str:
    if field == "asset_class":
        return _display_value(_pretty_label(value, ASSET_TYPE_LABELS))
    if field == "geography_region":
        return _display_value(_pretty_label(value, REGION_LABELS))
    if field == "equity_size":
        return _display_value(_pretty_label(value, SIZE_LABELS))
    if field == "equity_style":
        return _display_value(_pretty_label(value, STYLE_LABELS))
    if field == "sector":
        return _display_value(_pretty_label(value))
    if field == "bond_type":
        return _display_value(_pretty_label(value, BOND_TYPE_LABELS))
    if field == "distribution_policy":
        return _display_value(_format_distribution(value))
    if field == "currency":
        text = _normalized_known_text(value)
        return text.upper() if text else MISSING_DISPLAY
    if field == "issuer":
        return _display_value(_normalized_known_text(value))
    return _display_value(_pretty_label(value))


def _selectbox_label(value: str, *, placeholder: str, field: str) -> str:
    if value == placeholder:
        return value
    return _format_filter_value(field, value)


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


def _bucket_label(value: object) -> str:
    key = str(value or "")
    return _display_value(BUCKET_LABELS.get(key, _pretty_label(key)))


def _strategy_mix_line(buckets: list[dict[str, object]]) -> str:
    parts: list[str] = []
    for bucket in buckets:
        parts.append(f"{float(bucket['target_weight']):.0f}% {_bucket_label(bucket['bucket_name']).lower()}")
    return " / ".join(parts)


def _group_strategy_rows(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        bucket_name = str(row.get("bucket_name") or "")
        grouped.setdefault(bucket_name, []).append(row)
    return grouped


def _strategy_bucket_asset_label(bucket_name: str, selected_row: Optional[dict[str, object]]) -> str:
    if selected_row is not None:
        item = {
            "asset_class": selected_row.get("asset_class"),
            "geography_region": selected_row.get("geography_region"),
            "equity_size": selected_row.get("equity_size"),
            "equity_style": selected_row.get("equity_style"),
            "bond_type": selected_row.get("bond_type"),
            "duration_bucket": selected_row.get("duration_bucket"),
        }
        return _asset_label(item)
    if bucket_name.startswith("equity"):
        return "Equity"
    if "bond" in bucket_name:
        return "Bond"
    if bucket_name == "cash":
        return "Cash"
    if bucket_name in {"gold", "silver", "industrial_metals"} or "commodit" in bucket_name:
        return "Commodity"
    if bucket_name == "multi_asset":
        return "Multi-asset"
    return MISSING_DISPLAY


def _strategy_candidate_label(row: dict[str, object]) -> str:
    ticker = _normalized_known_text(row.get("ticker"))
    venue = _normalized_known_text(row.get("primary_venue"))
    fee = _format_percentage(row.get("ongoing_charges"))
    left = ticker or str(row.get("ISIN") or "")
    if fee:
        left = f"{left} ({fee})"
    if venue:
        return f"{left} | {venue}"
    return left


def _strategy_cell(value: str, *, muted: bool = False) -> str:
    css_class = "strategy-grid-cell muted" if muted else "strategy-grid-cell"
    return f"<div class='{css_class}'>{_escape(value)}</div>"


def _weighted_strategy_ter(rows: list[dict[str, object]]) -> tuple[Optional[float], float, float]:
    weighted_total = 0.0
    covered_weight = 0.0
    total_weight = 0.0
    for row in rows:
        try:
            target_weight = float(row.get("target_weight") or 0.0)
        except (TypeError, ValueError):
            continue
        total_weight += target_weight
        fee = row.get("ongoing_charges")
        try:
            fee_value = float(fee)
        except (TypeError, ValueError):
            continue
        weighted_total += target_weight * fee_value
        covered_weight += target_weight
    if covered_weight <= 0:
        return None, covered_weight, total_weight
    return weighted_total / covered_weight, covered_weight, total_weight


def _render_weighted_ter_card(rows: list[dict[str, object]]) -> None:
    weighted_ter, covered_weight, total_weight = _weighted_strategy_ter(rows)
    weighted_ter_label = _display_value(_format_percentage(weighted_ter))
    weighted_ter_meta = (
        f"Based on {covered_weight:.0f}% of target weight"
        if total_weight > 0
        else "No selected sleeves"
    )
    if total_weight > covered_weight and total_weight > 0:
        weighted_ter_meta = f"{weighted_ter_meta} ({total_weight:.0f}% total target)"
    st.markdown(
        f"<div class='stat-card'><span>Weighted TER</span><strong>{_escape(weighted_ter_label)}</strong><em>{_escape(weighted_ter_meta)}</em></div>",
        unsafe_allow_html=True,
    )


def _custom_bucket_default(index: int) -> tuple[str, float]:
    defaults = [
        ("equity_global", 60.0),
        ("intermediate_govt_bonds", 30.0),
        ("cash", 10.0),
    ]
    if index < len(defaults):
        return defaults[index]
    return CUSTOM_BUCKET_PLACEHOLDER, 0.0


def _custom_bucket_picker_label(bucket_name: str) -> str:
    if bucket_name == CUSTOM_BUCKET_PLACEHOLDER:
        return "Choose bucket"
    return BUCKET_PICKER_LABELS.get(bucket_name, bucket_name)


def _ensure_custom_bucket_rows() -> list[int]:
    row_ids = st.session_state.get(CUSTOM_BUCKET_IDS_KEY)
    if isinstance(row_ids, list) and row_ids:
        if CUSTOM_BUCKET_NEXT_ID_KEY not in st.session_state:
            st.session_state[CUSTOM_BUCKET_NEXT_ID_KEY] = max(int(row_id) for row_id in row_ids) + 1
        return [int(row_id) for row_id in row_ids]

    initial_ids = [0, 1, 2]
    st.session_state[CUSTOM_BUCKET_IDS_KEY] = initial_ids
    st.session_state[CUSTOM_BUCKET_NEXT_ID_KEY] = len(initial_ids)
    for idx, row_id in enumerate(initial_ids):
        default_bucket, default_weight = _custom_bucket_default(idx)
        st.session_state[f"custom_bucket_name_{row_id}"] = default_bucket
        st.session_state[f"custom_bucket_weight_{row_id}"] = default_weight
    return initial_ids


def _add_custom_bucket_row() -> None:
    row_ids = _ensure_custom_bucket_rows()
    if len(row_ids) >= MAX_CUSTOM_BUCKETS:
        return
    row_id = int(st.session_state.get(CUSTOM_BUCKET_NEXT_ID_KEY, len(row_ids)))
    st.session_state[CUSTOM_BUCKET_NEXT_ID_KEY] = row_id + 1
    updated_ids = [*row_ids, row_id]
    st.session_state[CUSTOM_BUCKET_IDS_KEY] = updated_ids
    default_bucket, default_weight = _custom_bucket_default(len(updated_ids) - 1)
    st.session_state[f"custom_bucket_name_{row_id}"] = default_bucket
    st.session_state[f"custom_bucket_weight_{row_id}"] = default_weight


def _remove_custom_bucket_row(row_id: int) -> None:
    row_ids = [int(item) for item in st.session_state.get(CUSTOM_BUCKET_IDS_KEY, [])]
    if row_id not in row_ids or len(row_ids) <= 1:
        return
    st.session_state[CUSTOM_BUCKET_IDS_KEY] = [item for item in row_ids if item != row_id]
    st.session_state.pop(f"custom_bucket_name_{row_id}", None)
    st.session_state.pop(f"custom_bucket_weight_{row_id}", None)


def _collect_custom_bucket_inputs() -> tuple[list[dict[str, object]], float, list[str], bool]:
    rows: list[dict[str, object]] = []
    total_weight = 0.0
    selected_names: list[str] = []
    has_missing_bucket = False
    bucket_options = [CUSTOM_BUCKET_PLACEHOLDER] + list(BUCKET_PICKER_LABELS.keys())

    row_ids = _ensure_custom_bucket_rows()
    for idx, row_id in enumerate(row_ids):
        default_bucket, default_weight = _custom_bucket_default(idx)
        bucket_key = f"custom_bucket_name_{row_id}"
        weight_key = f"custom_bucket_weight_{row_id}"
        _ensure_state_value(bucket_key, default_bucket, bucket_options)
        _ensure_state_value(weight_key, default_weight)

        label_cols = st.columns([1.58, 0.74, 0.22], gap="small")
        label_cols[0].markdown(f"<div class='control-label'>Bucket {idx + 1}</div>", unsafe_allow_html=True)
        label_cols[1].markdown(f"<div class='control-label'>Target weight {idx + 1}</div>", unsafe_allow_html=True)
        label_cols[2].markdown("<div class='control-label'>&nbsp;</div>", unsafe_allow_html=True)

        row_cols = st.columns([1.58, 0.74, 0.22], gap="small")
        with row_cols[0]:
            bucket_name = st.selectbox(
                f"Bucket {idx + 1}",
                bucket_options,
                key=bucket_key,
                format_func=_custom_bucket_picker_label,
                label_visibility="collapsed",
            )
        with row_cols[1]:
            target_weight = st.number_input(
                f"Target weight {idx + 1}",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                format="%.1f",
                key=weight_key,
                label_visibility="collapsed",
            )
        with row_cols[2]:
            st.button(
                "×",
                key=f"custom_bucket_remove_{row_id}",
                help="Remove bucket",
                type="primary",
                disabled=len(row_ids) <= 1,
                on_click=_remove_custom_bucket_row,
                args=(row_id,),
            )
        total_weight += float(target_weight)
        if bucket_name == CUSTOM_BUCKET_PLACEHOLDER:
            has_missing_bucket = True
        else:
            selected_names.append(bucket_name)
        rows.append({"bucket_name": bucket_name, "target_weight": float(target_weight)})

    return rows, total_weight, selected_names, has_missing_bucket


def _reset_explorer_filter(key: str, default: object) -> None:
    st.session_state[key] = default
    st.session_state["browse_page"] = 1


def _render_filter_tag_buttons(tags: list[dict[str, object]]) -> None:
    if not tags:
        _render_chip_row(["All active UCITS ETFs"], accent_first=True)
        return
    for row_index, chunk in enumerate(_chunked(tags, 4)):
        width_spec: list[float] = []
        for tag in chunk:
            label_width = min(2.9, max(1.35, len(str(tag["label"])) * 0.06))
            width_spec.extend([label_width, 0.2])
        width_spec.append(1.0)
        cols = st.columns(width_spec, gap="small")
        col_index = 0
        for tag in chunk:
            with cols[col_index]:
                st.markdown(f"<div class='filter-tag'>{_escape(tag['label'])}</div>", unsafe_allow_html=True)
            with cols[col_index + 1]:
                st.button(
                    "×",
                    key=f"explorer_filter_tag_{row_index}_{tag['state_key']}",
                    help=f"Remove {tag['label']}",
                    type="primary",
                    on_click=_reset_explorer_filter,
                    args=(str(tag["state_key"]), tag["default"]),
                )
            col_index += 2


def _render_strategy_bucket_table(strategy: dict[str, object]) -> list[dict[str, object]]:
    header_labels = [
        "Bucket",
        "Target",
        "Funds\navailable",
        "ASSET\nTYPE",
        "FUND TICKER\n(TER)",
        "ISIN",
        "Fund name",
        "Distribution",
        "Currency",
        "Fund size",
        "Region",
        "Size",
        "Style",
        "Bond type",
        "Duration",
    ]
    column_widths = [1.45, 0.72, 1.0, 1.05, 2.2, 1.25, 2.5, 1.08, 0.9, 1.0, 0.95, 0.8, 0.8, 1.0, 0.9]
    header_cols = st.columns(column_widths, gap="small")
    for col, label in zip(header_cols, header_labels):
        col.markdown(f"<div class='strategy-grid-head'>{_escape(label)}</div>", unsafe_allow_html=True)

    grouped_rows = _group_strategy_rows(list(strategy.get("rows") or []))
    strategy_slug = str(strategy.get("slug") or strategy.get("name") or "strategy")
    selected_rows: list[dict[str, object]] = []

    for bucket in strategy["buckets"]:
        bucket_name = str(bucket["bucket_name"])
        candidates = grouped_rows.get(bucket_name, [])
        selected_row: Optional[dict[str, object]] = None
        row_cols = st.columns(column_widths, gap="small")

        option_ids = [str(candidate["ISIN"]) for candidate in candidates]
        select_key = f"strategy_picker_{strategy_slug}_{bucket_name}"
        if option_ids:
            current_option = st.session_state.get(select_key)
            if current_option not in option_ids:
                st.session_state[select_key] = option_ids[0]
            candidate_map = {str(candidate["ISIN"]): candidate for candidate in candidates}
            with row_cols[4]:
                selected_isin = st.selectbox(
                    f"{strategy_slug}-{bucket_name}",
                    option_ids,
                    format_func=lambda isin: _strategy_candidate_label(candidate_map[isin]),
                    key=select_key,
                    label_visibility="collapsed",
                )
            selected_row = candidate_map[selected_isin]
        else:
            row_cols[4].markdown(_strategy_cell("No candidates", muted=True), unsafe_allow_html=True)

        item = {
            "asset_class": selected_row.get("asset_class") if selected_row else None,
            "geography_region": selected_row.get("geography_region") if selected_row else None,
            "equity_size": selected_row.get("equity_size") if selected_row else None,
            "equity_style": selected_row.get("equity_style") if selected_row else None,
            "bond_type": selected_row.get("bond_type") if selected_row else None,
            "duration_bucket": selected_row.get("duration_bucket") if selected_row else None,
        }

        row_cols[0].markdown(_strategy_cell(_bucket_label(bucket_name)), unsafe_allow_html=True)
        row_cols[1].markdown(_strategy_cell(f"{float(bucket['target_weight']):.0f}%"), unsafe_allow_html=True)
        row_cols[2].markdown(_strategy_cell(str(len(candidates))), unsafe_allow_html=True)
        row_cols[3].markdown(_strategy_cell(_strategy_bucket_asset_label(bucket_name, selected_row)), unsafe_allow_html=True)

        detail_values = [
            _display_value(_normalized_known_text(selected_row.get("ISIN"))) if selected_row else MISSING_DISPLAY,
            _display_value(_normalized_known_text(selected_row.get("instrument_name"))) if selected_row else MISSING_DISPLAY,
            _display_value(_format_distribution(selected_row.get("distribution_policy"))) if selected_row else MISSING_DISPLAY,
            _display_value(_normalized_known_text(selected_row.get("currency"))) if selected_row else MISSING_DISPLAY,
            _display_value(_format_fund_size(selected_row.get("fund_size_value"), selected_row.get("fund_size_currency"))) if selected_row else MISSING_DISPLAY,
            _region_label(item) if selected_row else MISSING_DISPLAY,
            _equity_size_label(item) if selected_row else MISSING_DISPLAY,
            _equity_style_label(item) if selected_row else MISSING_DISPLAY,
            _bond_type_label(item) if selected_row else MISSING_DISPLAY,
            _duration_label(item) if selected_row else MISSING_DISPLAY,
        ]
        for col, value in zip(row_cols[5:], detail_values):
            col.markdown(_strategy_cell(value, muted=value == MISSING_DISPLAY), unsafe_allow_html=True)
        if selected_row is not None:
            selected_rows.append({**selected_row, "target_weight": float(bucket["target_weight"])})
        st.markdown("<div class='strategy-grid-divider'></div>", unsafe_allow_html=True)
    return selected_rows


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


def _render_explorer_table(df: pd.DataFrame, *, height: int) -> None:
    if df.empty:
        return
    rows_html: list[str] = []
    for row_index, row in enumerate(df.to_dict(orient="records")):
        ter_text = str(row.get("TER") or "").strip()
        ter_value = ""
        if ter_text:
            try:
                ter_value = str(float(ter_text.rstrip("%")))
            except ValueError:
                ter_value = ""
        cells: list[str] = []
        for column in df.columns:
            cell_value = _escape(row.get(column, ""))
            cells.append(f"<td>{cell_value}</td>")
        rows_html.append(
            f"<tr data-row-index='{row_index}' data-ter-value='{_escape(ter_value)}'>{''.join(cells)}</tr>"
        )
    table_id = "explorer-fund-table"
    head_cells: list[str] = []
    for column in df.columns:
        if column == "TER":
            head_cells.append(
                "<th>"
                "<button type='button' class='ter-sort-button' data-sort-direction='none' title='Sort displayed rows by TER'>"
                "<span>TER</span>"
                "<span class='ter-sort-icon' aria-hidden='true'><span class='up'>&uarr;</span><span class='down'>&darr;</span></span>"
                "</button>"
                "</th>"
            )
        else:
            head_cells.append(f"<th>{_escape(column)}</th>")
    table_html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <style>
        :root {{
          --paper: #f6f1e8;
          --panel: rgba(255,255,255,0.88);
          --ink: #15231f;
          --muted: #67746f;
          --line: rgba(21,35,31,0.10);
          --moss: #21453f;
        }}
        html, body {{
          margin: 0;
          padding: 0;
          background: transparent;
          color: var(--ink);
          font-family: 'IBM Plex Sans', sans-serif;
        }}
        .table-shell {{
          overflow: hidden;
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 24px;
          box-shadow: 0 14px 40px rgba(21,35,31,0.06);
        }}
        .table-scroll {{
          overflow: auto;
          max-height: {height}px;
        }}
        .browser-table {{
          width: 100%;
          min-width: 1600px;
          border-collapse: collapse;
          border-spacing: 0;
        }}
        .browser-table thead th {{
          position: sticky;
          top: 0;
          z-index: 2;
          background: #eef2ec;
          border-bottom: 1px solid var(--line);
          color: var(--muted);
          font-size: 0.72rem;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          text-align: left;
          padding: 0.82rem 0.78rem;
          white-space: nowrap;
        }}
        .browser-table tbody td {{
          padding: 0.78rem;
          border-bottom: 1px solid rgba(21,35,31,0.08);
          font-size: 0.91rem;
          color: var(--ink);
          background: rgba(255,255,255,0.5);
          vertical-align: top;
        }}
        .browser-table tbody tr:nth-child(even) td {{
          background: rgba(244,239,231,0.78);
        }}
        .browser-table tbody tr:hover td {{
          background: rgba(33,69,63,0.08);
        }}
        .ter-sort-button {{
          display: inline-flex;
          align-items: center;
          gap: 0.42rem;
          border: 0;
          background: transparent;
          padding: 0;
          margin: 0;
          color: inherit;
          font: inherit;
          cursor: pointer;
        }}
        .ter-sort-icon {{
          display: inline-flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          line-height: 0.72;
          font-size: 0.62rem;
          color: #90a09a;
          margin-top: -0.02rem;
        }}
        .ter-sort-icon .up,
        .ter-sort-icon .down {{
          display: block;
        }}
        .ter-sort-button[data-sort-direction="asc"] .ter-sort-icon .up,
        .ter-sort-button[data-sort-direction="desc"] .ter-sort-icon .down {{
          color: var(--moss);
        }}
        .ter-sort-button:hover .ter-sort-icon .up,
        .ter-sort-button:hover .ter-sort-icon .down {{
          color: var(--moss);
        }}
      </style>
    </head>
    <body>
      <div class="table-shell">
        <div class="table-scroll">
          <table id="{table_id}" class="browser-table">
            <thead><tr>{''.join(head_cells)}</tr></thead>
            <tbody>{''.join(rows_html)}</tbody>
          </table>
        </div>
      </div>
      <script>
        (function() {{
          const table = document.getElementById("{table_id}");
          if (!table) return;
          const button = table.querySelector(".ter-sort-button");
          const tbody = table.querySelector("tbody");
          if (!button || !tbody) return;

          const getTerValue = (row) => {{
            const raw = row.getAttribute("data-ter-value") || "";
            if (!raw) return Number.POSITIVE_INFINITY;
            const parsed = Number.parseFloat(raw);
            return Number.isFinite(parsed) ? parsed : Number.POSITIVE_INFINITY;
          }};

          const getRowIndex = (row) => Number.parseInt(row.getAttribute("data-row-index") || "0", 10);

          button.addEventListener("click", function() {{
            const current = button.getAttribute("data-sort-direction");
            const next = current === "asc" ? "desc" : "asc";
            button.setAttribute("data-sort-direction", next);

            const rows = Array.from(tbody.querySelectorAll("tr"));
            rows.sort((left, right) => {{
              const leftValue = getTerValue(left);
              const rightValue = getTerValue(right);
              if (leftValue === rightValue) {{
                return getRowIndex(left) - getRowIndex(right);
              }}
              return next === "asc" ? leftValue - rightValue : rightValue - leftValue;
            }});

            rows.forEach((row) => tbody.appendChild(row));
          }});
        }})();
      </script>
    </body>
    </html>
    """
    components.html(
        table_html,
        height=height + 32,
        scrolling=False,
    )


try:
    db_path = _db_path()
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

filters_payload = load_filters(db_path)
completeness = load_completeness_payload(db_path, "ALL")

profile_fields = completeness["product_profile"]["fields"]
strict_filters = completeness["strategy_readiness"]["strict_hard_filters"]
fee_gaps = completeness["fee_gaps"]["missing_fees_top_issuers"]

st.markdown(
    """
    <div class="hero-box">
        <div class="eyebrow">UCITS ETF Explorer</div>
        <h1>Find UCITS ETFs that fit your portfolio.</h1>
        <p>Screen the UCITS fund universe by exchange, region, income policy, fund size, structure and cost, then review model portfolio ideas built from the same shortlist.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div class='view-toggle-spacer'></div>", unsafe_allow_html=True)
active_view = _toggle_choice("View", VIEW_OPTIONS, default="Explorer", key="active_view")
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
    exchange_options = ["Any exchange"] + [str(row["value"]) for row in filters_payload["venue"]]
    asset_type_options = _selectbox_options(filters_payload["asset_class"], "asset type")
    region_options = _selectbox_options(filters_payload["geography_region"], "region")
    size_options = _selectbox_options(filters_payload["equity_size"], "size")
    style_options = _selectbox_options(filters_payload["equity_style"], "style")
    sector_options = _selectbox_options(filters_payload["sector"], "sector")
    bond_type_options = _selectbox_options(filters_payload["bond_type"], "bond type")
    currency_options = _selectbox_options(filters_payload["currency"], "currency")
    distribution_options = _selectbox_options(filters_payload["distribution_policy"], "distribution")
    issuer_options = _selectbox_options(filters_payload["issuer_top"], "issuer")
    hedged_options = ["Any hedge state", "Yes", "No"]
    page_size_options = [25, 50, 100]

    _ensure_state_value("explorer_search", "")
    _ensure_state_value("explorer_exchange", exchange_options[0], exchange_options)
    _ensure_state_value("explorer_asset_class", asset_type_options[0], asset_type_options)
    _ensure_state_value("explorer_geography_region", region_options[0], region_options)
    _ensure_state_value("explorer_equity_size", size_options[0], size_options)
    _ensure_state_value("explorer_equity_style", style_options[0], style_options)
    _ensure_state_value("explorer_sector", sector_options[0], sector_options)
    _ensure_state_value("explorer_bond_type", bond_type_options[0], bond_type_options)
    _ensure_state_value("explorer_currency", currency_options[0], currency_options)
    _ensure_state_value("explorer_distribution", distribution_options[0], distribution_options)
    _ensure_state_value("explorer_issuer", issuer_options[0], issuer_options)
    _ensure_state_value("explorer_hedged", hedged_options[0], hedged_options)
    _ensure_state_value("explorer_page_size", 50, page_size_options)

    search = st.sidebar.text_input("Search", placeholder="ISIN, fund name, ticker, issuer, benchmark", key="explorer_search")
    exchange = st.sidebar.selectbox("Exchange", exchange_options, key="explorer_exchange")
    asset_class = st.sidebar.selectbox(
        "Asset type",
        asset_type_options,
        key="explorer_asset_class",
        format_func=lambda value: _selectbox_label(value, placeholder=asset_type_options[0], field="asset_class"),
    )
    geography_region = st.sidebar.selectbox(
        "Region",
        region_options,
        key="explorer_geography_region",
        format_func=lambda value: _selectbox_label(value, placeholder=region_options[0], field="geography_region"),
    )
    equity_size = st.sidebar.selectbox(
        "Size",
        size_options,
        key="explorer_equity_size",
        format_func=lambda value: _selectbox_label(value, placeholder=size_options[0], field="equity_size"),
    )
    equity_style = st.sidebar.selectbox(
        "Style",
        style_options,
        key="explorer_equity_style",
        format_func=lambda value: _selectbox_label(value, placeholder=style_options[0], field="equity_style"),
    )
    sector = st.sidebar.selectbox(
        "Sector",
        sector_options,
        key="explorer_sector",
        format_func=lambda value: _selectbox_label(value, placeholder=sector_options[0], field="sector"),
    )
    bond_type = st.sidebar.selectbox(
        "Bond type",
        bond_type_options,
        key="explorer_bond_type",
        format_func=lambda value: _selectbox_label(value, placeholder=bond_type_options[0], field="bond_type"),
    )
    currency = st.sidebar.selectbox(
        "Currency",
        currency_options,
        key="explorer_currency",
        format_func=lambda value: _selectbox_label(value, placeholder=currency_options[0], field="currency"),
    )
    distribution = st.sidebar.selectbox(
        "Distribution",
        distribution_options,
        key="explorer_distribution",
        format_func=lambda value: _selectbox_label(value, placeholder=distribution_options[0], field="distribution_policy"),
    )
    issuer = st.sidebar.selectbox(
        "Issuer",
        issuer_options,
        key="explorer_issuer",
        format_func=lambda value: _selectbox_label(value, placeholder=issuer_options[0], field="issuer"),
    )
    hedged = st.sidebar.selectbox("Hedged", hedged_options, key="explorer_hedged")
    page_size = int(st.sidebar.selectbox("Rows per page", page_size_options, key="explorer_page_size"))
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
    fund_params = {
        "limit": str(page_size),
        "sort": "name",
        "direction": "asc",
    }
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

    active_filters: list[dict[str, object]] = []
    if exchange != "Any exchange":
        active_filters.append({"label": f"Exchange: {exchange}", "state_key": "explorer_exchange", "default": exchange_options[0]})
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
            state_key = {
                "asset_class": "explorer_asset_class",
                "geography_region": "explorer_geography_region",
                "equity_size": "explorer_equity_size",
                "equity_style": "explorer_equity_style",
                "sector": "explorer_sector",
                "bond_type": "explorer_bond_type",
                "distribution_policy": "explorer_distribution",
                "issuer": "explorer_issuer",
                "currency": "explorer_currency",
            }[key]
            default_value = {
                "explorer_asset_class": asset_type_options[0],
                "explorer_geography_region": region_options[0],
                "explorer_equity_size": size_options[0],
                "explorer_equity_style": style_options[0],
                "explorer_sector": sector_options[0],
                "explorer_bond_type": bond_type_options[0],
                "explorer_distribution": distribution_options[0],
                "explorer_issuer": issuer_options[0],
                "explorer_currency": currency_options[0],
            }[state_key]
            active_filters.append(
                {
                    "label": f"{label}: {_format_filter_value(key, fund_params[key])}",
                    "state_key": state_key,
                    "default": default_value,
                }
            )
    if fund_params.get("hedged"):
        active_filters.append({"label": f"Hedged: {hedged}", "state_key": "explorer_hedged", "default": hedged_options[0]})
    if search:
        active_filters.append({"label": f"Search: {search}", "state_key": "explorer_search", "default": ""})
    _render_filter_tag_buttons(active_filters)

    filter_signature = tuple(sorted(fund_params.items()))
    if st.session_state.get("browse_filter_signature") != filter_signature:
        st.session_state["browse_filter_signature"] = filter_signature
        st.session_state["browse_page"] = 1

    preview_payload = load_funds_payload(db_path, tuple(sorted({**fund_params, "offset": "0"}.items())))
    total = int(preview_payload["total"])
    max_pages = max(1, math.ceil(total / page_size))
    page = max(1, min(int(st.session_state.get("browse_page", 1)), max_pages))
    st.session_state["browse_page"] = page

    nav_left, nav_mid, nav_right = st.columns([0.9, 1.0, 0.9], gap="medium")
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

    fund_params["offset"] = str((page - 1) * page_size)
    funds_payload = load_funds_payload(db_path, tuple(sorted(fund_params.items())))
    st.markdown(
        f"<div class='table-note'><span>Showing page {page} of {max_pages}</span><span>{funds_payload['total']} funds match the current filters</span></div>",
        unsafe_allow_html=True,
    )

    fund_table = _fund_table(funds_payload["items"])
    if fund_table.empty:
        st.info("No funds matched the current filters.")
    else:
        _render_explorer_table(fund_table, height=620)

elif active_view == "Strategies":
    strategy_catalog = {str(strategy["name"]): strategy for strategy in STRATEGIES}
    selected_strategy_name = st.selectbox("Strategy", list(strategy_catalog.keys()), key="strategy_selector")

    ctrl_one, ctrl_two = st.columns([0.95, 1.3], gap="medium")
    strategy_exchange_options = {"All exchanges": "ALL", "London": "XLON", "Xetra": "XETR"}
    strategy_exchange_label = ctrl_one.selectbox("Exchange scope", list(strategy_exchange_options.keys()), key="strategy_venue")
    strategy_venue = strategy_exchange_options[strategy_exchange_label]
    preferred_currency_order = ctrl_two.text_input("Preferred trading currencies", value="USD,EUR,GBP", key="strategy_currency_order")
    allow_missing_fees = False
    allow_missing_currency = False

    strategy_payload = load_strategy_payload(
        db_path,
        strategy_venue,
        preferred_currency_order,
        selected_strategy_name,
        allow_missing_fees,
        allow_missing_currency,
    )
    if not strategy_payload["strategies"]:
        st.info("This strategy is not available under the current constraints.")
    else:
        selected_strategy = strategy_payload["strategies"][0]
        st.caption("Each sleeve is shown once. Pick a candidate ticker inside the row to inspect the matching UCITS implementation for that bucket.")
        source_url = str(selected_strategy.get("source_url") or "")
        source_html = (
            f"<p class='section-copy' style='margin-top:0.7rem;'>Inspired by the allocation at "
            f"<a href='{_escape(source_url)}' target='_blank'>Lazy Portfolio ETF</a>.</p>"
            if source_url
            else ""
        )
        st.markdown(
            f"""
            <div class="section-box">
                <div class="eyebrow">Strategy brief</div>
                <h3>{_escape(selected_strategy['name'])}</h3>
                <p class="section-copy">{_escape(str(selected_strategy.get('description') or ''))}</p>
                <p class="section-copy" style="margin-top:0.7rem;"><strong>Construction.</strong> {_escape(_strategy_mix_line(selected_strategy['buckets']))}.</p>
                <p class="section-copy" style="margin-top:0.55rem;">{_escape(str(selected_strategy.get('detail') or ''))}</p>
                <p class="section-copy" style="margin-top:0.55rem;"><strong>Implementation note.</strong> {_escape(str(selected_strategy.get('implementation_note') or ''))}</p>
                {source_html}
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Funds available are unbounded in the UI: each sleeve includes every ranked candidate returned by the final bucket filter.")
        weighted_ter_slot = st.empty()
        selected_rows = _render_strategy_bucket_table(selected_strategy)
        with weighted_ter_slot.container():
            _render_weighted_ter_card(selected_rows)

elif active_view == "Custom":
    st.markdown(
        """
        <div class="section-box">
            <div class="eyebrow">Custom bucket mix</div>
            <h3>Build your own allocation from the bucket library.</h3>
            <p class="section-copy">Choose up to 10 sleeves, assign their target weights, and keep the total at exactly 100% before the shortlist is generated.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    custom_ctrl_one, custom_ctrl_two = st.columns([0.95, 1.3], gap="medium")
    custom_exchange_options = {"All exchanges": "ALL", "London": "XLON", "Xetra": "XETR"}
    custom_exchange_label = custom_ctrl_one.selectbox("Exchange scope", list(custom_exchange_options.keys()), key="custom_venue")
    custom_venue = custom_exchange_options[custom_exchange_label]
    custom_currency_order = custom_ctrl_two.text_input(
        "Preferred trading currencies",
        value="USD,EUR,GBP",
        key="custom_currency_order",
    )
    custom_rows, custom_total_weight, custom_selected_names, custom_has_missing_bucket = _collect_custom_bucket_inputs()
    current_bucket_count = len(_ensure_custom_bucket_rows())
    add_col, _ = st.columns([0.28, 0.72], gap="small")
    with add_col:
        st.button(
            "+ Add bucket",
            key="custom_bucket_add",
            use_container_width=True,
            disabled=current_bucket_count >= MAX_CUSTOM_BUCKETS,
            on_click=_add_custom_bucket_row,
        )
    if current_bucket_count >= MAX_CUSTOM_BUCKETS:
        st.caption("Maximum 10 buckets reached.")
    duplicate_bucket_names = sorted({name for name in custom_selected_names if custom_selected_names.count(name) > 1})
    valid_total = math.isclose(custom_total_weight, 100.0, abs_tol=0.05)

    if custom_has_missing_bucket:
        st.warning("Choose a bucket for every row before generating the shortlist.")
    if duplicate_bucket_names:
        duplicate_labels = ", ".join(_bucket_label(name) for name in duplicate_bucket_names)
        st.warning(f"Use each bucket only once. Duplicate selections: {duplicate_labels}.")
    if not valid_total:
        st.warning(f"Target weights currently add to {custom_total_weight:.1f}%. Adjust them to 100.0%.")
    else:
        st.caption("Weights sum to 100.0%.")

    if custom_has_missing_bucket or duplicate_bucket_names or not valid_total:
        st.info("Complete the bucket selection and make the weights sum to 100% to render the custom shortlist.")
    else:
        custom_payload = load_custom_strategy_payload(
            db_path,
            custom_venue,
            custom_currency_order,
            tuple(
                (str(row["bucket_name"]), float(row["target_weight"]))
                for row in custom_rows
            ),
            False,
            False,
        )
        custom_strategy = custom_payload["strategies"][0]
        st.caption("Funds available are unbounded in the UI: each sleeve includes every ranked candidate returned by the final bucket filter.")
        weighted_ter_slot = st.empty()
        selected_rows = _render_strategy_bucket_table(custom_strategy)
        with weighted_ter_slot.container():
            _render_weighted_ter_card(selected_rows)

elif active_view == "Coverage":
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
