from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from etf_app.profile import ensure_instrument_cost_current_view, ensure_product_profile_schema
from etf_app.taxonomy import classify_instrument, ensure_taxonomy_schema, load_universe_rows, upsert_taxonomy


ALL_VENUES = ("XLON", "XETR")
NAME_EXCLUSION_TOKENS = ("ETP", "ETN", "NOTE")
ISSUER_DENYLIST = ("LEVERAGE SHARES",)
SMALL_CAP_VALUE_ISINS = {"IE0003R87OG3"}
GOLD_BUCKET_POLICY = "disclosed_non_ucits_physical_gold_exception"

THEMATIC_NAME_PATTERNS = (
    r"\bWATER\b",
    r"\bHYDROGEN\b",
    r"\bROBOTICS?\b",
    r"\bAI\b",
    r"\bARTIFICIAL INTELLIGENCE\b",
    r"\bAUTOMATION\b",
    r"\bCYBER\b",
    r"\bSECURITY\b",
    r"\bBLOCKCHAIN\b",
    r"\bMETAVERSE\b",
    r"\bCLOUD\b",
    r"\bDIGITAL\b",
    r"\bBATTERY\b",
    r"\bLITHIUM\b",
    r"\bURANIUM\b",
    r"\bCLEAN ENERGY\b",
    r"\bNEW ENERGY\b",
    r"\bSPACE\b",
    r"\bGENOM(?:IC|ICS)\b",
    r"\bSEMICONDUCTOR\b",
    r"\bFINTECH\b",
    r"\bINFRASTRUCTURE\b",
    r"\bPRIVATE EQUITY\b",
    r"\bREAL ESTATE\b",
    r"\bPROPERTY\b",
)
NON_CORE_GLOBAL_PATTERNS = (
    r"\bALPHA\b",
    r"\bENHANCED\b",
    r"\bRESEARCH\b",
    r"\bREI\b",
    r"\bESG\b",
    r"\bSRI\b",
    r"\bSCREENED\b",
    r"\bCLIMATE\b",
    r"\bPARIS\b",
    r"\bLOW CARBON\b",
    r"\bEXCLUSIONS?\b",
)
GOLD_EQUITY_PROXY_PATTERNS = (
    r"\bMINERS?\b",
    r"\bMINING\b",
    r"\bPRODUCERS?\b",
    r"\bBUGS\b",
    r"\bROYALT(?:Y|IES)\b",
)

STRATEGIES = (
    {
        "name": "All-Weather",
        "filename": "recommendations_all_weather.csv",
        "description": "Risk-balanced core mix for growth, deflation, inflation and liquidity shocks.",
        "buckets": (
            ("equity_global", 30.0),
            ("long_govt_bonds", 40.0),
            ("intermediate_govt_bonds", 15.0),
            ("gold", 7.5),
            ("cash", 7.5),
        ),
    },
    {
        "name": "Golden Butterfly",
        "filename": "recommendations_golden_butterfly.csv",
        "description": "Diversified mix with equity, small-cap value, long/short bonds, and gold.",
        "buckets": (
            ("equity_global", 20.0),
            ("equity_small_cap_value", 20.0),
            ("long_bonds", 20.0),
            ("short_bonds", 20.0),
            ("gold", 20.0),
        ),
    },
)


@dataclass(frozen=True)
class AttemptConfig:
    step: str
    venue_scope: str = "selected"
    allow_missing_fees: bool = False
    match_mode: str = "strict"


@dataclass(frozen=True)
class GoldPolicySummary:
    policy_name: str
    eligible_ucits_gold_count: int
    eligible_non_ucits_exception_gold_count: int
    ignored_gold_equity_proxy_count: int
    note: str


BUCKET_ATTEMPTS: dict[str, tuple[AttemptConfig, ...]] = {
    "equity_global": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_factor", venue_scope="all", match_mode="allow_factor"),
        AttemptConfig(
            "fallback_allow_missing_fees",
            venue_scope="all",
            allow_missing_fees=True,
            match_mode="allow_factor",
        ),
    ),
    "equity_small_cap_value": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_small_cap_proxy", venue_scope="all", match_mode="proxy"),
        AttemptConfig(
            "fallback_small_cap_proxy_allow_missing_fees",
            venue_scope="all",
            allow_missing_fees=True,
            match_mode="proxy",
        ),
    ),
    "long_govt_bonds": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_missing_fees", venue_scope="all", allow_missing_fees=True),
    ),
    "intermediate_govt_bonds": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_missing_fees", venue_scope="all", allow_missing_fees=True),
    ),
    "long_bonds": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_missing_fees", venue_scope="all", allow_missing_fees=True),
    ),
    "short_bonds": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_missing_fees", venue_scope="all", allow_missing_fees=True),
    ),
    "cash": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_short_bond_proxy", venue_scope="all", match_mode="bond_proxy"),
        AttemptConfig(
            "fallback_short_bond_proxy_allow_missing_fees",
            venue_scope="all",
            allow_missing_fees=True,
            match_mode="bond_proxy",
        ),
    ),
    "gold": (
        AttemptConfig("strict"),
        AttemptConfig("fallback_venue_expand", venue_scope="all"),
        AttemptConfig("fallback_allow_missing_fees", venue_scope="all", allow_missing_fees=True),
    ),
}


COMPILED_THEMATIC_PATTERNS = [re.compile(pattern, flags=re.IGNORECASE) for pattern in THEMATIC_NAME_PATTERNS]
COMPILED_NON_CORE_GLOBAL_PATTERNS = [re.compile(pattern, flags=re.IGNORECASE) for pattern in NON_CORE_GLOBAL_PATTERNS]
COMPILED_GOLD_EQUITY_PROXY_PATTERNS = [re.compile(pattern, flags=re.IGNORECASE) for pattern in GOLD_EQUITY_PROXY_PATTERNS]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETF strategy recommender")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue filter (ALL means XLON+XETR only)",
    )
    parser.add_argument(
        "--preferred-currency-order",
        default="USD,EUR,GBP",
        help="Currency sort order, comma separated (default: USD,EUR,GBP)",
    )
    parser.add_argument("--top-n", type=int, default=5, help="Top candidates per bucket")
    parser.add_argument(
        "--allow-missing-fees",
        action="store_true",
        help="Allow candidates without ongoing_charges after strict attempts",
    )
    parser.add_argument(
        "--allow-missing-currency",
        action="store_true",
        help="Allow candidates with missing trading currency (pushed to bottom)",
    )
    parser.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated recommendation CSVs",
    )
    return parser


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_currency_order(raw: str) -> list[str]:
    values = [value.strip().upper() for value in raw.split(",") if value.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for value in values + ["USD", "EUR", "GBP"]:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def venue_scope(arg: str) -> list[str]:
    if arg == "XLON":
        return ["XLON"]
    if arg == "XETR":
        return ["XETR"]
    return list(ALL_VENUES)


def normalize_text(value: object) -> str:
    return str(value or "").upper()


def normalize_name_for_match(name: str | None) -> str:
    if not name:
        return ""
    text = name.upper().replace("&", " AND ")
    for pattern, replacement in (
        (r"\bWLD\b", "WORLD"),
        (r"\bWRLD\b", "WORLD"),
        (r"\bGLB\b", "GLOBAL"),
        (r"\bALL-?WORLD\b", "ALL WORLD"),
        (r"S\+P", "S P"),
    ):
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"[-_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def has_name_exclusion(name: str) -> bool:
    upper = normalize_text(name)
    return any(token in upper for token in NAME_EXCLUSION_TOKENS)


def has_issuer_denylist(issuer_name: str) -> bool:
    upper = normalize_text(issuer_name)
    return any(token in upper for token in ISSUER_DENYLIST)


def has_pattern(name: str | None, patterns: list[re.Pattern[str]]) -> bool:
    normalized = normalize_name_for_match(name)
    return any(pattern.search(normalized) for pattern in patterns)


def has_thematic_name(name: str | None) -> bool:
    return has_pattern(name, COMPILED_THEMATIC_PATTERNS)


def has_non_core_global_modifier(name: str | None) -> bool:
    return has_pattern(name, COMPILED_NON_CORE_GLOBAL_PATTERNS)


def has_gold_equity_proxy_name(name: str | None) -> bool:
    return has_pattern(name, COMPILED_GOLD_EQUITY_PROXY_PATTERNS)


def has_gold_like_name(name: str | None) -> bool:
    return bool(re.search(r"\b(GOLD|BULLION)\b", normalize_name_for_match(name)))


def name_implies_leverage_inverse(name: str) -> bool:
    upper = normalize_text(name)
    patterns = (
        r"\b[2-9]X\b",
        r"\b-[1-9]X\b",
        r"\bLEVERAGED?\b",
        r"\bINVERSE\b",
        r"\bSHORT\b",
        r"\bULTRA\b",
        r"\bDAILY\b",
    )
    return any(re.search(pattern, upper) for pattern in patterns)


def filter_rows_by_venues(rows: list[dict[str, object]], venues: list[str]) -> list[dict[str, object]]:
    allowed = set(venues)
    return [dict(row) for row in rows if str(row.get("primary_venue") or "") in allowed]


def apply_hard_filters(
    base_rows: list[dict[str, object]],
    *,
    bucket_name: str,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> tuple[list[dict[str, object]], dict[str, int], int]:
    excluded = {
        "instrument_type": 0,
        "leverage_flag": 0,
        "inverse_flag": 0,
        "xs_isin": 0,
        "name_etp_etn_note": 0,
        "issuer_denylist": 0,
        "missing_fees": 0,
        "missing_currency": 0,
    }
    kept: list[dict[str, object]] = []
    considered = len(base_rows)
    is_gold_bucket = bucket_name == "gold"

    for row in base_rows:
        reasons: list[str] = []
        instrument_type = normalize_text(row["instrument_type"])
        name_text = str(row["instrument_name"] or "")

        if is_gold_bucket:
            instrument_type_ok = instrument_type in {"ETF", "ETC"}
        else:
            instrument_type_ok = instrument_type == "ETF"
        if not instrument_type_ok:
            excluded["instrument_type"] += 1
            reasons.append("instrument_type_not_allowed")
        if int(row["leverage_flag"] or 0) != 0:
            excluded["leverage_flag"] += 1
            reasons.append("leverage_flag")
        if int(row["inverse_flag"] or 0) != 0:
            excluded["inverse_flag"] += 1
            reasons.append("inverse_flag")
        if (not is_gold_bucket) and str(row["isin"] or "").upper().startswith("XS"):
            excluded["xs_isin"] += 1
            reasons.append("isin_xs")
        has_etp_etn_note = has_name_exclusion(name_text)
        if is_gold_bucket:
            if has_etp_etn_note and name_implies_leverage_inverse(name_text):
                excluded["name_etp_etn_note"] += 1
                reasons.append("name_etp_etn_note_leverage_implied")
        elif has_etp_etn_note:
            excluded["name_etp_etn_note"] += 1
            reasons.append("name_etp_etn_note")
        if has_issuer_denylist(str(row["issuer_normalized"] or "")):
            excluded["issuer_denylist"] += 1
            reasons.append("issuer_denylist")

        fee_missing_flag = 1 if row["ongoing_charges"] is None else 0
        currency_missing_flag = 1 if not row["currency"] else 0
        if fee_missing_flag and not allow_missing_fees:
            excluded["missing_fees"] += 1
            reasons.append("missing_fees")
        if currency_missing_flag and not allow_missing_currency:
            excluded["missing_currency"] += 1
            reasons.append("missing_currency")

        row["fee_missing_flag"] = fee_missing_flag
        row["currency_missing_flag"] = currency_missing_flag
        if reasons:
            row["excluded_reason"] = "|".join(reasons)
            continue

        row["excluded_reason"] = None
        kept.append(row)

    return kept, excluded, considered


def is_core_global_equity(row: dict[str, object], *, allow_factor: bool) -> tuple[bool, list[str]]:
    asset_class = str(row.get("asset_class") or "unknown").lower()
    geography_region = str(row.get("geography_region") or "unknown").lower()
    geography_scope = str(row.get("geography_scope") or "unknown").lower()
    if asset_class != "equity":
        return False, []
    if geography_region != "global" or geography_scope != "global":
        return False, []
    if row.get("sector") or row.get("theme"):
        return False, []
    if has_thematic_name(str(row.get("instrument_name") or "")):
        return False, []
    reasons = ["asset_class=equity", "geography_region=global", "core_scope=global"]
    if allow_factor:
        return True, reasons
    if row.get("factor") or row.get("equity_size") or row.get("equity_style"):
        return False, []
    if has_non_core_global_modifier(str(row.get("instrument_name") or "")):
        return False, []
    return True, reasons


def is_small_cap_value_candidate(row: dict[str, object], *, proxy_only: bool) -> tuple[bool, list[str]]:
    asset_class = str(row.get("asset_class") or "unknown").lower()
    geography_region = str(row.get("geography_region") or "unknown").lower()
    if asset_class != "equity" or geography_region != "global":
        return False, []
    if row.get("sector") or row.get("theme") or has_thematic_name(str(row.get("instrument_name") or "")):
        return False, []

    isin = str(row.get("isin") or "").upper()
    name = normalize_name_for_match(str(row.get("instrument_name") or ""))
    size = str(row.get("equity_size") or "").lower()
    style = str(row.get("equity_style") or "").lower()

    if not proxy_only:
        if isin in SMALL_CAP_VALUE_ISINS:
            return True, ["small_cap_value_seed"]
        if size == "small" and style == "value":
            return True, ["small_cap_value_taxonomy"]
        if "SMALL CAP VALUE" in name:
            return True, ["small_cap_value_name"]
        return False, []

    if size == "small" or "SMALL CAP" in name:
        return True, ["small_cap_proxy"]
    return False, []


def is_govt_bond(row: dict[str, object]) -> bool:
    return int(row.get("govt_bond_flag") or 0) == 1 or str(row.get("bond_type") or "").lower() == "govt"


def match_bucket(bucket_name: str, row: dict[str, object], *, match_mode: str = "strict") -> tuple[bool, list[str]]:
    asset_class = str(row.get("asset_class") or "unknown").lower()
    duration_bucket = str(row.get("duration_bucket") or "unknown").lower()
    commodity_type = str(row.get("commodity_type") or "unknown").lower()

    if bucket_name == "equity_global":
        return is_core_global_equity(row, allow_factor=match_mode == "allow_factor")

    if bucket_name == "equity_small_cap_value":
        return is_small_cap_value_candidate(row, proxy_only=match_mode == "proxy")

    if bucket_name == "long_govt_bonds":
        if asset_class == "bond" and is_govt_bond(row) and duration_bucket == "long":
            return True, ["asset_class=bond", "bond_type=govt", "duration=long"]
        return False, []

    if bucket_name == "intermediate_govt_bonds":
        if asset_class == "bond" and is_govt_bond(row) and duration_bucket == "intermediate":
            return True, ["asset_class=bond", "bond_type=govt", "duration=intermediate"]
        return False, []

    if bucket_name == "long_bonds":
        if asset_class == "bond" and duration_bucket == "long":
            return True, ["asset_class=bond", "duration=long"]
        return False, []

    if bucket_name == "short_bonds":
        if asset_class == "bond" and duration_bucket == "short":
            return True, ["asset_class=bond", "duration=short"]
        return False, []

    if bucket_name == "cash":
        if asset_class == "cash" or int(row.get("cash_flag") or 0) == 1 or int(row.get("cash_proxy_flag") or 0) == 1:
            return True, ["asset_class=cash_or_proxy"]
        if match_mode == "bond_proxy" and asset_class == "bond" and duration_bucket == "short" and is_govt_bond(row):
            return True, ["short_govt_bond_proxy"]
        return False, []

    if bucket_name == "gold":
        if asset_class == "commodity" and commodity_type == "gold" and not has_gold_equity_proxy_name(str(row.get("instrument_name") or "")):
            return True, ["asset_class=commodity", "commodity_type=gold"]
        return False, []

    return False, []


def venue_rank(venue: str | None) -> int:
    if venue == "XLON":
        return 0
    if venue == "XETR":
        return 1
    return 2


def currency_rank(currency: str | None, order: list[str]) -> int:
    if currency is None:
        return 99
    code = currency.upper()
    return order.index(code) if code in order else 50


def bucket_preference(bucket_name: str, row: dict[str, object]) -> int:
    if bucket_name == "equity_global":
        preference = 0
        if has_non_core_global_modifier(str(row.get("instrument_name") or "")):
            preference += 20
        if row.get("factor"):
            preference += 5
        if row.get("equity_size"):
            preference += 5
        if row.get("equity_style"):
            preference += 5
        return preference
    if bucket_name == "equity_small_cap_value":
        return 0 if str(row.get("equity_style") or "").lower() == "value" else 20
    if bucket_name == "cash":
        return 0 if str(row.get("asset_class") or "").lower() == "cash" else 10
    return 0


def rank_key(row: dict[str, object], currency_order: list[str], bucket_name: str) -> tuple[object, ...]:
    fee_missing = int(row["fee_missing_flag"])
    fee_value = float(row["ongoing_charges"]) if row["ongoing_charges"] is not None else 99.0
    return (
        bucket_preference(bucket_name, row),
        fee_missing,
        fee_value,
        venue_rank(str(row["primary_venue"] or "")),
        currency_rank(row["currency"], currency_order),
        str(row["isin"]),
    )


def to_output_row(
    strategy_name: str,
    bucket_name: str,
    target_weight: float,
    row: dict[str, object],
    bucket_reasons: list[str],
    selection_step: str,
    currency_order: list[str],
) -> dict[str, object]:
    selection_reason = {
        "filters": [
            "plain_vanilla",
            "taxonomy_backed",
            f"bucket={bucket_name}",
            f"selection_step={selection_step}",
        ],
        "bucket_reasons": bucket_reasons,
        "rank_keys": {
            "bucket_preference": bucket_preference(bucket_name, row),
            "fee": row["ongoing_charges"],
            "venue": row["primary_venue"],
            "currency": row["currency"],
            "venue_rank": venue_rank(str(row["primary_venue"] or "")),
            "currency_rank": currency_rank(row["currency"], currency_order),
            "isin": row["isin"],
        },
    }
    if bucket_name == "gold":
        selection_reason["bucket_policy"] = GOLD_BUCKET_POLICY
        if int(row.get("gold_policy_exception_flag") or 0) == 1:
            selection_reason["filters"].append("non_ucits_gold_exception_disclosed")
            selection_reason["bucket_policy_exception"] = "non_ucits_physical_gold"
    return {
        "strategy_name": strategy_name,
        "bucket_name": bucket_name,
        "target_weight": target_weight,
        "ISIN": row["isin"],
        "primary_venue": row["primary_venue"],
        "ticker": row["ticker"],
        "currency": row["currency"],
        "issuer_normalized": row["issuer_normalized"],
        "distribution_policy": row["distribution_policy"],
        "ongoing_charges": row["ongoing_charges"],
        "ongoing_charges_asof": row["ongoing_charges_asof"],
        "instrument_name": row["instrument_name"],
        "asset_class": row["asset_class"],
        "geography_scope": row["geography_scope"],
        "geography_region": row["geography_region"],
        "geography_country": row["geography_country"],
        "equity_size": row["equity_size"],
        "equity_style": row["equity_style"],
        "factor": row["factor"],
        "sector": row["sector"],
        "theme": row["theme"],
        "bond_type": row["bond_type"],
        "duration_bucket": row["duration_bucket"],
        "fee_missing_flag": int(row["fee_missing_flag"]),
        "currency_missing_flag": int(row["currency_missing_flag"]),
        "selection_reason": json.dumps(selection_reason, ensure_ascii=True),
    }


def effective_venues(selected_venues: list[str], venue_scope_name: str) -> list[str]:
    if venue_scope_name == "all":
        return list(ALL_VENUES)
    return list(selected_venues)


def summarize_gold_policy(
    *,
    eligible_ucits_gold_count: int,
    eligible_non_ucits_exception_gold_count: int,
    ignored_gold_equity_proxy_count: int,
) -> GoldPolicySummary:
    note = (
        "Gold policy allows a disclosed non-UCITS physical gold exception when "
        "no UCITS gold commodity instrument is available."
    )
    if eligible_ucits_gold_count > 0:
        note = (
            f"{note} Found {eligible_ucits_gold_count} eligible UCITS gold commodity "
            "instrument(s) in the selected universe."
        )
        if eligible_non_ucits_exception_gold_count > 0:
            note = (
                f"{note} {eligible_non_ucits_exception_gold_count} non-UCITS physical gold "
                "instrument(s) also qualify under the exception but are not required."
            )
    else:
        note = f"{note} No eligible UCITS gold commodity instrument was found in the selected universe."
        details: list[str] = []
        if eligible_non_ucits_exception_gold_count > 0:
            details.append(
                f"{eligible_non_ucits_exception_gold_count} non-UCITS physical gold instrument(s) are available under the disclosed exception"
            )
        if ignored_gold_equity_proxy_count > 0:
            details.append(
                f"{ignored_gold_equity_proxy_count} gold miner/producer equity proxy instrument(s) were ignored"
            )
        if not details:
            details.append("no disclosed exception candidate was found")
        if details:
            note = f"{note} {'; '.join(details)}."
    return GoldPolicySummary(
        policy_name=GOLD_BUCKET_POLICY,
        eligible_ucits_gold_count=eligible_ucits_gold_count,
        eligible_non_ucits_exception_gold_count=eligible_non_ucits_exception_gold_count,
        ignored_gold_equity_proxy_count=ignored_gold_equity_proxy_count,
        note=note,
    )


def load_non_mvp_gold_like_rows(conn: sqlite3.Connection, venues: list[str]) -> list[dict[str, object]]:
    placeholders = ",".join("?" for _ in venues)
    rows = conn.execute(
        f"""
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            i.instrument_type,
            COALESCE(i.leverage_flag, 0) AS leverage_flag,
            COALESCE(i.inverse_flag, 0) AS inverse_flag,
            COALESCE(iss.normalized_name, iss.issuer_name, '') AS issuer_normalized,
            l.venue_mic AS primary_venue,
            l.ticker,
            NULLIF(TRIM(l.trading_currency), '') AS currency,
            pp.distribution_policy,
            pp.benchmark_name,
            pp.asset_class_hint,
            pp.domicile_country,
            pp.replication_method,
            pp.hedged_flag,
            pp.hedged_target,
            c.ongoing_charges,
            c.asof_date AS ongoing_charges_asof
        FROM instrument i
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        LEFT JOIN issuer iss
          ON iss.issuer_id = i.issuer_id
        LEFT JOIN product_profile pp
          ON pp.instrument_id = i.instrument_id
        LEFT JOIN instrument_cost_current c
          ON c.instrument_id = i.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 0
          AND COALESCE(l.status, 'active') = 'active'
          AND l.venue_mic IN ({placeholders})
          AND (
              UPPER(i.instrument_name) LIKE '%GOLD%'
              OR UPPER(i.instrument_name) LIKE '%BULLION%'
          )
        ORDER BY i.isin
        """,
        tuple(venues),
    ).fetchall()
    candidates = [dict(row) for row in rows]
    for row in candidates:
        row["gold_policy_exception_flag"] = 0
    return candidates


def load_gold_exception_candidates(conn: sqlite3.Connection, venues: list[str]) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for row in load_non_mvp_gold_like_rows(conn, venues):
        instrument_name = str(row.get("instrument_name") or "")
        if int(row.get("leverage_flag") or 0) != 0 or int(row.get("inverse_flag") or 0) != 0:
            continue
        if name_implies_leverage_inverse(instrument_name):
            continue

        result = classify_instrument(
            isin=str(row.get("isin") or ""),
            instrument_name=instrument_name,
            instrument_type=str(row.get("instrument_type") or ""),
            distribution_policy=str(row.get("distribution_policy") or "") or None,
            benchmark_name=str(row.get("benchmark_name") or "") or None,
            asset_class_hint=str(row.get("asset_class_hint") or "") or None,
            replication_method=str(row.get("replication_method") or "") or None,
            hedged_flag=int(row["hedged_flag"]) if row.get("hedged_flag") in {0, 1} else None,
            hedged_target=str(row.get("hedged_target") or "") or None,
            domicile_country=str(row.get("domicile_country") or "") or None,
        )
        if result.asset_class != "commodity" or result.commodity_type != "gold":
            continue

        candidates.append(
            {
                "instrument_id": row["instrument_id"],
                "isin": row["isin"],
                "instrument_name": row["instrument_name"],
                "instrument_type": row["instrument_type"],
                "leverage_flag": row["leverage_flag"],
                "inverse_flag": row["inverse_flag"],
                "issuer_normalized": row["issuer_normalized"],
                "primary_venue": row["primary_venue"],
                "ticker": row["ticker"],
                "currency": row["currency"],
                "distribution_policy": row["distribution_policy"],
                "ongoing_charges": row["ongoing_charges"],
                "ongoing_charges_asof": row["ongoing_charges_asof"],
                "asset_class": result.asset_class,
                "geography_scope": result.geography_scope,
                "geography_region": result.geography_region,
                "geography_country": result.geography_country,
                "equity_size": result.equity_size,
                "equity_style": result.equity_style,
                "factor": result.factor,
                "sector": result.sector,
                "theme": result.theme,
                "bond_type": result.bond_type,
                "duration_bucket": result.duration_bucket,
                "duration_years_low": result.duration_years_low,
                "duration_years_high": result.duration_years_high,
                "commodity_type": result.commodity_type,
                "cash_proxy_flag": result.cash_proxy_flag,
                "gold_flag": result.gold_flag,
                "cash_flag": result.cash_flag,
                "govt_bond_flag": result.govt_bond_flag,
                "gold_policy_exception_flag": 1,
            }
        )
    return candidates


def inspect_gold_policy(
    conn: sqlite3.Connection,
    *,
    base_rows: list[dict[str, object]],
    selected_venues: list[str],
) -> GoldPolicySummary:
    scoped_rows = filter_rows_by_venues(base_rows, selected_venues)
    eligible_ucits_gold_count = sum(1 for row in scoped_rows if match_bucket("gold", row)[0])
    ignored_gold_equity_proxy_count = sum(
        1
        for row in scoped_rows
        if str(row.get("asset_class") or "").lower() == "equity"
        and has_gold_like_name(str(row.get("instrument_name") or ""))
        and has_gold_equity_proxy_name(str(row.get("instrument_name") or ""))
    )
    ignored_gold_equity_proxy_count += sum(
        1
        for row in load_non_mvp_gold_like_rows(conn, selected_venues)
        if has_gold_like_name(str(row.get("instrument_name") or ""))
        and has_gold_equity_proxy_name(str(row.get("instrument_name") or ""))
    )

    return summarize_gold_policy(
        eligible_ucits_gold_count=eligible_ucits_gold_count,
        eligible_non_ucits_exception_gold_count=len(load_gold_exception_candidates(conn, selected_venues)),
        ignored_gold_equity_proxy_count=ignored_gold_equity_proxy_count,
    )


def run_bucket_attempt(
    *,
    strategy_name: str,
    bucket_name: str,
    target_weight: float,
    base_rows: list[dict[str, object]],
    attempt: AttemptConfig,
    top_n: int,
    currency_order: list[str],
    selected_venues: list[str],
    allow_missing_currency: bool,
    gold_exception_rows: Optional[list[dict[str, object]]] = None,
) -> dict[str, object]:
    venues = effective_venues(selected_venues, attempt.venue_scope)
    scoped_rows = filter_rows_by_venues(base_rows, venues)
    if bucket_name == "gold" and gold_exception_rows:
        scoped_rows.extend(filter_rows_by_venues(gold_exception_rows, venues))
    filtered_rows, excluded_counts, considered_count = apply_hard_filters(
        scoped_rows,
        bucket_name=bucket_name,
        allow_missing_fees=attempt.allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
    )

    matched: list[tuple[tuple[object, ...], dict[str, object], list[str]]] = []
    for row in filtered_rows:
        ok, reasons = match_bucket(bucket_name, row, match_mode=attempt.match_mode)
        if not ok:
            continue
        matched.append((rank_key(row, currency_order, bucket_name), row, reasons))
    matched.sort(key=lambda item: item[0])

    selected_rows = [
        to_output_row(strategy_name, bucket_name, target_weight, row, reasons, attempt.step, currency_order)
        for _, row, reasons in matched[:top_n]
    ]
    pre_rank_top20 = [
        {
            "isin": row["isin"],
            "venue": row["primary_venue"],
            "ticker": row["ticker"],
            "fee": row["ongoing_charges"],
            "currency": row["currency"],
            "asset_class": row["asset_class"],
            "geography_region": row["geography_region"],
            "bond_type": row["bond_type"],
            "duration_bucket": row["duration_bucket"],
            "instrument_name": row["instrument_name"],
        }
        for _, row, _ in matched[:20]
    ]
    return {
        "step": attempt.step,
        "venues": venues,
        "allow_missing_fees": attempt.allow_missing_fees,
        "match_mode": attempt.match_mode,
        "considered": considered_count,
        "hard_kept": len(filtered_rows),
        "bucket_matches": len(matched),
        "final_selected": len(selected_rows),
        "selected_rows": selected_rows,
        "pre_rank_top20": pre_rank_top20,
        "excluded": excluded_counts,
    }


def pick_bucket_rows_with_fallbacks(
    *,
    strategy_name: str,
    bucket_name: str,
    target_weight: float,
    base_rows: list[dict[str, object]],
    top_n: int,
    currency_order: list[str],
    selected_venues: list[str],
    allow_missing_fees_flag: bool,
    allow_missing_currency: bool,
    gold_exception_rows: Optional[list[dict[str, object]]] = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    min_needed = min(top_n, 5)
    attempts: list[dict[str, object]] = []
    best_attempt: Optional[dict[str, object]] = None
    seen_signatures: set[tuple[tuple[str, ...], bool, str]] = set()

    for attempt in BUCKET_ATTEMPTS[bucket_name]:
        if attempt.allow_missing_fees and not allow_missing_fees_flag:
            continue
        venues = tuple(effective_venues(selected_venues, attempt.venue_scope))
        signature = (venues, attempt.allow_missing_fees, attempt.match_mode)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        result = run_bucket_attempt(
            strategy_name=strategy_name,
            bucket_name=bucket_name,
            target_weight=target_weight,
            base_rows=base_rows,
            attempt=attempt,
            top_n=top_n,
            currency_order=currency_order,
            selected_venues=selected_venues,
            allow_missing_currency=allow_missing_currency,
            gold_exception_rows=gold_exception_rows,
        )
        attempts.append(result)
        best_attempt = result
        if result["final_selected"] >= min_needed:
            break

    if best_attempt is None:
        raise RuntimeError(f"No attempts executed for bucket {bucket_name}")

    diagnostics = {
        "attempts": attempts,
        "final_step": best_attempt["step"],
        "final_selected": best_attempt["final_selected"],
        "final_hard_kept": best_attempt["hard_kept"],
        "final_bucket_matches": best_attempt["bucket_matches"],
        "pre_rank_top20": best_attempt["pre_rank_top20"],
    }
    return best_attempt["selected_rows"], diagnostics


def build_strategy_rows(
    strategy: dict[str, object],
    base_rows: list[dict[str, object]],
    *,
    selected_venues: list[str],
    top_n: int,
    currency_order: list[str],
    allow_missing_fees: bool,
    allow_missing_currency: bool,
    gold_policy: GoldPolicySummary | None = None,
    gold_exception_rows: Optional[list[dict[str, object]]] = None,
) -> tuple[list[dict[str, object]], dict[str, int], dict[str, dict[str, object]]]:
    out: list[dict[str, object]] = []
    emitted: dict[str, int] = {}
    diagnostics: dict[str, dict[str, object]] = {}
    for bucket_name, target_weight in strategy["buckets"]:
        selected, diag = pick_bucket_rows_with_fallbacks(
            strategy_name=strategy["name"],
            bucket_name=bucket_name,
            target_weight=float(target_weight),
            base_rows=base_rows,
            top_n=top_n,
            currency_order=currency_order,
            selected_venues=selected_venues,
            allow_missing_fees_flag=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
            gold_exception_rows=gold_exception_rows,
        )
        out.extend(selected)
        emitted[bucket_name] = len(selected)
        if bucket_name == "gold" and gold_policy is not None:
            diag = {
                **diag,
                "policy_name": gold_policy.policy_name,
                "policy_note": gold_policy.note,
                "eligible_ucits_gold_count": gold_policy.eligible_ucits_gold_count,
                "eligible_non_ucits_exception_gold_count": gold_policy.eligible_non_ucits_exception_gold_count,
                "ignored_gold_equity_proxy_count": gold_policy.ignored_gold_equity_proxy_count,
            }
        diagnostics[bucket_name] = diag
    return out, emitted, diagnostics


def export_strategy_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "strategy_name",
        "bucket_name",
        "target_weight",
        "ISIN",
        "primary_venue",
        "ticker",
        "currency",
        "issuer_normalized",
        "distribution_policy",
        "ongoing_charges",
        "ongoing_charges_asof",
        "instrument_name",
        "asset_class",
        "geography_scope",
        "geography_region",
        "geography_country",
        "equity_size",
        "equity_style",
        "factor",
        "sector",
        "theme",
        "bond_type",
        "duration_bucket",
        "fee_missing_flag",
        "currency_missing_flag",
        "selection_reason",
    ]
    try:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_latest{path.suffix}")
        with fallback.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"warning: {path.name} is locked; wrote {fallback.name} instead")


def print_bucket_summary(
    strategy_name: str,
    emitted: dict[str, int],
    diagnostics: dict[str, dict[str, object]],
    top_n: int,
) -> None:
    print(f"\n{strategy_name}:")
    for bucket_name in emitted:
        diag = diagnostics[bucket_name]
        print(f"  {bucket_name}:")
        for attempt in diag["attempts"]:
            venues = ",".join(attempt["venues"])
            print(
                f"    step={attempt['step']} venues={venues} "
                f"allow_missing_fees={attempt['allow_missing_fees']} "
                f"match_mode={attempt['match_mode']}"
            )
            print(f"      after_hard_filters={attempt['hard_kept']}")
            print(f"      after_bucket_filter={attempt['bucket_matches']}")
            print(f"      final_ranking_count={attempt['final_selected']}")
        print(f"    final_step={diag['final_step']}")
        print(f"    final_candidates_emitted={emitted[bucket_name]}")
        if bucket_name == "gold" and "policy_name" in diag:
            print(f"    policy={diag['policy_name']}")
            print(f"    eligible_ucits_gold={diag['eligible_ucits_gold_count']}")
            print(
                "    eligible_non_ucits_gold_exception="
                f"{diag['eligible_non_ucits_exception_gold_count']}"
            )
            print(f"    ignored_gold_equity_proxies={diag['ignored_gold_equity_proxy_count']}")
            print(f"    policy_note={diag['policy_note']}")
        if int(diag["final_selected"]) < min(top_n, 5):
            print("    debug_top20_pre_ranking:")
            for item in diag["pre_rank_top20"]:
                fee = "NULL" if item["fee"] is None else f"{float(item['fee']):.4f}"
                print(
                    f"      {item['isin']} | {item['venue']} | fee={fee} | "
                    f"{item['asset_class']} | {item['geography_region']} | "
                    f"{item['bond_type']} | {item['duration_bucket']} | {item['instrument_name']}"
                )


def ensure_recommendation_inputs(conn: sqlite3.Connection) -> int:
    ensure_product_profile_schema(conn)
    ensure_instrument_cost_current_view(conn)
    ensure_taxonomy_schema(conn)
    return upsert_taxonomy(conn, load_universe_rows(conn))


def load_base_candidates(conn: sqlite3.Connection) -> list[dict[str, object]]:
    placeholders = ",".join("?" for _ in ALL_VENUES)
    rows = conn.execute(
        f"""
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            i.instrument_type,
            COALESCE(i.leverage_flag, 0) AS leverage_flag,
            COALESCE(i.inverse_flag, 0) AS inverse_flag,
            COALESCE(iss.normalized_name, iss.issuer_name, '') AS issuer_normalized,
            l.venue_mic AS primary_venue,
            l.ticker,
            NULLIF(TRIM(l.trading_currency), '') AS currency,
            pp.distribution_policy,
            c.ongoing_charges,
            c.asof_date AS ongoing_charges_asof,
            COALESCE(t.asset_class, 'unknown') AS asset_class,
            COALESCE(t.geography_scope, 'unknown') AS geography_scope,
            COALESCE(t.geography_region, 'unknown') AS geography_region,
            t.geography_country,
            t.equity_size,
            t.equity_style,
            t.factor,
            t.sector,
            t.theme,
            COALESCE(t.bond_type, 'unknown') AS bond_type,
            COALESCE(t.duration_bucket, 'unknown') AS duration_bucket,
            t.duration_years_low,
            t.duration_years_high,
            COALESCE(t.commodity_type, 'unknown') AS commodity_type,
            COALESCE(t.cash_proxy_flag, 0) AS cash_proxy_flag,
            COALESCE(t.gold_flag, 0) AS gold_flag,
            COALESCE(t.cash_flag, 0) AS cash_flag,
            COALESCE(t.govt_bond_flag, 0) AS govt_bond_flag
        FROM instrument i
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
        LEFT JOIN product_profile pp ON pp.instrument_id = i.instrument_id
        LEFT JOIN instrument_cost_current c ON c.instrument_id = i.instrument_id
        LEFT JOIN instrument_taxonomy t ON t.instrument_id = i.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
          AND COALESCE(l.status, 'active') = 'active'
          AND l.venue_mic IN ({placeholders})
        ORDER BY i.isin
        """,
        tuple(ALL_VENUES),
    ).fetchall()
    return [dict(row) for row in rows]


def run_recommendations(
    *,
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
    artifacts_dir: str,
) -> int:
    db = Path(db_path)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    currency_order = parse_currency_order(preferred_currency_order)
    venues = venue_scope(venue)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        classified = ensure_recommendation_inputs(conn)
        base_rows = load_base_candidates(conn)
        gold_exception_rows = load_gold_exception_candidates(conn, venues)
        gold_policy = inspect_gold_policy(conn, base_rows=base_rows, selected_venues=venues)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if not base_rows:
        raise SystemExit("No base candidates loaded from XLON/XETR.")

    selected_scope_rows = filter_rows_by_venues(base_rows, venues)
    strict_candidates, strict_excluded_counts, considered_count = apply_hard_filters(
        selected_scope_rows,
        bucket_name="equity_global",
        allow_missing_fees=False,
        allow_missing_currency=allow_missing_currency,
    )

    print(f"taxonomy rows refreshed: {classified}")
    print(f"base candidates loaded: {considered_count} (venues={','.join(venues)})")
    print(f"kept after hard filters (strict fees): {len(strict_candidates)}")
    print(f"excluded_missing_fees={strict_excluded_counts['missing_fees']}")
    print(f"excluded_missing_currency={strict_excluded_counts['missing_currency']}")
    print(f"gold_policy={gold_policy.policy_name}")
    print(f"gold_policy_note={gold_policy.note}")

    artifacts_root = Path(artifacts_dir)
    outputs: list[tuple[str, Path, dict[str, int], dict[str, dict[str, object]]]] = []
    for strategy in STRATEGIES:
        rows, emitted, diagnostics = build_strategy_rows(
            strategy,
            base_rows,
            selected_venues=venues,
            top_n=top_n,
            currency_order=currency_order,
            allow_missing_fees=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
            gold_policy=gold_policy,
            gold_exception_rows=gold_exception_rows,
        )
        output_path = artifacts_root / strategy["filename"]
        export_strategy_csv(output_path, rows)
        outputs.append((strategy["name"], output_path, emitted, diagnostics))

    print("\n=== Bucket Summary ===")
    for strategy_name, _path, emitted, diagnostics in outputs:
        print_bucket_summary(strategy_name, emitted, diagnostics, top_n)

    print("\noutputs:")
    for strategy_name, output_path, emitted, _ in outputs:
        min_candidates = min(emitted.values()) if emitted else 0
        print(f"  {strategy_name}: {output_path}")
        print(f"    min candidates per bucket: {min_candidates}")
    print(f"generated_at: {now_utc_iso()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_recommendations(
        db_path=args.db_path,
        venue=args.venue,
        preferred_currency_order=args.preferred_currency_order,
        top_n=args.top_n,
        allow_missing_fees=args.allow_missing_fees,
        allow_missing_currency=args.allow_missing_currency,
        artifacts_dir=args.artifacts_dir,
    )
