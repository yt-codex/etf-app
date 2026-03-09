from __future__ import annotations

import datetime as dt
import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional


UCITS_RAW_PATTERNS = (
    re.compile(r"\bUCITS\b", flags=re.IGNORECASE),
    re.compile(r"\bU\.?ETF\b", flags=re.IGNORECASE),
    re.compile(r"\bUC\.?ETF\b", flags=re.IGNORECASE),
    re.compile(r"\bU\.?E\.(?:ACC|DIS|DIST|D|A|EOA|EOD)\b", flags=re.IGNORECASE),
    re.compile(r"\bUE\s+(?:ACC|DIS|DIST|D|A|EOA|EOD)\b", flags=re.IGNORECASE),
    re.compile(r"\bUE(?:ACC|DIS|DIST|EOA|EOD|DLA)\b", flags=re.IGNORECASE),
    re.compile(r"\bUC\.E\.(?:ACC|DIS|DIST|D|A|EOA|EOD)\b", flags=re.IGNORECASE),
)
UCITS_NORMALIZED_PHRASES = (
    "UCITS",
    "U ETF",
    "UC ETF",
    "UE ACC",
    "UE DIS",
    "UE DIST",
    "UE D",
    "UE A",
    "UE EOA",
    "UE EOD",
    "UE DLA",
)


@dataclass
class ProfileSyncStats:
    product_profile_rows_upserted: int = 0
    ucits_from_name: int = 0
    ucits_from_snapshot: int = 0
    instruments_ucits_updated: int = 0
    distributions_synced: int = 0
    costs_synced: int = 0
    metadata_synced: int = 0


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    ddl_type: str,
    default_sql: Optional[str] = None,
) -> bool:
    if column_name in table_columns(conn, table_name):
        return False
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}{default_clause}")
    return True


def ensure_product_profile_schema(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "instrument", "ucits_source", "TEXT")
    ensure_column(conn, "instrument", "ucits_updated_at", "TEXT")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_profile(
            instrument_id INTEGER PRIMARY KEY,
            distribution_policy TEXT NULL,
            ucits_flag INTEGER NULL CHECK (ucits_flag IN (0, 1)),
            ucits_source TEXT NULL,
            ucits_updated_at TEXT NULL,
            ongoing_charges REAL NULL,
            ongoing_charges_asof TEXT NULL,
            benchmark_name TEXT NULL,
            asset_class_hint TEXT NULL,
            domicile_country TEXT NULL,
            fund_size_value REAL NULL,
            fund_size_currency TEXT NULL,
            fund_size_asof TEXT NULL,
            fund_size_scope TEXT NULL,
            replication_method TEXT NULL,
            hedged_flag INTEGER NULL CHECK (hedged_flag IN (0, 1)),
            hedged_target TEXT NULL,
            equity_size_hint TEXT NULL,
            equity_style_hint TEXT NULL,
            sector_hint TEXT NULL,
            sector_weight REAL NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
        )
        """
    )
    ensure_column(conn, "product_profile", "distribution_policy", "TEXT")
    ensure_column(conn, "product_profile", "ucits_flag", "INTEGER")
    ensure_column(conn, "product_profile", "ucits_source", "TEXT")
    ensure_column(conn, "product_profile", "ucits_updated_at", "TEXT")
    ensure_column(conn, "product_profile", "ongoing_charges", "REAL")
    ensure_column(conn, "product_profile", "ongoing_charges_asof", "TEXT")
    ensure_column(conn, "product_profile", "benchmark_name", "TEXT")
    ensure_column(conn, "product_profile", "asset_class_hint", "TEXT")
    ensure_column(conn, "product_profile", "domicile_country", "TEXT")
    ensure_column(conn, "product_profile", "fund_size_value", "REAL")
    ensure_column(conn, "product_profile", "fund_size_currency", "TEXT")
    ensure_column(conn, "product_profile", "fund_size_asof", "TEXT")
    ensure_column(conn, "product_profile", "fund_size_scope", "TEXT")
    ensure_column(conn, "product_profile", "replication_method", "TEXT")
    ensure_column(conn, "product_profile", "hedged_flag", "INTEGER")
    ensure_column(conn, "product_profile", "hedged_target", "TEXT")
    ensure_column(conn, "product_profile", "equity_size_hint", "TEXT")
    ensure_column(conn, "product_profile", "equity_style_hint", "TEXT")
    ensure_column(conn, "product_profile", "sector_hint", "TEXT")
    ensure_column(conn, "product_profile", "sector_weight", "REAL")
    ensure_column(conn, "product_profile", "updated_at", "TEXT", f"'{now_utc_iso()}'")


def ensure_instrument_cost_current_view(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cost_snapshot(
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
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cost_snapshot_instrument_asof ON cost_snapshot(instrument_id, asof_date)"
    )
    conn.execute("DROP VIEW IF EXISTS instrument_cost_current")
    conn.execute(
        """
        CREATE VIEW instrument_cost_current AS
        WITH ranked AS (
            SELECT
                instrument_id,
                asof_date,
                ongoing_charges,
                doc_id,
                quality_flag,
                cost_id,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument_id
                    ORDER BY asof_date DESC, cost_id DESC
                ) AS rn
            FROM cost_snapshot
            WHERE ongoing_charges IS NOT NULL
              AND quality_flag IN (
                  'ok',
                  'partial',
                  'issuer_page_ok',
                  'amundi_factsheet_ok',
                  'avantis_kid_ok',
                  'wisdomtree_kid_ok',
                  'invesco_kid_ok',
                  'lse_ter_ok'
              )
        )
        SELECT
            instrument_id,
            asof_date,
            ongoing_charges,
            doc_id,
            quality_flag
        FROM ranked
        WHERE rn = 1
        """
    )


def detect_ucits_flag(instrument_name: Optional[str]) -> Optional[int]:
    if not instrument_name:
        return None
    if any(pattern.search(instrument_name) for pattern in UCITS_RAW_PATTERNS):
        return 1

    normalized = re.sub(r"[^A-Z0-9]+", " ", instrument_name.upper())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if any(phrase in normalized for phrase in UCITS_NORMALIZED_PHRASES):
        return 1
    return None


def _load_latest_distribution(conn: sqlite3.Connection) -> dict[int, str]:
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    instrument_id,
                    use_of_income,
                    asof_date,
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_id
                        ORDER BY asof_date DESC, id DESC
                    ) AS rn
                FROM issuer_metadata_snapshot
                WHERE use_of_income IS NOT NULL
                  AND TRIM(use_of_income) <> ''
            )
            SELECT instrument_id, use_of_income
            FROM ranked
            WHERE rn = 1
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {int(row["instrument_id"]): str(row["use_of_income"]) for row in rows}


def _load_latest_ucits_snapshot(conn: sqlite3.Connection) -> dict[int, tuple[int, str]]:
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    instrument_id,
                    ucits_compliant,
                    asof_date,
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_id
                        ORDER BY asof_date DESC, id DESC
                    ) AS rn
                FROM issuer_metadata_snapshot
                WHERE ucits_compliant IS NOT NULL
            )
            SELECT instrument_id, ucits_compliant
            FROM ranked
            WHERE rn = 1
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        int(row["instrument_id"]): (int(row["ucits_compliant"]), "issuer_metadata_snapshot")
        for row in rows
    }


def _load_current_costs(conn: sqlite3.Connection) -> dict[int, tuple[float, str]]:
    try:
        rows = conn.execute(
            """
            SELECT instrument_id, ongoing_charges, asof_date
            FROM instrument_cost_current
            WHERE ongoing_charges IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        int(row["instrument_id"]): (float(row["ongoing_charges"]), str(row["asof_date"]))
        for row in rows
    }


def _normalize_text_value(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_flag(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        if value in {0, 1}:
            return int(value)
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    return None


def _coerce_optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(".") > text.rfind(","):
            text = text.replace(",", "")
        else:
            text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        parts = text.split(",")
        if len(parts) > 1 and all(part.isdigit() and len(part) == 3 for part in parts[1:]):
            text = "".join(parts)
        else:
            text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_date_value(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    normalized = text.replace("as of", "").replace("As of", "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%b/%Y", "%d/%B/%Y", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            return dt.datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    return normalized


def _first_present(payload: dict[str, object], *keys: str) -> object:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _normalize_replication_method(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    upper = text.upper()
    if "PHYS" in upper:
        return "physical"
    if "SYNTH" in upper or "SWAP" in upper:
        return "synthetic"
    return text


def _normalize_domicile_country(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    aliases = {
        "FRANCE": "France",
        "FRENCH": "France",
        "GERMAN": "Germany",
        "GERMANY": "Germany",
        "IRELAND": "Ireland",
        "IRISH": "Ireland",
        "LUXEMBOURG": "Luxembourg",
        "LUXEMBOURGISH": "Luxembourg",
        "UNITED KINGDOM": "United Kingdom",
        "UK": "United Kingdom",
    }
    return aliases.get(text.upper(), text)


def _infer_domicile_country_from_isin(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text or len(text) < 2:
        return None
    prefix = text[:2].upper()
    prefix_map = {
        "AT": "Austria",
        "BE": "Belgium",
        "CH": "Switzerland",
        "DE": "Germany",
        "ES": "Spain",
        "FR": "France",
        "GB": "United Kingdom",
        "IE": "Ireland",
        "IT": "Italy",
        "LU": "Luxembourg",
        "NL": "Netherlands",
    }
    return prefix_map.get(prefix)


def _normalize_fund_size_scope(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    upper = text.upper()
    if "SHARE" in upper:
        return "share_class"
    if "FUND" in upper or "AUM" in upper or "ASSET" in upper:
        return "fund"
    return text.lower()


def _normalize_equity_size_hint(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    upper = text.upper()
    if "ALL CAP" in upper or "ALL_CAP" in upper:
        return "all_cap"
    if "LARGE" in upper or "GIANT" in upper:
        return "large"
    if "MID" in upper or "MEDIUM" in upper:
        return "mid"
    if "SMALL" in upper or "MICRO" in upper:
        return "small"
    return text.lower()


def _normalize_equity_style_hint(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    upper = text.upper()
    if "VALUE" in upper:
        return "value"
    if "GROWTH" in upper:
        return "growth"
    if "BLEND" in upper or "CORE" in upper:
        return "blend"
    return text.lower()


def _normalize_sector_hint(value: object) -> Optional[str]:
    text = _normalize_text_value(value)
    if not text:
        return None
    mapping = {
        "TECHNOLOGY": "technology",
        "HEALTHCARE": "health_care",
        "HEALTH CARE": "health_care",
        "FINANCIAL SERVICES": "financials",
        "FINANCIALS": "financials",
        "ENERGY": "energy",
        "UTILITIES": "utilities",
        "INDUSTRIALS": "industrials",
        "REAL ESTATE": "real_estate",
        "BASIC MATERIALS": "materials",
        "MATERIALS": "materials",
        "COMMUNICATION SERVICES": "communication",
        "COMMUNICATION": "communication",
        "CONSUMER CYCLICAL": "consumer_cyclical",
        "CONSUMER DEFENSIVE": "consumer_defensive",
    }
    return mapping.get(text.upper(), text.lower().replace(" ", "_"))


def _extract_profile_metadata_from_payload(payload: dict[str, object]) -> dict[str, object]:
    extracted: dict[str, object] = {}
    for container_key in ("parsed", "parse", "parse_ongoing", "profile_metadata", "metadata"):
        nested = payload.get(container_key)
        if isinstance(nested, dict):
            payload = {**payload, **nested}

    benchmark_name = _normalize_text_value(
        _first_present(payload, "benchmark_name", "benchmark", "index_name", "benchmark_index_name")
    )
    asset_class_hint = _normalize_text_value(
        _first_present(payload, "asset_class_hint", "asset_class", "assetClass", "fund_asset_class")
    )
    domicile_country = _normalize_domicile_country(
        _first_present(payload, "domicile_country", "domicile", "fund_domicile", "domicileCountry")
    )
    fund_size_value = _coerce_optional_float(
        _first_present(
            payload,
            "fund_size_value",
            "aum_value",
            "assets_under_management_value",
            "net_assets_of_fund_value",
            "net_assets_value",
        )
    )
    fund_size_currency = _normalize_text_value(
        _first_present(
            payload,
            "fund_size_currency",
            "aum_currency",
            "assets_under_management_currency",
            "net_assets_of_fund_currency",
            "net_assets_currency",
        )
    )
    fund_size_asof = _normalize_date_value(
        _first_present(
            payload,
            "fund_size_asof",
            "aum_asof",
            "assets_under_management_asof",
            "net_assets_of_fund_asof",
            "net_assets_asof",
        )
    )
    fund_size_scope = _normalize_fund_size_scope(
        _first_present(
            payload,
            "fund_size_scope",
            "aum_scope",
            "assets_under_management_scope",
            "net_assets_of_fund_scope",
            "net_assets_scope",
        )
    )
    if fund_size_value is not None and fund_size_scope is None:
        fund_size_scope = "fund"
    replication_method = _normalize_replication_method(
        _first_present(payload, "replication_method", "replication", "replication_type", "replicationType")
    )
    hedged_flag = _coerce_optional_flag(_first_present(payload, "hedged_flag", "hedged", "currency_hedged"))
    hedged_target = _normalize_text_value(
        _first_present(payload, "hedged_target", "hedged_currency", "currency_hedged_to")
    )
    if hedged_flag is None and hedged_target is not None:
        hedged_flag = 1
    equity_size_hint = _normalize_equity_size_hint(
        _first_present(payload, "equity_size_hint", "equity_size", "market_cap", "market_cap_category")
    )
    equity_style_hint = _normalize_equity_style_hint(
        _first_present(payload, "equity_style_hint", "equity_style", "investment_style", "investment_style_hint")
    )
    sector_hint = _normalize_sector_hint(
        _first_present(payload, "sector_hint", "top_sector", "dominant_sector", "sector")
    )
    sector_weight = _coerce_optional_float(
        _first_present(payload, "sector_weight", "top_sector_weight", "dominant_sector_weight")
    )

    extracted["benchmark_name"] = benchmark_name
    extracted["asset_class_hint"] = asset_class_hint
    extracted["domicile_country"] = domicile_country
    extracted["fund_size_value"] = fund_size_value
    extracted["fund_size_currency"] = fund_size_currency
    extracted["fund_size_asof"] = fund_size_asof
    extracted["fund_size_scope"] = fund_size_scope
    extracted["replication_method"] = replication_method
    extracted["hedged_flag"] = hedged_flag
    extracted["hedged_target"] = hedged_target
    extracted["equity_size_hint"] = equity_size_hint
    extracted["equity_style_hint"] = equity_style_hint
    extracted["sector_hint"] = sector_hint
    extracted["sector_weight"] = sector_weight
    return extracted


def _merge_profile_metadata_rows(
    rows: list[sqlite3.Row],
    per_instrument: dict[int, dict[str, object]],
) -> None:
    for row in rows:
        instrument_id = int(row["instrument_id"])
        try:
            payload = json.loads(str(row["raw_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue

        extracted = _extract_profile_metadata_from_payload(payload)
        current = per_instrument.setdefault(
            instrument_id,
            {
                "benchmark_name": None,
                "asset_class_hint": None,
                "domicile_country": None,
                "fund_size_value": None,
                "fund_size_currency": None,
                "fund_size_asof": None,
                "fund_size_scope": None,
                "replication_method": None,
                "hedged_flag": None,
                "hedged_target": None,
                "equity_size_hint": None,
                "equity_style_hint": None,
                "sector_hint": None,
                "sector_weight": None,
            },
        )
        for key, value in extracted.items():
            if value is None:
                continue
            if current.get(key) is None:
                current[key] = value


def _load_latest_profile_metadata(conn: sqlite3.Connection) -> dict[int, dict[str, object]]:
    per_instrument: dict[int, dict[str, object]] = {}
    try:
        issuer_rows = conn.execute(
            """
            SELECT instrument_id, raw_json
            FROM issuer_metadata_snapshot
            WHERE raw_json IS NOT NULL
              AND TRIM(raw_json) <> ''
            ORDER BY asof_date DESC, id DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        issuer_rows = []
    _merge_profile_metadata_rows(issuer_rows, per_instrument)

    try:
        cost_rows = conn.execute(
            """
            SELECT instrument_id, raw_json
            FROM cost_snapshot
            WHERE raw_json IS NOT NULL
              AND TRIM(raw_json) <> ''
            ORDER BY asof_date DESC, cost_id DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        cost_rows = []
    _merge_profile_metadata_rows(cost_rows, per_instrument)
    return per_instrument


def refresh_product_profile(conn: sqlite3.Connection) -> ProfileSyncStats:
    ensure_product_profile_schema(conn)
    ensure_instrument_cost_current_view(conn)

    ts = now_utc_iso()
    stats = ProfileSyncStats()
    latest_distribution = _load_latest_distribution(conn)
    latest_ucits = _load_latest_ucits_snapshot(conn)
    current_costs = _load_current_costs(conn)
    latest_metadata = _load_latest_profile_metadata(conn)

    instruments = conn.execute(
        """
        SELECT instrument_id, isin, instrument_name, ucits_flag, ucits_source
        FROM instrument
        ORDER BY instrument_id
        """
    ).fetchall()

    instrument_updates: list[tuple[Optional[int], Optional[str], Optional[str], int]] = []
    profile_upserts: list[
        tuple[
            int,
            Optional[str],
            Optional[int],
            Optional[str],
            Optional[str],
            Optional[float],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[float],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[int],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[float],
            str,
        ]
    ] = []

    for row in instruments:
        instrument_id = int(row["instrument_id"])
        instrument_name = row["instrument_name"]
        snapshot_ucits = latest_ucits.get(instrument_id)
        name_ucits = detect_ucits_flag(instrument_name)

        next_ucits = row["ucits_flag"]
        next_source = row["ucits_source"]
        next_ucits_updated_at: Optional[str] = None

        if snapshot_ucits is not None:
            next_ucits = snapshot_ucits[0]
            next_source = snapshot_ucits[1]
            next_ucits_updated_at = ts
            stats.ucits_from_snapshot += 1
        elif name_ucits == 1:
            next_ucits = 1
            next_source = "instrument_name"
            next_ucits_updated_at = ts
            stats.ucits_from_name += 1
        elif next_ucits == 1 and not next_source:
            next_source = "legacy_seed"
            next_ucits_updated_at = ts

        if next_ucits != row["ucits_flag"] or next_source != row["ucits_source"]:
            instrument_updates.append((next_ucits, next_source, next_ucits_updated_at, instrument_id))

        distribution_policy = latest_distribution.get(instrument_id)
        current_cost = current_costs.get(instrument_id)
        ongoing_charges = current_cost[0] if current_cost else None
        ongoing_charges_asof = current_cost[1] if current_cost else None
        metadata = latest_metadata.get(instrument_id, {})
        benchmark_name = _normalize_text_value(metadata.get("benchmark_name"))
        asset_class_hint = _normalize_text_value(metadata.get("asset_class_hint"))
        domicile_country = _normalize_domicile_country(metadata.get("domicile_country"))
        if domicile_country is None:
            domicile_country = _infer_domicile_country_from_isin(row["isin"])
        fund_size_value = _coerce_optional_float(metadata.get("fund_size_value"))
        fund_size_currency = _normalize_text_value(metadata.get("fund_size_currency"))
        fund_size_asof = _normalize_date_value(metadata.get("fund_size_asof"))
        fund_size_scope = _normalize_fund_size_scope(metadata.get("fund_size_scope"))
        replication_method = _normalize_text_value(metadata.get("replication_method"))
        hedged_flag = _coerce_optional_flag(metadata.get("hedged_flag"))
        hedged_target = _normalize_text_value(metadata.get("hedged_target"))
        equity_size_hint = _normalize_equity_size_hint(metadata.get("equity_size_hint"))
        equity_style_hint = _normalize_equity_style_hint(metadata.get("equity_style_hint"))
        sector_hint = _normalize_sector_hint(metadata.get("sector_hint"))
        sector_weight = _coerce_optional_float(metadata.get("sector_weight"))

        if distribution_policy is not None:
            stats.distributions_synced += 1
        if ongoing_charges is not None:
            stats.costs_synced += 1
        if any(
            value is not None
            for value in (
                benchmark_name,
                asset_class_hint,
                domicile_country,
                fund_size_value,
                fund_size_currency,
                fund_size_asof,
                fund_size_scope,
                replication_method,
                hedged_flag,
                hedged_target,
                equity_size_hint,
                equity_style_hint,
                sector_hint,
                sector_weight,
            )
        ):
            stats.metadata_synced += 1

        if any(
            value is not None
            for value in (
                distribution_policy,
                next_ucits,
                next_source,
                ongoing_charges,
                ongoing_charges_asof,
                benchmark_name,
                asset_class_hint,
                domicile_country,
                replication_method,
                hedged_flag,
                hedged_target,
                equity_size_hint,
                equity_style_hint,
                sector_hint,
                sector_weight,
            )
        ):
            profile_upserts.append(
                (
                    instrument_id,
                    distribution_policy,
                    next_ucits,
                    next_source,
                    next_ucits_updated_at,
                    ongoing_charges,
                    ongoing_charges_asof,
                    benchmark_name,
                    asset_class_hint,
                    domicile_country,
                    fund_size_value,
                    fund_size_currency,
                    fund_size_asof,
                    fund_size_scope,
                    replication_method,
                    hedged_flag,
                    hedged_target,
                    equity_size_hint,
                    equity_style_hint,
                    sector_hint,
                    sector_weight,
                    ts,
                )
            )

    if instrument_updates:
        conn.executemany(
            """
            UPDATE instrument
            SET ucits_flag = ?, ucits_source = ?, ucits_updated_at = COALESCE(?, ucits_updated_at)
            WHERE instrument_id = ?
            """,
            instrument_updates,
        )
        stats.instruments_ucits_updated = len(instrument_updates)

    if profile_upserts:
        conn.executemany(
            """
            INSERT INTO product_profile(
                instrument_id,
                distribution_policy,
                ucits_flag,
                ucits_source,
                ucits_updated_at,
                ongoing_charges,
                ongoing_charges_asof,
                benchmark_name,
                asset_class_hint,
                domicile_country,
                fund_size_value,
                fund_size_currency,
                fund_size_asof,
                fund_size_scope,
                replication_method,
                hedged_flag,
                hedged_target,
                equity_size_hint,
                equity_style_hint,
                sector_hint,
                sector_weight,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                distribution_policy = COALESCE(excluded.distribution_policy, product_profile.distribution_policy),
                ucits_flag = COALESCE(excluded.ucits_flag, product_profile.ucits_flag),
                ucits_source = COALESCE(excluded.ucits_source, product_profile.ucits_source),
                ucits_updated_at = COALESCE(excluded.ucits_updated_at, product_profile.ucits_updated_at),
                ongoing_charges = COALESCE(excluded.ongoing_charges, product_profile.ongoing_charges),
                ongoing_charges_asof = COALESCE(excluded.ongoing_charges_asof, product_profile.ongoing_charges_asof),
                benchmark_name = COALESCE(excluded.benchmark_name, product_profile.benchmark_name),
                asset_class_hint = COALESCE(excluded.asset_class_hint, product_profile.asset_class_hint),
                domicile_country = COALESCE(excluded.domicile_country, product_profile.domicile_country),
                fund_size_value = COALESCE(excluded.fund_size_value, product_profile.fund_size_value),
                fund_size_currency = COALESCE(excluded.fund_size_currency, product_profile.fund_size_currency),
                fund_size_asof = COALESCE(excluded.fund_size_asof, product_profile.fund_size_asof),
                fund_size_scope = COALESCE(excluded.fund_size_scope, product_profile.fund_size_scope),
                replication_method = COALESCE(excluded.replication_method, product_profile.replication_method),
                hedged_flag = COALESCE(excluded.hedged_flag, product_profile.hedged_flag),
                hedged_target = COALESCE(excluded.hedged_target, product_profile.hedged_target),
                equity_size_hint = COALESCE(excluded.equity_size_hint, product_profile.equity_size_hint),
                equity_style_hint = COALESCE(excluded.equity_style_hint, product_profile.equity_style_hint),
                sector_hint = COALESCE(excluded.sector_hint, product_profile.sector_hint),
                sector_weight = COALESCE(excluded.sector_weight, product_profile.sector_weight),
                updated_at = excluded.updated_at
            """,
            profile_upserts,
        )
        stats.product_profile_rows_upserted = len(profile_upserts)

    return stats
