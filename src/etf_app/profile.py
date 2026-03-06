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
            replication_method TEXT NULL,
            hedged_flag INTEGER NULL CHECK (hedged_flag IN (0, 1)),
            hedged_target TEXT NULL,
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
    ensure_column(conn, "product_profile", "replication_method", "TEXT")
    ensure_column(conn, "product_profile", "hedged_flag", "INTEGER")
    ensure_column(conn, "product_profile", "hedged_target", "TEXT")
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


def _extract_profile_metadata_from_payload(payload: dict[str, object]) -> dict[str, object]:
    extracted: dict[str, object] = {}
    for container_key in ("parsed", "parse"):
        nested = payload.get(container_key)
        if isinstance(nested, dict):
            payload = {**payload, **nested}

    benchmark_name = _normalize_text_value(payload.get("benchmark_name"))
    asset_class_hint = _normalize_text_value(payload.get("asset_class_hint"))
    domicile_country = _normalize_text_value(payload.get("domicile_country"))
    replication_method = _normalize_text_value(payload.get("replication_method"))
    hedged_flag = _coerce_optional_flag(payload.get("hedged_flag"))
    hedged_target = _normalize_text_value(payload.get("hedged_target"))
    if hedged_flag is None and hedged_target is not None:
        hedged_flag = 1

    extracted["benchmark_name"] = benchmark_name
    extracted["asset_class_hint"] = asset_class_hint
    extracted["domicile_country"] = domicile_country
    extracted["replication_method"] = replication_method
    extracted["hedged_flag"] = hedged_flag
    extracted["hedged_target"] = hedged_target
    return extracted


def _load_latest_profile_metadata(conn: sqlite3.Connection) -> dict[int, dict[str, object]]:
    try:
        rows = conn.execute(
            """
            SELECT instrument_id, raw_json
            FROM issuer_metadata_snapshot
            WHERE raw_json IS NOT NULL
              AND TRIM(raw_json) <> ''
            ORDER BY asof_date DESC, id DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    per_instrument: dict[int, dict[str, object]] = {}
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
                "replication_method": None,
                "hedged_flag": None,
                "hedged_target": None,
            },
        )
        for key, value in extracted.items():
            if value is None:
                continue
            if current.get(key) is None:
                current[key] = value
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
        SELECT instrument_id, instrument_name, ucits_flag, ucits_source
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
            Optional[str],
            Optional[int],
            Optional[str],
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
        domicile_country = _normalize_text_value(metadata.get("domicile_country"))
        replication_method = _normalize_text_value(metadata.get("replication_method"))
        hedged_flag = _coerce_optional_flag(metadata.get("hedged_flag"))
        hedged_target = _normalize_text_value(metadata.get("hedged_target"))

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
                replication_method,
                hedged_flag,
                hedged_target,
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
                    replication_method,
                    hedged_flag,
                    hedged_target,
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
                replication_method,
                hedged_flag,
                hedged_target,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                replication_method = COALESCE(excluded.replication_method, product_profile.replication_method),
                hedged_flag = COALESCE(excluded.hedged_flag, product_profile.hedged_flag),
                hedged_target = COALESCE(excluded.hedged_target, product_profile.hedged_target),
                updated_at = excluded.updated_at
            """,
            profile_upserts,
        )
        stats.product_profile_rows_upserted = len(profile_upserts)

    return stats
