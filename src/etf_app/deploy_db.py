from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from etf_app.profile import ensure_instrument_cost_current_view, ensure_product_profile_schema
from etf_app.taxonomy import ensure_taxonomy_schema


RUNTIME_TABLES = (
    "instrument",
    "issuer",
    "listing",
    "product_profile",
    "instrument_taxonomy",
    "cost_snapshot",
)

CURRENT_COST_QUALITY_FLAGS = (
    "ok",
    "partial",
    "issuer_page_ok",
    "amundi_factsheet_ok",
    "avantis_kid_ok",
    "wisdomtree_kid_ok",
    "invesco_kid_ok",
    "lse_ter_ok",
)

STRATEGY_EXCEPTION_VENUES = ("XLON", "XETR")


@dataclass(frozen=True)
class DeployDbStats:
    source_path: str
    output_path: str
    instrument_rows: int
    issuer_rows: int
    listing_rows: int
    product_profile_rows: int
    instrument_taxonomy_rows: int
    cost_snapshot_rows: int
    source_size_bytes: int
    output_size_bytes: int


def _copy_table_schema(source: sqlite3.Connection, target: sqlite3.Connection, table_name: str) -> bool:
    row = source.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
          AND sql IS NOT NULL
        """,
        (table_name,),
    ).fetchone()
    if row is None or not row[0]:
        return False
    target.execute(str(row[0]))
    return True


def _copy_indexes(source: sqlite3.Connection, target: sqlite3.Connection, table_name: str) -> None:
    rows = source.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'index'
          AND tbl_name = ?
          AND sql IS NOT NULL
        ORDER BY name
        """,
        (table_name,),
    ).fetchall()
    for row in rows:
        target.execute(str(row[0]))


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[object, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row is not None else 0


def build_deploy_db(*, source_db_path: str, output_db_path: str) -> DeployDbStats:
    source_path = Path(source_db_path)
    output_path = Path(output_db_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source DB not found: {source_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(output_path))
    try:
        source_conn.row_factory = sqlite3.Row
        target_conn.row_factory = sqlite3.Row
        target_conn.execute("PRAGMA foreign_keys = OFF")
        target_conn.execute("PRAGMA journal_mode = DELETE")
        target_conn.execute("PRAGMA synchronous = NORMAL")

        copied_tables = {table_name: _copy_table_schema(source_conn, target_conn, table_name) for table_name in RUNTIME_TABLES}

        target_conn.execute("ATTACH DATABASE ? AS source_db", (str(source_path),))
        venue_placeholders = ",".join("?" for _ in STRATEGY_EXCEPTION_VENUES)
        target_conn.execute(
            f"""
            CREATE TEMP TABLE selected_instrument_ids AS
            SELECT DISTINCT instrument_id
            FROM (
                SELECT instrument_id
                FROM source_db.instrument
                WHERE COALESCE(universe_mvp_flag, 0) = 1

                UNION

                SELECT i.instrument_id
                FROM source_db.instrument i
                JOIN source_db.listing l
                  ON l.instrument_id = i.instrument_id
                 AND COALESCE(l.primary_flag, 0) = 1
                 AND COALESCE(l.status, 'active') = 'active'
                WHERE COALESCE(i.universe_mvp_flag, 0) = 0
                  AND l.venue_mic IN ({venue_placeholders})
                  AND (
                      UPPER(i.instrument_name) LIKE '%GOLD%'
                      OR UPPER(i.instrument_name) LIKE '%BULLION%'
                  )
            )
            """,
            STRATEGY_EXCEPTION_VENUES,
        )
        target_conn.execute("CREATE INDEX temp.idx_selected_instrument_ids ON selected_instrument_ids(instrument_id)")
        target_conn.execute(
            """
            CREATE TEMP TABLE selected_issuer_ids AS
            SELECT DISTINCT issuer_id
            FROM source_db.instrument
            WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
              AND issuer_id IS NOT NULL
            """
        )
        target_conn.execute("CREATE INDEX temp.idx_selected_issuer_ids ON selected_issuer_ids(issuer_id)")

        target_conn.execute(
            """
            INSERT INTO instrument
            SELECT *
            FROM source_db.instrument
            WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
            """
        )
        target_conn.execute(
            """
            INSERT INTO issuer
            SELECT *
            FROM source_db.issuer
            WHERE issuer_id IN (SELECT issuer_id FROM selected_issuer_ids)
            """
        )
        target_conn.execute(
            """
            INSERT INTO listing
            SELECT *
            FROM source_db.listing
            WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
              AND COALESCE(primary_flag, 0) = 1
              AND COALESCE(status, 'active') = 'active'
            """
        )

        if copied_tables["product_profile"] and _table_exists(source_conn, "product_profile"):
            target_conn.execute(
                """
                INSERT INTO product_profile
                SELECT *
                FROM source_db.product_profile
                WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
                """
            )
        if copied_tables["instrument_taxonomy"] and _table_exists(source_conn, "instrument_taxonomy"):
            target_conn.execute(
                """
                INSERT INTO instrument_taxonomy
                SELECT *
                FROM source_db.instrument_taxonomy
                WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
                """
            )
        if copied_tables["cost_snapshot"] and _table_exists(source_conn, "cost_snapshot"):
            placeholders = ",".join("?" for _ in CURRENT_COST_QUALITY_FLAGS)
            target_conn.execute(
                f"""
                INSERT INTO cost_snapshot
                SELECT cs.*
                FROM source_db.cost_snapshot cs
                JOIN (
                    WITH ranked AS (
                        SELECT
                            cost_id,
                            instrument_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY instrument_id
                                ORDER BY asof_date DESC, cost_id DESC
                            ) AS rn
                        FROM source_db.cost_snapshot
                        WHERE instrument_id IN (SELECT instrument_id FROM selected_instrument_ids)
                          AND ongoing_charges IS NOT NULL
                          AND quality_flag IN ({placeholders})
                    )
                    SELECT cost_id
                    FROM ranked
                    WHERE rn = 1
                ) keep
                  ON keep.cost_id = cs.cost_id
                """,
                CURRENT_COST_QUALITY_FLAGS,
            )

        target_conn.commit()
        target_conn.execute("DETACH DATABASE source_db")

        for table_name in RUNTIME_TABLES:
            if copied_tables.get(table_name):
                _copy_indexes(source_conn, target_conn, table_name)

        ensure_product_profile_schema(target_conn)
        ensure_instrument_cost_current_view(target_conn)
        ensure_taxonomy_schema(target_conn)
        target_conn.commit()
        target_conn.execute("VACUUM")
    finally:
        source_conn.close()
        target_conn.close()

    verify = sqlite3.connect(str(output_path))
    try:
        instrument_rows = _scalar(verify, "SELECT COUNT(*) FROM instrument")
        issuer_rows = _scalar(verify, "SELECT COUNT(*) FROM issuer")
        listing_rows = _scalar(verify, "SELECT COUNT(*) FROM listing")
        product_profile_rows = _scalar(verify, "SELECT COUNT(*) FROM product_profile")
        instrument_taxonomy_rows = _scalar(verify, "SELECT COUNT(*) FROM instrument_taxonomy")
        cost_snapshot_rows = _scalar(verify, "SELECT COUNT(*) FROM cost_snapshot")
    finally:
        verify.close()

    return DeployDbStats(
        source_path=str(source_path),
        output_path=str(output_path),
        instrument_rows=instrument_rows,
        issuer_rows=issuer_rows,
        listing_rows=listing_rows,
        product_profile_rows=product_profile_rows,
        instrument_taxonomy_rows=instrument_taxonomy_rows,
        cost_snapshot_rows=cost_snapshot_rows,
        source_size_bytes=source_path.stat().st_size,
        output_size_bytes=output_path.stat().st_size,
    )
