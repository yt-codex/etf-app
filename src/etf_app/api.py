from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from etf_app.completeness import collect_completeness_snapshot
from etf_app.profile import (
    ProfileSyncStats,
    ensure_instrument_cost_current_view,
    ensure_product_profile_schema,
    refresh_product_profile,
)
from etf_app.recommend import (
    STRATEGIES,
    build_strategy_rows,
    filter_rows_by_venues,
    inspect_gold_policy,
    load_base_candidates,
    load_gold_exception_candidates,
    parse_currency_order,
    venue_scope,
)
from etf_app.taxonomy import ensure_taxonomy_schema, load_universe_rows, upsert_taxonomy


MAX_PAGE_SIZE = 250
DEFAULT_PAGE_SIZE = 50

FUNDS_FROM_SQL = """
FROM instrument i
JOIN listing l
  ON l.instrument_id = i.instrument_id
 AND COALESCE(l.primary_flag, 0) = 1
LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
LEFT JOIN product_profile pp ON pp.instrument_id = i.instrument_id
LEFT JOIN instrument_taxonomy t ON t.instrument_id = i.instrument_id
WHERE COALESCE(i.universe_mvp_flag, 0) = 1
  AND COALESCE(l.status, 'active') = 'active'
"""

FUNDS_SELECT_SQL = f"""
SELECT
    i.instrument_id,
    i.isin,
    i.instrument_name,
    i.instrument_type,
    COALESCE(i.ucits_flag, pp.ucits_flag, 0) AS ucits_flag,
    i.ucits_source,
    COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown') AS issuer_name,
    l.venue_mic AS primary_venue,
    l.ticker,
    NULLIF(TRIM(l.trading_currency), '') AS currency,
    pp.distribution_policy,
    pp.ongoing_charges,
    pp.ongoing_charges_asof,
    pp.benchmark_name,
    pp.asset_class_hint,
    pp.domicile_country,
    pp.fund_size_value,
    pp.fund_size_currency,
    pp.fund_size_asof,
    pp.fund_size_scope,
    pp.replication_method,
    pp.hedged_flag AS profile_hedged_flag,
    pp.hedged_target AS profile_hedged_target,
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
    COALESCE(t.govt_bond_flag, 0) AS govt_bond_flag,
    t.hedged_flag AS taxonomy_hedged_flag,
    t.hedged_target AS taxonomy_hedged_target,
    t.taxonomy_version,
    t.evidence_json
{FUNDS_FROM_SQL}
"""


def open_read_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(Path(db_path)))
    conn.row_factory = sqlite3.Row
    ensure_product_profile_schema(conn)
    ensure_instrument_cost_current_view(conn)
    ensure_taxonomy_schema(conn)
    return conn


def refresh_read_models(db_path: str) -> dict[str, int]:
    conn = open_read_conn(db_path)
    try:
        conn.execute("BEGIN")
        profile_stats = refresh_product_profile(conn)
        taxonomy_rows_refreshed = upsert_taxonomy(conn, load_universe_rows(conn))
        conn.commit()
        return {
            "taxonomy_rows_refreshed": taxonomy_rows_refreshed,
            "product_profile_rows_upserted": profile_stats.product_profile_rows_upserted,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _parse_int(value: Optional[str], *, default: int, minimum: int, maximum: int) -> int:
    if value is None:
        return default
    parsed = int(value)
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def _parse_query_string(raw_query: str) -> dict[str, str]:
    return {key: values[-1] for key, values in parse_qs(raw_query, keep_blank_values=False).items()}


def _resolved_hedged_flag(row: sqlite3.Row) -> Optional[int]:
    profile_flag = row["profile_hedged_flag"]
    if profile_flag in (0, 1):
        return int(profile_flag)
    if int(row["taxonomy_hedged_flag"] or 0) == 1:
        return 1
    return None


def _resolved_hedged_target(row: sqlite3.Row) -> Optional[str]:
    return row["profile_hedged_target"] or row["taxonomy_hedged_target"]


def _serialize_fund_row(row: sqlite3.Row, *, include_evidence: bool = False) -> dict[str, object]:
    payload = {
        "instrument_id": int(row["instrument_id"]),
        "isin": str(row["isin"]),
        "instrument_name": str(row["instrument_name"]),
        "instrument_type": str(row["instrument_type"]),
        "issuer_name": str(row["issuer_name"]),
        "primary_venue": row["primary_venue"],
        "ticker": row["ticker"],
        "currency": row["currency"],
        "ucits_flag": int(row["ucits_flag"] or 0),
        "ucits_source": row["ucits_source"],
        "distribution_policy": row["distribution_policy"],
        "ongoing_charges": row["ongoing_charges"],
        "ongoing_charges_asof": row["ongoing_charges_asof"],
        "benchmark_name": row["benchmark_name"],
        "asset_class_hint": row["asset_class_hint"],
        "domicile_country": row["domicile_country"],
        "fund_size_value": row["fund_size_value"],
        "fund_size_currency": row["fund_size_currency"],
        "fund_size_asof": row["fund_size_asof"],
        "fund_size_scope": row["fund_size_scope"],
        "replication_method": row["replication_method"],
        "hedged_flag": _resolved_hedged_flag(row),
        "hedged_target": _resolved_hedged_target(row),
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
        "duration_years_low": row["duration_years_low"],
        "duration_years_high": row["duration_years_high"],
        "commodity_type": row["commodity_type"],
        "gold_flag": int(row["gold_flag"] or 0),
        "cash_flag": int(row["cash_flag"] or 0),
        "cash_proxy_flag": int(row["cash_proxy_flag"] or 0),
        "govt_bond_flag": int(row["govt_bond_flag"] or 0),
        "taxonomy_version": row["taxonomy_version"],
    }
    if include_evidence:
        evidence_json = row["evidence_json"]
        payload["taxonomy_evidence"] = json.loads(evidence_json) if evidence_json else None
    return payload


def _build_fund_filters(params: dict[str, str]) -> tuple[list[str], list[object]]:
    clauses: list[str] = []
    values: list[object] = []
    scalar_filters = {
        "asset_class": "COALESCE(t.asset_class, 'unknown') = ?",
        "geography_scope": "COALESCE(t.geography_scope, 'unknown') = ?",
        "geography_region": "COALESCE(t.geography_region, 'unknown') = ?",
        "geography_country": "t.geography_country = ?",
        "equity_size": "t.equity_size = ?",
        "equity_style": "t.equity_style = ?",
        "factor": "t.factor = ?",
        "sector": "t.sector = ?",
        "theme": "t.theme = ?",
        "bond_type": "COALESCE(t.bond_type, 'unknown') = ?",
        "duration_bucket": "COALESCE(t.duration_bucket, 'unknown') = ?",
        "commodity_type": "COALESCE(t.commodity_type, 'unknown') = ?",
        "issuer": "COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown') = ?",
        "venue": "l.venue_mic = ?",
        "currency": "NULLIF(TRIM(l.trading_currency), '') = ?",
        "distribution_policy": "pp.distribution_policy = ?",
        "domicile_country": "pp.domicile_country = ?",
        "replication_method": "pp.replication_method = ?",
    }
    for key, clause in scalar_filters.items():
        value = params.get(key)
        if value:
            clauses.append(clause)
            values.append(value)

    search = params.get("q")
    if search:
        needle = f"%{search.strip()}%"
        clauses.append(
            "("
            "i.isin LIKE ? OR i.instrument_name LIKE ? OR l.ticker LIKE ? OR "
            "COALESCE(iss.normalized_name, iss.issuer_name, '') LIKE ? OR "
            "COALESCE(pp.benchmark_name, '') LIKE ?"
            ")"
        )
        values.extend([needle, needle, needle, needle, needle])

    hedged = _parse_bool(params.get("hedged"))
    if hedged is True:
        clauses.append("(pp.hedged_flag = 1 OR (pp.hedged_flag IS NULL AND COALESCE(t.hedged_flag, 0) = 1))")
    elif hedged is False:
        clauses.append("pp.hedged_flag = 0")

    gold = _parse_bool(params.get("gold"))
    if gold is True:
        clauses.append("COALESCE(t.gold_flag, 0) = 1")
    elif gold is False:
        clauses.append("COALESCE(t.gold_flag, 0) = 0")

    return clauses, values


def list_funds(
    conn: sqlite3.Connection,
    *,
    params: dict[str, str],
) -> dict[str, object]:
    limit = _parse_int(params.get("limit"), default=DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE)
    offset = _parse_int(params.get("offset"), default=0, minimum=0, maximum=100_000)
    sort = (params.get("sort") or "name").strip().lower()
    direction = (params.get("direction") or "asc").strip().lower()
    if direction not in {"asc", "desc"}:
        raise ValueError(f"Invalid sort direction: {direction}")

    sort_sql = {
        "name": "i.instrument_name",
        "isin": "i.isin",
        "issuer": "COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown')",
        "venue": "l.venue_mic",
        "currency": "NULLIF(TRIM(l.trading_currency), '')",
        "fee": "pp.ongoing_charges",
        "asset_class": "COALESCE(t.asset_class, 'unknown')",
    }.get(sort)
    if sort_sql is None:
        raise ValueError(f"Unsupported sort field: {sort}")

    clauses, values = _build_fund_filters(params)
    where_sql = "".join(f"\n  AND {clause}" for clause in clauses)
    count_sql = f"SELECT COUNT(*) AS total {FUNDS_FROM_SQL}{where_sql}"
    total = int(conn.execute(count_sql, values).fetchone()["total"])

    if sort == "fee":
        order_sql = f"ORDER BY pp.ongoing_charges IS NULL, {sort_sql} {direction.upper()}, i.isin ASC"
    else:
        order_sql = f"ORDER BY {sort_sql} {direction.upper()}, i.isin ASC"

    rows = conn.execute(
        f"""
        {FUNDS_SELECT_SQL}
        {where_sql}
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        [*values, limit, offset],
    ).fetchall()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [_serialize_fund_row(row) for row in rows],
    }


def get_fund_detail(conn: sqlite3.Connection, isin: str) -> Optional[dict[str, object]]:
    row = conn.execute(
        f"""
        {FUNDS_SELECT_SQL}
          AND i.isin = ?
        LIMIT 1
        """,
        (isin.upper(),),
    ).fetchone()
    if row is None:
        return None
    return _serialize_fund_row(row, include_evidence=True)


def _collect_counts(
    conn: sqlite3.Connection,
    *,
    expression: str,
    where: str,
    limit: Optional[int] = None,
) -> list[dict[str, object]]:
    sql = f"""
        SELECT {expression} AS value, COUNT(*) AS count
        {FUNDS_FROM_SQL}
          AND {where}
        GROUP BY {expression}
        ORDER BY count DESC, value
    """
    if limit is not None:
        sql = f"{sql}\nLIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    return [{"value": row["value"], "count": int(row["count"])} for row in rows]


def list_filter_options(conn: sqlite3.Connection) -> dict[str, object]:
    hedged_counts = {
        "true": int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS count
                {FUNDS_FROM_SQL}
                  AND (pp.hedged_flag = 1 OR (pp.hedged_flag IS NULL AND COALESCE(t.hedged_flag, 0) = 1))
                """
            ).fetchone()["count"]
        ),
        "false": int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS count
                {FUNDS_FROM_SQL}
                  AND pp.hedged_flag = 0
                """
            ).fetchone()["count"]
        ),
        "unknown": int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS count
                {FUNDS_FROM_SQL}
                  AND NOT (pp.hedged_flag = 1 OR pp.hedged_flag = 0)
                  AND COALESCE(t.hedged_flag, 0) = 0
                """
            ).fetchone()["count"]
        ),
    }
    return {
        "asset_class": _collect_counts(
            conn,
            expression="COALESCE(t.asset_class, 'unknown')",
            where="COALESCE(t.asset_class, 'unknown') <> 'unknown'",
        ),
        "geography_region": _collect_counts(
            conn,
            expression="COALESCE(t.geography_region, 'unknown')",
            where="COALESCE(t.geography_region, 'unknown') <> 'unknown'",
        ),
        "equity_size": _collect_counts(conn, expression="t.equity_size", where="t.equity_size IS NOT NULL"),
        "equity_style": _collect_counts(conn, expression="t.equity_style", where="t.equity_style IS NOT NULL"),
        "factor": _collect_counts(conn, expression="t.factor", where="t.factor IS NOT NULL"),
        "sector": _collect_counts(conn, expression="t.sector", where="t.sector IS NOT NULL"),
        "theme": _collect_counts(conn, expression="t.theme", where="t.theme IS NOT NULL"),
        "bond_type": _collect_counts(
            conn,
            expression="COALESCE(t.bond_type, 'unknown')",
            where="COALESCE(t.bond_type, 'unknown') <> 'unknown'",
        ),
        "commodity_type": _collect_counts(
            conn,
            expression="COALESCE(t.commodity_type, 'unknown')",
            where="COALESCE(t.commodity_type, 'unknown') <> 'unknown'",
        ),
        "venue": _collect_counts(conn, expression="l.venue_mic", where="l.venue_mic IS NOT NULL"),
        "currency": _collect_counts(
            conn,
            expression="NULLIF(TRIM(l.trading_currency), '')",
            where="NULLIF(TRIM(l.trading_currency), '') IS NOT NULL",
        ),
        "distribution_policy": _collect_counts(
            conn,
            expression="pp.distribution_policy",
            where="pp.distribution_policy IS NOT NULL AND TRIM(pp.distribution_policy) <> ''",
        ),
        "issuer_top": _collect_counts(
            conn,
            expression="COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown')",
            where="COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown') <> ''",
            limit=25,
        ),
        "hedged_flag": hedged_counts,
    }


def get_strategy_snapshot(
    conn: sqlite3.Connection,
    *,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
    strategy_name: str | None = None,
) -> dict[str, object]:
    currency_order = parse_currency_order(preferred_currency_order)
    selected_venues = venue_scope(venue)
    base_rows = load_base_candidates(conn)
    gold_policy = inspect_gold_policy(conn, base_rows=base_rows, selected_venues=selected_venues)
    gold_exception_rows = load_gold_exception_candidates(conn, selected_venues)

    strategies: list[dict[str, object]] = []
    selected_strategies = list(STRATEGIES)
    if strategy_name:
        selected_strategies = [strategy for strategy in STRATEGIES if str(strategy["name"]) == strategy_name]
        if not selected_strategies:
            raise ValueError(f"unknown strategy_name: {strategy_name}")
    for strategy in selected_strategies:
        rows, emitted, diagnostics = build_strategy_rows(
            strategy,
            base_rows,
            selected_venues=selected_venues,
            top_n=top_n,
            currency_order=currency_order,
            allow_missing_fees=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
            gold_policy=gold_policy,
            gold_exception_rows=gold_exception_rows,
        )
        serialized_rows: list[dict[str, object]] = []
        for row in rows:
            serialized = dict(row)
            selection_reason = serialized.get("selection_reason")
            if selection_reason:
                serialized["selection_reason"] = json.loads(str(selection_reason))
            serialized_rows.append(serialized)
        strategy_diagnostics: dict[str, object] = {}
        for bucket_name, diag in diagnostics.items():
            attempts = [
                {
                    "step": str(attempt["step"]),
                    "venues": [str(venue_name) for venue_name in attempt["venues"]],
                    "allow_missing_fees": bool(attempt["allow_missing_fees"]),
                    "match_mode": str(attempt["match_mode"]),
                    "considered": int(attempt["considered"]),
                    "hard_kept": int(attempt["hard_kept"]),
                    "bucket_matches": int(attempt["bucket_matches"]),
                    "final_selected": int(attempt["final_selected"]),
                }
                for attempt in diag["attempts"]
            ]
            bucket_diag: dict[str, object] = {
                "final_step": str(diag["final_step"]),
                "final_selected": int(diag["final_selected"]),
                "final_hard_kept": int(diag["final_hard_kept"]),
                "final_bucket_matches": int(diag["final_bucket_matches"]),
                "attempts": attempts,
            }
            if bucket_name == "gold":
                bucket_diag["gold_policy"] = asdict(gold_policy)
            strategy_diagnostics[bucket_name] = bucket_diag
        strategies.append(
            {
                "slug": str(strategy.get("slug") or ""),
                "name": str(strategy["name"]),
                "description": str(strategy["description"]),
                "detail": str(strategy.get("detail") or ""),
                "implementation_note": str(strategy.get("implementation_note") or ""),
                "source_url": str(strategy.get("source_url") or ""),
                "filename": str(strategy["filename"]),
                "buckets": [{"bucket_name": bucket_name, "target_weight": float(weight)} for bucket_name, weight in strategy["buckets"]],
                "emitted": {key: int(value) for key, value in emitted.items()},
                "diagnostics": strategy_diagnostics,
                "rows": serialized_rows,
            }
        )
    return {
        "selected_venues": selected_venues,
        "top_n": top_n,
        "preferred_currency_order": currency_order,
        "allow_missing_fees": allow_missing_fees,
        "allow_missing_currency": allow_missing_currency,
        "gold_policy": asdict(gold_policy),
        "strategies": strategies,
    }


def get_completeness_snapshot(
    conn: sqlite3.Connection,
    *,
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    return collect_completeness_snapshot(
        conn,
        db_path=db_path,
        taxonomy_rows_refreshed=0,
        profile_sync_stats=ProfileSyncStats(),
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
    )


def _json_response(start_response: Callable[..., object], status: str, payload: dict[str, object]) -> list[bytes]:
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Methods", "GET, OPTIONS"),
        ("Access-Control-Allow-Headers", "Content-Type"),
    ]
    start_response(status, headers)
    return [body]


def create_app(db_path: str) -> Callable[..., list[bytes]]:
    def app(environ: dict[str, object], start_response: Callable[..., object]) -> list[bytes]:
        method = str(environ.get("REQUEST_METHOD") or "GET").upper()
        path = str(environ.get("PATH_INFO") or "/")
        if method == "OPTIONS":
            return _json_response(start_response, "200 OK", {"ok": True})
        if method != "GET":
            return _json_response(start_response, "405 Method Not Allowed", {"error": "method_not_allowed"})

        params = _parse_query_string(str(environ.get("QUERY_STRING") or ""))
        try:
            conn = open_read_conn(db_path)
            try:
                if path in {"/", "/api"}:
                    return _json_response(
                        start_response,
                        "200 OK",
                        {
                            "service": "etf-app-api",
                            "db_path": db_path,
                            "endpoints": [
                                "/health",
                                "/api/funds",
                                "/api/funds/{isin}",
                                "/api/filters",
                                "/api/strategies",
                                "/api/completeness",
                            ],
                        },
                    )
                if path == "/health":
                    count = int(
                        conn.execute(
                            "SELECT COUNT(*) AS count FROM instrument WHERE COALESCE(universe_mvp_flag, 0) = 1"
                        ).fetchone()["count"]
                    )
                    return _json_response(start_response, "200 OK", {"status": "ok", "mvp_instruments": count})
                if path == "/api/funds":
                    return _json_response(start_response, "200 OK", list_funds(conn, params=params))
                if path.startswith("/api/funds/"):
                    isin = path.rsplit("/", 1)[-1].strip().upper()
                    if not isin:
                        return _json_response(start_response, "400 Bad Request", {"error": "missing_isin"})
                    payload = get_fund_detail(conn, isin)
                    if payload is None:
                        return _json_response(start_response, "404 Not Found", {"error": "fund_not_found", "isin": isin})
                    return _json_response(start_response, "200 OK", payload)
                if path == "/api/filters":
                    return _json_response(start_response, "200 OK", list_filter_options(conn))
                if path == "/api/strategies":
                    payload = get_strategy_snapshot(
                        conn,
                        venue=params.get("venue", "ALL"),
                        preferred_currency_order=params.get("preferred_currency_order", "USD,EUR,GBP"),
                        top_n=_parse_int(params.get("top_n"), default=5, minimum=1, maximum=5000),
                        allow_missing_fees=bool(_parse_bool(params.get("allow_missing_fees")) or False),
                        allow_missing_currency=bool(_parse_bool(params.get("allow_missing_currency")) or False),
                        strategy_name=params.get("strategy_name"),
                    )
                    return _json_response(start_response, "200 OK", payload)
                if path == "/api/completeness":
                    payload = get_completeness_snapshot(
                        conn,
                        db_path=db_path,
                        venue=params.get("venue", "ALL"),
                        preferred_currency_order=params.get("preferred_currency_order", "USD,EUR,GBP"),
                        top_n=_parse_int(params.get("top_n"), default=5, minimum=1, maximum=25),
                        allow_missing_fees=bool(_parse_bool(params.get("allow_missing_fees")) or False),
                        allow_missing_currency=bool(_parse_bool(params.get("allow_missing_currency")) or False),
                        )
                    return _json_response(start_response, "200 OK", payload)
            finally:
                conn.close()
        except ValueError as exc:
            return _json_response(start_response, "400 Bad Request", {"error": "invalid_request", "detail": str(exc)})
        except Exception as exc:
            return _json_response(start_response, "500 Internal Server Error", {"error": "server_error", "detail": str(exc)})

        return _json_response(start_response, "404 Not Found", {"error": "not_found"})

    return app


def run_api_server(
    *,
    db_path: str,
    host: str,
    port: int,
    refresh_derived_on_start: bool,
) -> int:
    db = Path(db_path)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    if refresh_derived_on_start:
        stats = refresh_read_models(str(db))
        print(
            "refreshed read models: "
            f"product_profile_rows_upserted={stats['product_profile_rows_upserted']} "
            f"taxonomy_rows_refreshed={stats['taxonomy_rows_refreshed']}"
        )

    print(f"serving API on http://{host}:{port}")
    try:
        with make_server(host, port, create_app(str(db))) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("API server stopped")
    return 0
