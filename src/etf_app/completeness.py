from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Optional

from etf_app.profile import (
    ProfileSyncStats,
    ensure_instrument_cost_current_view,
    ensure_product_profile_schema,
    refresh_product_profile,
)
from etf_app.recommend import (
    STRATEGIES,
    apply_hard_filters,
    build_strategy_rows,
    filter_rows_by_venues,
    inspect_gold_policy,
    load_base_candidates,
    parse_currency_order,
    venue_scope,
)
from etf_app.taxonomy import ensure_taxonomy_schema, load_universe_rows, upsert_taxonomy


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def pct(known: int, total: int) -> float:
    return round((100.0 * known / total), 2) if total else 0.0


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, object]:
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def _safe_count(value: object) -> int:
    return int(value or 0)


def _field_coverage(known: int, total: int) -> dict[str, object]:
    return {"known": known, "total": total, "pct": pct(known, total)}


def _write_json_artifact(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        return path
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_latest{path.suffix}")
        fallback.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        print(f"warning: {path.name} is locked; wrote {fallback.name} instead")
        return fallback


def collect_universe_summary(conn: sqlite3.Connection) -> dict[str, object]:
    overview = _row_to_dict(
        conn.execute(
            """
            SELECT
                COUNT(*) AS total_instruments,
                SUM(CASE WHEN COALESCE(i.status, 'active') = 'active' THEN 1 ELSE 0 END) AS active_instruments,
                SUM(CASE WHEN l.listing_id IS NOT NULL THEN 1 ELSE 0 END) AS active_primary_listings
            FROM instrument i
            LEFT JOIN listing l
              ON l.instrument_id = i.instrument_id
             AND COALESCE(l.primary_flag, 0) = 1
             AND COALESCE(l.status, 'active') = 'active'
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            """
        ).fetchone()
    )
    venues = [
        {"venue": str(row["venue"]), "count": int(row["count"])}
        for row in conn.execute(
            """
            SELECT COALESCE(l.venue_mic, 'NULL') AS venue, COUNT(*) AS count
            FROM instrument i
            LEFT JOIN listing l
              ON l.instrument_id = i.instrument_id
             AND COALESCE(l.primary_flag, 0) = 1
             AND COALESCE(l.status, 'active') = 'active'
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            GROUP BY COALESCE(l.venue_mic, 'NULL')
            ORDER BY count DESC, venue
            """
        )
    ]
    return {"overview": overview, "primary_venues": venues}


def collect_profile_summary(conn: sqlite3.Connection) -> dict[str, object]:
    row = _row_to_dict(
        conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN p.instrument_id IS NOT NULL THEN 1 ELSE 0 END) AS profile_rows,
                SUM(CASE WHEN p.distribution_policy IS NOT NULL AND TRIM(p.distribution_policy) <> '' THEN 1 ELSE 0 END) AS distribution_policy,
                SUM(CASE WHEN p.ucits_flag IS NOT NULL THEN 1 ELSE 0 END) AS ucits_flag,
                SUM(CASE WHEN p.ongoing_charges IS NOT NULL THEN 1 ELSE 0 END) AS ongoing_charges,
                SUM(CASE WHEN p.benchmark_name IS NOT NULL AND TRIM(p.benchmark_name) <> '' THEN 1 ELSE 0 END) AS benchmark_name,
                SUM(CASE WHEN p.asset_class_hint IS NOT NULL AND TRIM(p.asset_class_hint) <> '' THEN 1 ELSE 0 END) AS asset_class_hint,
                SUM(CASE WHEN p.domicile_country IS NOT NULL AND TRIM(p.domicile_country) <> '' THEN 1 ELSE 0 END) AS domicile_country,
                SUM(CASE WHEN p.replication_method IS NOT NULL AND TRIM(p.replication_method) <> '' THEN 1 ELSE 0 END) AS replication_method,
                SUM(CASE WHEN p.hedged_flag IS NOT NULL THEN 1 ELSE 0 END) AS hedged_flag,
                SUM(CASE WHEN p.hedged_target IS NOT NULL AND TRIM(p.hedged_target) <> '' THEN 1 ELSE 0 END) AS hedged_target
            FROM instrument i
            LEFT JOIN product_profile p ON p.instrument_id = i.instrument_id
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            """
        ).fetchone()
    )
    total = _safe_count(row.get("total"))
    return {
        "total": total,
        "fields": {
            "profile_rows": _field_coverage(_safe_count(row.get("profile_rows")), total),
            "distribution_policy": _field_coverage(_safe_count(row.get("distribution_policy")), total),
            "ucits_flag": _field_coverage(_safe_count(row.get("ucits_flag")), total),
            "ongoing_charges": _field_coverage(_safe_count(row.get("ongoing_charges")), total),
            "benchmark_name": _field_coverage(_safe_count(row.get("benchmark_name")), total),
            "asset_class_hint": _field_coverage(_safe_count(row.get("asset_class_hint")), total),
            "domicile_country": _field_coverage(_safe_count(row.get("domicile_country")), total),
            "replication_method": _field_coverage(_safe_count(row.get("replication_method")), total),
            "hedged_flag": _field_coverage(_safe_count(row.get("hedged_flag")), total),
            "hedged_target": _field_coverage(_safe_count(row.get("hedged_target")), total),
        },
    }


def collect_taxonomy_summary(conn: sqlite3.Connection) -> dict[str, object]:
    row = _row_to_dict(
        conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN COALESCE(t.asset_class, 'unknown') <> 'unknown' THEN 1 ELSE 0 END) AS asset_class_known,
                SUM(CASE WHEN COALESCE(t.geography_region, 'unknown') <> 'unknown' OR t.geography_country IS NOT NULL THEN 1 ELSE 0 END) AS geography_known,
                SUM(CASE WHEN t.asset_class = 'equity' THEN 1 ELSE 0 END) AS equity_total,
                SUM(CASE WHEN t.asset_class = 'equity' AND (COALESCE(t.geography_region, 'unknown') <> 'unknown' OR t.geography_country IS NOT NULL) THEN 1 ELSE 0 END) AS equity_geography_known,
                SUM(CASE WHEN t.asset_class = 'equity' AND t.equity_size IS NOT NULL THEN 1 ELSE 0 END) AS equity_size_known,
                SUM(CASE WHEN t.asset_class = 'equity' AND t.equity_style IS NOT NULL THEN 1 ELSE 0 END) AS equity_style_known,
                SUM(CASE WHEN t.asset_class = 'equity' AND t.factor IS NOT NULL THEN 1 ELSE 0 END) AS equity_factor_known,
                SUM(CASE WHEN t.asset_class = 'equity' AND t.sector IS NOT NULL THEN 1 ELSE 0 END) AS equity_sector_known,
                SUM(CASE WHEN t.asset_class = 'equity' AND t.theme IS NOT NULL THEN 1 ELSE 0 END) AS equity_theme_known,
                SUM(CASE WHEN t.asset_class = 'bond' THEN 1 ELSE 0 END) AS bond_total,
                SUM(CASE WHEN t.asset_class = 'bond' AND COALESCE(t.bond_type, 'unknown') <> 'unknown' THEN 1 ELSE 0 END) AS bond_type_known,
                SUM(CASE WHEN t.asset_class = 'bond' AND COALESCE(t.duration_bucket, 'unknown') <> 'unknown' THEN 1 ELSE 0 END) AS bond_duration_known,
                SUM(CASE WHEN t.asset_class = 'bond' AND (t.duration_years_low IS NOT NULL OR t.duration_years_high IS NOT NULL) THEN 1 ELSE 0 END) AS bond_duration_bounds_known
            FROM instrument i
            LEFT JOIN instrument_taxonomy t ON t.instrument_id = i.instrument_id
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            """
        ).fetchone()
    )
    total = _safe_count(row.get("total"))
    equity_total = _safe_count(row.get("equity_total"))
    bond_total = _safe_count(row.get("bond_total"))
    asset_classes = [
        {"asset_class": str(r["asset_class"]), "count": int(r["count"])}
        for r in conn.execute(
            """
            SELECT COALESCE(t.asset_class, 'unknown') AS asset_class, COUNT(*) AS count
            FROM instrument i
            LEFT JOIN instrument_taxonomy t ON t.instrument_id = i.instrument_id
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            GROUP BY COALESCE(t.asset_class, 'unknown')
            ORDER BY count DESC, asset_class
            """
        )
    ]
    return {
        "total": total,
        "fields": {
            "asset_class_known": _field_coverage(_safe_count(row.get("asset_class_known")), total),
            "geography_known": _field_coverage(_safe_count(row.get("geography_known")), total),
        },
        "equity": {
            "total": equity_total,
            "geography_known": _field_coverage(_safe_count(row.get("equity_geography_known")), equity_total),
            "size_known": _field_coverage(_safe_count(row.get("equity_size_known")), equity_total),
            "style_known": _field_coverage(_safe_count(row.get("equity_style_known")), equity_total),
            "factor_known": _field_coverage(_safe_count(row.get("equity_factor_known")), equity_total),
            "sector_known": _field_coverage(_safe_count(row.get("equity_sector_known")), equity_total),
            "theme_known": _field_coverage(_safe_count(row.get("equity_theme_known")), equity_total),
        },
        "bond": {
            "total": bond_total,
            "bond_type_known": _field_coverage(_safe_count(row.get("bond_type_known")), bond_total),
            "duration_bucket_known": _field_coverage(_safe_count(row.get("bond_duration_known")), bond_total),
            "duration_bounds_known": _field_coverage(_safe_count(row.get("bond_duration_bounds_known")), bond_total),
        },
        "asset_class_distribution": asset_classes,
    }


def collect_fee_gap_summary(conn: sqlite3.Connection) -> dict[str, object]:
    overall = _row_to_dict(
        conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN c.ongoing_charges IS NULL THEN 1 ELSE 0 END) AS missing_fees,
                SUM(CASE WHEN c.ongoing_charges IS NULL AND (l.trading_currency IS NULL OR TRIM(l.trading_currency) = '') THEN 1 ELSE 0 END) AS missing_fees_and_currency
            FROM instrument i
            LEFT JOIN listing l
              ON l.instrument_id = i.instrument_id
             AND COALESCE(l.primary_flag, 0) = 1
             AND COALESCE(l.status, 'active') = 'active'
            LEFT JOIN instrument_cost_current c ON c.instrument_id = i.instrument_id
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
            """
        ).fetchone()
    )
    by_issuer = [
        {"issuer": str(row["issuer"]), "missing_fee_count": int(row["missing_fee_count"])}
        for row in conn.execute(
            """
            SELECT
                COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown') AS issuer,
                COUNT(*) AS missing_fee_count
            FROM instrument i
            LEFT JOIN issuer iss ON iss.issuer_id = i.issuer_id
            LEFT JOIN instrument_cost_current c ON c.instrument_id = i.instrument_id
            WHERE COALESCE(i.universe_mvp_flag, 0) = 1
              AND c.ongoing_charges IS NULL
            GROUP BY COALESCE(iss.normalized_name, iss.issuer_name, 'Unknown')
            ORDER BY missing_fee_count DESC, issuer
            LIMIT 10
            """
        )
    ]
    return {"overview": overall, "missing_fees_top_issuers": by_issuer}


def collect_strategy_summary(
    conn: sqlite3.Connection,
    *,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    currency_order = parse_currency_order(preferred_currency_order)
    selected_venues = venue_scope(venue)
    base_rows = load_base_candidates(conn)
    selected_scope_rows = filter_rows_by_venues(base_rows, selected_venues)
    strict_candidates, strict_excluded_counts, considered_count = apply_hard_filters(
        selected_scope_rows,
        bucket_name="equity_global",
        allow_missing_fees=False,
        allow_missing_currency=allow_missing_currency,
    )
    gold_policy = inspect_gold_policy(conn, base_rows=base_rows, selected_venues=selected_venues)

    strategies: dict[str, object] = {}
    for strategy in STRATEGIES:
        rows, emitted, diagnostics = build_strategy_rows(
            strategy,
            base_rows,
            selected_venues=selected_venues,
            top_n=top_n,
            currency_order=currency_order,
            allow_missing_fees=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
            gold_policy=gold_policy,
        )
        buckets: dict[str, object] = {}
        for bucket_name, _target_weight in strategy["buckets"]:
            diag = diagnostics[bucket_name]
            attempts = [
                {
                    "step": str(attempt["step"]),
                    "venues": [str(v) for v in attempt["venues"]],
                    "allow_missing_fees": bool(attempt["allow_missing_fees"]),
                    "match_mode": str(attempt["match_mode"]),
                    "considered": int(attempt["considered"]),
                    "hard_kept": int(attempt["hard_kept"]),
                    "bucket_matches": int(attempt["bucket_matches"]),
                    "final_selected": int(attempt["final_selected"]),
                }
                for attempt in diag["attempts"]
            ]
            bucket_summary: dict[str, object] = {
                "emitted": int(emitted[bucket_name]),
                "final_step": str(diag["final_step"]),
                "final_selected": int(diag["final_selected"]),
                "final_hard_kept": int(diag["final_hard_kept"]),
                "final_bucket_matches": int(diag["final_bucket_matches"]),
                "attempts": attempts,
            }
            if bucket_name == "gold":
                bucket_summary["gold_policy"] = {
                    "policy_name": gold_policy.policy_name,
                    "eligible_ucits_gold_count": gold_policy.eligible_ucits_gold_count,
                    "excluded_non_ucits_gold_count": gold_policy.excluded_non_ucits_gold_count,
                    "ignored_gold_equity_proxy_count": gold_policy.ignored_gold_equity_proxy_count,
                    "note": gold_policy.note,
                }
            buckets[bucket_name] = bucket_summary
        strategies[str(strategy["name"])] = {
            "total_rows_emitted": len(rows),
            "min_candidates_per_bucket": min(emitted.values()) if emitted else 0,
            "buckets": buckets,
        }

    return {
        "selected_venues": selected_venues,
        "base_rows_total": len(base_rows),
        "base_rows_selected_venues": len(selected_scope_rows),
        "strict_hard_filters": {
            "considered": considered_count,
            "kept": len(strict_candidates),
            "excluded": strict_excluded_counts,
        },
        "strategies": strategies,
    }


def collect_completeness_snapshot(
    conn: sqlite3.Connection,
    *,
    db_path: str,
    taxonomy_rows_refreshed: int,
    profile_sync_stats: ProfileSyncStats,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> dict[str, object]:
    return {
        "generated_at": now_utc_iso(),
        "db_path": db_path,
        "inputs_refreshed": {
            "taxonomy_rows_refreshed": taxonomy_rows_refreshed,
            "product_profile_rows_upserted": profile_sync_stats.product_profile_rows_upserted,
            "metadata_synced": profile_sync_stats.metadata_synced,
            "costs_synced": profile_sync_stats.costs_synced,
            "distributions_synced": profile_sync_stats.distributions_synced,
            "instruments_ucits_updated": profile_sync_stats.instruments_ucits_updated,
        },
        "universe": collect_universe_summary(conn),
        "product_profile": collect_profile_summary(conn),
        "taxonomy": collect_taxonomy_summary(conn),
        "fee_gaps": collect_fee_gap_summary(conn),
        "strategy_readiness": collect_strategy_summary(
            conn,
            venue=venue,
            preferred_currency_order=preferred_currency_order,
            top_n=top_n,
            allow_missing_fees=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
        ),
    }


def print_completeness_summary(report_path: Path, snapshot: dict[str, object]) -> None:
    profile = snapshot["product_profile"]
    taxonomy = snapshot["taxonomy"]
    strategy = snapshot["strategy_readiness"]
    print(f"\ncompleteness_report: {report_path}")
    print(
        "profile ongoing_charges coverage: "
        f"{profile['fields']['ongoing_charges']['known']}/{profile['fields']['ongoing_charges']['total']} "
        f"({profile['fields']['ongoing_charges']['pct']:.2f}%)"
    )
    print(
        "profile benchmark_name coverage: "
        f"{profile['fields']['benchmark_name']['known']}/{profile['fields']['benchmark_name']['total']} "
        f"({profile['fields']['benchmark_name']['pct']:.2f}%)"
    )
    print(
        "equity geography coverage: "
        f"{taxonomy['equity']['geography_known']['known']}/{taxonomy['equity']['geography_known']['total']} "
        f"({taxonomy['equity']['geography_known']['pct']:.2f}%)"
    )
    print(
        "bond duration coverage: "
        f"{taxonomy['bond']['duration_bucket_known']['known']}/{taxonomy['bond']['duration_bucket_known']['total']} "
        f"({taxonomy['bond']['duration_bucket_known']['pct']:.2f}%)"
    )
    print(
        "strict hard-filter candidates: "
        f"{strategy['strict_hard_filters']['kept']}/{strategy['strict_hard_filters']['considered']}"
    )


def generate_completeness_report(
    *,
    db_path: str,
    artifacts_dir: str,
    venue: str = "ALL",
    preferred_currency_order: str = "USD,EUR,GBP",
    top_n: int = 5,
    allow_missing_fees: bool = False,
    allow_missing_currency: bool = False,
) -> Path:
    db = Path(db_path)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        ensure_product_profile_schema(conn)
        ensure_instrument_cost_current_view(conn)
        profile_stats = refresh_product_profile(conn)
        ensure_taxonomy_schema(conn)
        taxonomy_rows_refreshed = upsert_taxonomy(conn, load_universe_rows(conn))
        snapshot = collect_completeness_snapshot(
            conn,
            db_path=str(db),
            taxonomy_rows_refreshed=taxonomy_rows_refreshed,
            profile_sync_stats=profile_stats,
            venue=venue,
            preferred_currency_order=preferred_currency_order,
            top_n=top_n,
            allow_missing_fees=allow_missing_fees,
            allow_missing_currency=allow_missing_currency,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    output_path = _write_json_artifact(Path(artifacts_dir) / "completeness_report.json", snapshot)
    print_completeness_summary(output_path, snapshot)
    return output_path
