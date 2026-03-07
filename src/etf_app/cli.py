from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from etf_app import listing_hygiene, listing_ingest, universe_refine
from etf_app.completeness import generate_completeness_report
from etf_app.issuer_fee_enrich import run_issuer_fee_backfill
from etf_app.recommend import run_recommendations
from etf_app.taxonomy import ensure_taxonomy_schema, load_universe_rows, print_taxonomy_stats, upsert_taxonomy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETF pipeline entrypoint")
    subparsers = parser.add_subparsers(dest="command", required=True)

    patch_data = subparsers.add_parser(
        "rebuild-derived-data",
        aliases=["patch-data"],
        help="Rebuild derived data without fetching new listings",
    )
    patch_data.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    patch_data.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated CSV artifacts",
    )

    refresh = subparsers.add_parser(
        "refresh-data",
        aliases=["stage1-refresh"],
        help="Run listing ingest, hygiene, and universe rebuild end to end",
    )
    refresh.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    refresh.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated CSV artifacts",
    )
    refresh.add_argument(
        "--skip-cboe",
        action="store_true",
        help="Skip optional Cboe ingestion",
    )

    classify = subparsers.add_parser(
        "build-taxonomy",
        aliases=["classify-taxonomy"],
        help="Build normalized taxonomy and legacy compatibility classifications",
    )
    classify.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")

    completeness = subparsers.add_parser(
        "report-completeness",
        help="Write a JSON completeness report for profile, taxonomy, and strategy readiness",
    )
    completeness.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    completeness.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated report artifacts",
    )
    completeness.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue filter for strategy-readiness stats",
    )
    completeness.add_argument(
        "--preferred-currency-order",
        default="USD,EUR,GBP",
        help="Currency sort order, comma separated (default: USD,EUR,GBP)",
    )
    completeness.add_argument("--top-n", type=int, default=5, help="Top candidates per bucket")
    completeness.add_argument(
        "--allow-missing-fees",
        action="store_true",
        help="Allow strategy-readiness fallbacks that tolerate missing fees",
    )
    completeness.add_argument(
        "--allow-missing-currency",
        action="store_true",
        help="Allow strategy-readiness stats to tolerate missing trading currency",
    )

    issuer_fees = subparsers.add_parser(
        "backfill-issuer-fees",
        help="Backfill missing fees from official issuer product-list PDFs",
    )
    issuer_fees.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    issuer_fees.add_argument(
        "--source",
        action="append",
        default=[],
        help="Optional source key filter; repeatable. Supported: spdr, jpmorgan",
    )

    recommend = subparsers.add_parser(
        "recommend",
        aliases=["recommend-strategies"],
        help="Generate taxonomy-backed strategy recommendations",
    )
    recommend.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    recommend.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue filter (ALL means XLON+XETR only)",
    )
    recommend.add_argument(
        "--preferred-currency-order",
        default="USD,EUR,GBP",
        help="Currency sort order, comma separated (default: USD,EUR,GBP)",
    )
    recommend.add_argument("--top-n", type=int, default=5, help="Top candidates per bucket")
    recommend.add_argument(
        "--allow-missing-fees",
        action="store_true",
        help="Allow candidates without ongoing_charges after strict attempts",
    )
    recommend.add_argument(
        "--allow-missing-currency",
        action="store_true",
        help="Allow candidates with missing trading currency (pushed to bottom)",
    )
    recommend.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated recommendation CSVs",
    )
    return parser


def _artifact_path(root: str, filename: str) -> str:
    return str(Path(root) / filename)


def run_patch_data(db_path: str, artifacts_dir: str) -> int:
    primary_csv = _artifact_path(artifacts_dir, "primary_listings.csv")
    universe_csv = _artifact_path(artifacts_dir, "universe_mvp.csv")

    listing_hygiene.main(["--db-path", db_path, "--output-csv", primary_csv])
    universe_refine.main(["--db-path", db_path, "--output-csv", universe_csv])
    run_completeness_report(
        db_path=db_path,
        artifacts_dir=artifacts_dir,
        venue="ALL",
        preferred_currency_order="USD,EUR,GBP",
        top_n=5,
        allow_missing_fees=False,
        allow_missing_currency=False,
    )
    return 0


def run_stage1_refresh(db_path: str, artifacts_dir: str, skip_cboe: bool) -> int:
    ingest_args = ["--db-path", db_path]
    if skip_cboe:
        ingest_args.append("--skip-cboe")
    listing_ingest.main(ingest_args)
    return run_patch_data(db_path, artifacts_dir)


def run_classify_taxonomy(db_path: str) -> int:
    conn = sqlite3.connect(str(Path(db_path)))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        ensure_taxonomy_schema(conn)
        rows = load_universe_rows(conn)
        updated = upsert_taxonomy(conn, rows)
        conn.commit()
        print(f"classified instruments: {updated}")
        print_taxonomy_stats(conn)
        run_completeness_report(
            db_path=db_path,
            artifacts_dir="artifacts",
            venue="ALL",
            preferred_currency_order="USD,EUR,GBP",
            top_n=5,
            allow_missing_fees=False,
            allow_missing_currency=False,
        )
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_completeness_report(
    db_path: str,
    artifacts_dir: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
) -> int:
    generate_completeness_report(
        db_path=db_path,
        artifacts_dir=artifacts_dir,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
    )
    return 0


def run_issuer_fee_enrichment(db_path: str, source: list[str]) -> int:
    results = run_issuer_fee_backfill(db_path=db_path, source_keys=source)
    for key, stats in results.items():
        print(f"{key}: attempted={stats['attempted']} matched={stats['matched']} inserted={stats['inserted']}")
    return 0


def run_recommend_strategies(
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
    allow_missing_fees: bool,
    allow_missing_currency: bool,
    artifacts_dir: str,
) -> int:
    return run_recommendations(
        db_path=db_path,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
        allow_missing_fees=allow_missing_fees,
        allow_missing_currency=allow_missing_currency,
        artifacts_dir=artifacts_dir,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in {"rebuild-derived-data", "patch-data"}:
        return run_patch_data(args.db_path, args.artifacts_dir)
    if args.command in {"refresh-data", "stage1-refresh"}:
        return run_stage1_refresh(args.db_path, args.artifacts_dir, args.skip_cboe)
    if args.command in {"build-taxonomy", "classify-taxonomy"}:
        return run_classify_taxonomy(args.db_path)
    if args.command == "report-completeness":
        return run_completeness_report(
            args.db_path,
            args.artifacts_dir,
            args.venue,
            args.preferred_currency_order,
            args.top_n,
            args.allow_missing_fees,
            args.allow_missing_currency,
        )
    if args.command == "backfill-issuer-fees":
        return run_issuer_fee_enrichment(args.db_path, args.source)
    if args.command in {"recommend", "recommend-strategies"}:
        return run_recommend_strategies(
            args.db_path,
            args.venue,
            args.preferred_currency_order,
            args.top_n,
            args.allow_missing_fees,
            args.allow_missing_currency,
            args.artifacts_dir,
        )
    raise SystemExit(f"Unknown command: {args.command}")
