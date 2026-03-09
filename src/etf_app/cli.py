from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from etf_app.api import run_api_server
from etf_app.deploy_artifact import build_deploy_artifact
from etf_app.deploy_db import build_deploy_db
from etf_app.ft_enrich import run_ft_metadata_backfill
from etf_app import listing_hygiene, listing_ingest, universe_refine
from etf_app.completeness import generate_completeness_report
from etf_app.issuer_fee_enrich import run_issuer_fee_backfill
from etf_app.issuer_normalize import normalize_unknown_issuers
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

    normalize_issuers = subparsers.add_parser(
        "normalize-issuers",
        help="Normalize missing ETF issuers using domain and instrument-name evidence",
    )
    normalize_issuers.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    normalize_issuers.add_argument(
        "--only-missing-fees",
        action="store_true",
        help="Only normalize issuer rows that still lack ongoing_charges",
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
        help="Optional source key filter; repeatable. Supported: spdr, jpmorgan, invesco, vaneck, bnpparibas",
    )

    ft_metadata = subparsers.add_parser(
        "backfill-ft-metadata",
        help="Backfill fund size, style, and sector hints from FT ETF tearsheets",
    )
    ft_metadata.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    ft_metadata.add_argument("--limit", type=int, default=100, help="Maximum instruments to attempt (0 means all matching targets)")
    ft_metadata.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Supported FT venue scope",
    )
    ft_metadata.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between resolved FT fetches",
    )
    ft_metadata.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Optional ticker filter; repeatable and matched case-insensitively",
    )
    ft_metadata.add_argument(
        "--isin",
        action="append",
        default=[],
        help="Optional ISIN filter; repeatable and matched case-insensitively",
    )
    ft_metadata.add_argument(
        "--commit-every",
        type=int,
        default=0,
        help="Flush product_profile/taxonomy and commit every N resolved FT snapshots",
    )

    deploy_db = subparsers.add_parser(
        "build-deploy-db",
        help="Export a slim SQLite database containing only the Streamlit runtime tables",
    )
    deploy_db.add_argument("--db-path", default="stage1_etf.db", help="Path to source SQLite DB")
    deploy_db.add_argument(
        "--output-path",
        default="deploy_stage1_etf.db",
        help="Path to output deploy SQLite DB",
    )

    refresh_deploy = subparsers.add_parser(
        "refresh-deploy-artifact",
        help="Run the refresh/enrichment pipeline and produce a slim deploy DB, .gz, and manifest",
    )
    refresh_deploy.add_argument("--db-path", default="stage1_etf.db", help="Path to source SQLite DB")
    refresh_deploy.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Directory for generated completeness and gap artifacts",
    )
    refresh_deploy.add_argument(
        "--deploy-db-path",
        default="deploy_stage1_etf.db",
        help="Path to output deploy SQLite DB",
    )
    refresh_deploy.add_argument(
        "--deploy-gzip-path",
        default="deploy_stage1_etf.db.gz",
        help="Path to output compressed deploy artifact",
    )
    refresh_deploy.add_argument(
        "--manifest-path",
        default="artifacts/deploy_manifest.json",
        help="Path to JSON manifest describing the deploy artifact",
    )
    refresh_deploy.add_argument(
        "--version-label",
        default="",
        help="Optional version label written into the manifest (defaults to the current UTC date)",
    )
    refresh_deploy.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip listing refresh and rebuild steps and operate on the existing source DB",
    )
    refresh_deploy.add_argument(
        "--skip-cboe",
        action="store_true",
        help="Skip optional Cboe ingestion during the refresh step",
    )
    refresh_deploy.add_argument(
        "--skip-ft",
        action="store_true",
        help="Skip the FT metadata backfill sweep",
    )
    refresh_deploy.add_argument(
        "--ft-limit",
        type=int,
        default=0,
        help="Maximum FT targets to attempt (0 means all matching targets)",
    )
    refresh_deploy.add_argument(
        "--ft-venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Primary venue scope for FT tearsheet resolution",
    )
    refresh_deploy.add_argument(
        "--ft-sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between resolved FT fetches",
    )
    refresh_deploy.add_argument(
        "--ft-commit-every",
        type=int,
        default=100,
        help="Flush FT metadata changes every N resolved snapshots",
    )
    refresh_deploy.add_argument(
        "--skip-issuer-fees",
        action="store_true",
        help="Skip issuer-fee backfills before the completeness and deploy steps",
    )
    refresh_deploy.add_argument(
        "--issuer-fee-source",
        action="append",
        default=[],
        help="Optional issuer-fee source filter; repeatable",
    )
    refresh_deploy.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Venue scope used for completeness and deploy smoke tests",
    )
    refresh_deploy.add_argument(
        "--preferred-currency-order",
        default="USD,EUR,GBP",
        help="Currency sort order for completeness and smoke tests",
    )
    refresh_deploy.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Top candidates per bucket for completeness and smoke tests",
    )

    serve_api = subparsers.add_parser(
        "serve-api",
        help="Serve a thin JSON API over the SQLite ETF database",
    )
    serve_api.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    serve_api.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    serve_api.add_argument("--port", type=int, default=8000, help="Port to listen on")
    serve_api.add_argument(
        "--refresh-derived-on-start",
        action="store_true",
        help="Refresh product_profile and taxonomy once before serving requests",
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


def run_patch_data(db_path: str, artifacts_dir: str, *, emit_completeness: bool = True) -> int:
    primary_csv = _artifact_path(artifacts_dir, "primary_listings.csv")
    universe_csv = _artifact_path(artifacts_dir, "universe_mvp.csv")

    listing_hygiene.main(["--db-path", db_path, "--output-csv", primary_csv])
    universe_refine.main(["--db-path", db_path, "--output-csv", universe_csv])
    if emit_completeness:
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


def run_stage1_refresh(
    db_path: str,
    artifacts_dir: str,
    skip_cboe: bool,
    *,
    emit_completeness: bool = True,
) -> int:
    ingest_args = ["--db-path", db_path]
    if skip_cboe:
        ingest_args.append("--skip-cboe")
    listing_ingest.main(ingest_args)
    return run_patch_data(db_path, artifacts_dir, emit_completeness=emit_completeness)


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


def run_ft_enrichment(
    db_path: str,
    limit: int,
    venue: str,
    sleep_seconds: float,
    tickers: list[str],
    isins: list[str],
    commit_every: int,
) -> int:
    stats = run_ft_metadata_backfill(
        db_path=db_path,
        limit=limit,
        venue=venue,
        sleep_seconds=sleep_seconds,
        tickers=tickers,
        isins=isins,
        commit_every=max(0, int(commit_every)),
    )
    print(
        f"ft metadata backfill: attempted={stats.attempted} resolved={stats.resolved} "
        f"failures_recorded={stats.failures_recorded} "
        f"summary_parsed={stats.summary_parsed} holdings_parsed={stats.holdings_parsed} "
        f"snapshots_inserted={stats.snapshots_inserted} "
        f"profile_rows_upserted={stats.profile_rows_upserted} "
        f"taxonomy_rows_updated={stats.taxonomy_rows_updated}"
    )
    return 0


def run_build_deploy_db(db_path: str, output_path: str) -> int:
    stats = build_deploy_db(source_db_path=db_path, output_db_path=output_path)
    print(
        "deploy db built: "
        f"instruments={stats.instrument_rows} "
        f"issuers={stats.issuer_rows} "
        f"listings={stats.listing_rows} "
        f"profiles={stats.product_profile_rows} "
        f"taxonomy={stats.instrument_taxonomy_rows} "
        f"current_cost_rows={stats.cost_snapshot_rows} "
        f"source_bytes={stats.source_size_bytes} "
        f"output_bytes={stats.output_size_bytes}"
    )
    return 0


def run_refresh_deploy_artifact(
    db_path: str,
    artifacts_dir: str,
    deploy_db_path: str,
    deploy_gzip_path: str,
    manifest_path: str,
    version_label: str,
    skip_refresh: bool,
    skip_cboe: bool,
    skip_ft: bool,
    ft_limit: int,
    ft_venue: str,
    ft_sleep_seconds: float,
    ft_commit_every: int,
    skip_issuer_fees: bool,
    issuer_fee_sources: list[str],
    venue: str,
    preferred_currency_order: str,
    top_n: int,
) -> int:
    if not skip_refresh:
        run_stage1_refresh(
            db_path,
            artifacts_dir,
            skip_cboe,
            emit_completeness=False,
        )
    else:
        print("refresh-deploy-artifact: skipping listing refresh")

    if not skip_ft:
        run_ft_enrichment(
            db_path=db_path,
            limit=ft_limit,
            venue=ft_venue,
            sleep_seconds=ft_sleep_seconds,
            tickers=[],
            isins=[],
            commit_every=ft_commit_every,
        )
    else:
        print("refresh-deploy-artifact: skipping FT metadata backfill")

    if not skip_issuer_fees:
        run_issuer_fee_enrichment(db_path=db_path, source=issuer_fee_sources)
    else:
        print("refresh-deploy-artifact: skipping issuer fee backfill")

    completeness_path = generate_completeness_report(
        db_path=db_path,
        artifacts_dir=artifacts_dir,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
        allow_missing_fees=False,
        allow_missing_currency=False,
    )
    stats = build_deploy_artifact(
        source_db_path=db_path,
        deploy_db_path=deploy_db_path,
        deploy_gzip_path=deploy_gzip_path,
        manifest_path=manifest_path,
        completeness_report_path=str(completeness_path),
        version_label=version_label,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
    )
    print(
        "deploy artifact ready: "
        f"version={stats.version_label} "
        f"deploy_db={stats.deploy_db.output_path} "
        f"deploy_db_sha256={stats.deploy_gzip.source_sha256} "
        f"deploy_gzip={stats.deploy_gzip.gzip_path} "
        f"deploy_gzip_sha256={stats.deploy_gzip.gzip_sha256} "
        f"manifest={stats.manifest_path}"
    )
    print(
        "deploy smoke tests: "
        f"explorer_total={stats.smoke_tests.explorer_total} "
        f"strategy_gold_rows={stats.smoke_tests.strategy_gold_row_count} "
        f"custom_rows={stats.smoke_tests.custom_row_count} "
        f"strict_candidates={stats.smoke_tests.completeness_strict_candidates}"
    )
    return 0


def run_issuer_normalization(db_path: str, only_missing_fees: bool) -> int:
    conn = sqlite3.connect(str(Path(db_path)))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        stats = normalize_unknown_issuers(conn, only_missing_fees=only_missing_fees)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(
        f"issuer normalization: candidates={stats['candidates']} updated={stats['updated']} "
        f"only_missing_fees={only_missing_fees}"
    )
    return 0


def run_api(
    db_path: str,
    host: str,
    port: int,
    refresh_derived_on_start: bool,
) -> int:
    return run_api_server(
        db_path=db_path,
        host=host,
        port=port,
        refresh_derived_on_start=refresh_derived_on_start,
    )


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
    if args.command == "normalize-issuers":
        return run_issuer_normalization(args.db_path, args.only_missing_fees)
    if args.command == "backfill-issuer-fees":
        return run_issuer_fee_enrichment(args.db_path, args.source)
    if args.command == "backfill-ft-metadata":
        return run_ft_enrichment(
            args.db_path,
            args.limit,
            args.venue,
            args.sleep_seconds,
            args.ticker,
            args.isin,
            args.commit_every,
        )
    if args.command == "build-deploy-db":
        return run_build_deploy_db(args.db_path, args.output_path)
    if args.command == "refresh-deploy-artifact":
        return run_refresh_deploy_artifact(
            args.db_path,
            args.artifacts_dir,
            args.deploy_db_path,
            args.deploy_gzip_path,
            args.manifest_path,
            args.version_label,
            args.skip_refresh,
            args.skip_cboe,
            args.skip_ft,
            args.ft_limit,
            args.ft_venue,
            args.ft_sleep_seconds,
            args.ft_commit_every,
            args.skip_issuer_fees,
            args.issuer_fee_source,
            args.venue,
            args.preferred_currency_order,
            args.top_n,
        )
    if args.command == "serve-api":
        return run_api(
            args.db_path,
            args.host,
            args.port,
            args.refresh_derived_on_start,
        )
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


if __name__ == "__main__":
    raise SystemExit(main())
