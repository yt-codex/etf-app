from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

from etf_app.api import (
    get_completeness_snapshot,
    get_custom_strategy_snapshot,
    get_strategy_snapshot,
    list_filter_options,
    list_funds,
    open_read_conn,
)
from etf_app.deploy_db import DeployDbStats, build_deploy_db


DEFAULT_SMOKE_STRATEGY_NAME = "Harry Browne Permanent Portfolio"
DEFAULT_SMOKE_BUCKETS = (
    {"bucket_name": "equity_global", "target_weight": 60.0},
    {"bucket_name": "short_bonds", "target_weight": 40.0},
)


@dataclass(frozen=True)
class DeployGzipStats:
    source_path: str
    gzip_path: str
    source_size_bytes: int
    gzip_size_bytes: int
    source_sha256: str
    gzip_sha256: str


@dataclass(frozen=True)
class DeploySmokeTestStats:
    explorer_total: int
    explorer_page_items: int
    filter_asset_class_options: int
    strategy_name: str
    strategy_row_count: int
    strategy_gold_row_count: int
    custom_bucket_count: int
    custom_row_count: int
    completeness_rows_with_gaps: int
    completeness_strict_candidates: int


@dataclass(frozen=True)
class DeployArtifactStats:
    version_label: str
    deploy_db: DeployDbStats
    deploy_gzip: DeployGzipStats
    smoke_tests: DeploySmokeTestStats
    manifest_path: str


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gzip_deploy_db(*, source_path: str, gzip_path: str, compresslevel: int = 9) -> DeployGzipStats:
    source = Path(source_path)
    target = Path(gzip_path)
    if not source.exists():
        raise FileNotFoundError(f"Deploy DB not found: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    with source.open("rb") as src, gzip.open(target, "wb", compresslevel=compresslevel) as dst:
        shutil.copyfileobj(src, dst)
    return DeployGzipStats(
        source_path=str(source),
        gzip_path=str(target),
        source_size_bytes=source.stat().st_size,
        gzip_size_bytes=target.stat().st_size,
        source_sha256=compute_sha256(source),
        gzip_sha256=compute_sha256(target),
    )


def smoke_test_deploy_db(
    *,
    db_path: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
) -> DeploySmokeTestStats:
    conn = open_read_conn(db_path)
    try:
        explorer = list_funds(
            conn,
            params={"limit": "10", "offset": "0", "sort": "name", "direction": "asc"},
        )
        if int(explorer["total"]) <= 0:
            raise ValueError("deploy smoke test failed: explorer returned no rows")
        filters = list_filter_options(conn)
        asset_class_options = len(filters.get("asset_class", []))
        if asset_class_options <= 0:
            raise ValueError("deploy smoke test failed: filter options are empty")

        strategy_snapshot = get_strategy_snapshot(
            conn,
            venue=venue,
            preferred_currency_order=preferred_currency_order,
            top_n=top_n,
            allow_missing_fees=False,
            allow_missing_currency=False,
            strategy_name=DEFAULT_SMOKE_STRATEGY_NAME,
        )
        strategy_rows = strategy_snapshot["strategies"][0]["rows"]
        gold_rows = [row for row in strategy_rows if row["bucket_name"] == "gold"]
        if not gold_rows:
            raise ValueError("deploy smoke test failed: named strategy gold bucket returned no candidates")

        custom_snapshot = get_custom_strategy_snapshot(
            conn,
            venue=venue,
            preferred_currency_order=preferred_currency_order,
            top_n=top_n,
            allow_missing_fees=False,
            allow_missing_currency=False,
            buckets=list(DEFAULT_SMOKE_BUCKETS),
        )
        custom_rows = custom_snapshot["strategies"][0]["rows"]
        if not custom_rows:
            raise ValueError("deploy smoke test failed: custom allocation returned no candidates")

        completeness = get_completeness_snapshot(
            conn,
            db_path=db_path,
            venue=venue,
            preferred_currency_order=preferred_currency_order,
            top_n=top_n,
            allow_missing_fees=False,
            allow_missing_currency=False,
        )
        strict_candidates = int(completeness["strategy_readiness"]["strict_hard_filters"]["kept"])
        if strict_candidates <= 0:
            raise ValueError("deploy smoke test failed: completeness strict candidate count is zero")
        return DeploySmokeTestStats(
            explorer_total=int(explorer["total"]),
            explorer_page_items=len(explorer["items"]),
            filter_asset_class_options=asset_class_options,
            strategy_name=DEFAULT_SMOKE_STRATEGY_NAME,
            strategy_row_count=len(strategy_rows),
            strategy_gold_row_count=len(gold_rows),
            custom_bucket_count=len(DEFAULT_SMOKE_BUCKETS),
            custom_row_count=len(custom_rows),
            completeness_rows_with_gaps=int(completeness["gap_summary"]["rows_with_gaps"]),
            completeness_strict_candidates=strict_candidates,
        )
    finally:
        conn.close()


def _safe_pct(numerator: int, denominator: int) -> float:
    return round((100.0 * numerator / denominator), 4) if denominator else 0.0


def _load_completeness_summary(report_path: Path) -> dict[str, object]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    profile_fields = payload.get("product_profile", {}).get("fields", {})
    taxonomy_equity = payload.get("taxonomy", {}).get("equity", {})
    strict_filters = payload.get("strategy_readiness", {}).get("strict_hard_filters", {})
    gap_summary = payload.get("gap_summary", {})
    return {
        "generated_at": payload.get("generated_at"),
        "gap_rows": gap_summary.get("rows_with_gaps"),
        "missing_field_counts": gap_summary.get("missing_field_counts", {}),
        "profile_ongoing_charges_pct": profile_fields.get("ongoing_charges", {}).get("pct"),
        "profile_fund_size_pct": profile_fields.get("fund_size_value", {}).get("pct"),
        "taxonomy_equity_size_pct": taxonomy_equity.get("size_known", {}).get("pct"),
        "taxonomy_equity_style_pct": taxonomy_equity.get("style_known", {}).get("pct"),
        "strict_candidates_kept": strict_filters.get("kept"),
        "strict_candidates_considered": strict_filters.get("considered"),
    }


def write_deploy_manifest(
    *,
    manifest_path: str,
    version_label: str,
    deploy_db: DeployDbStats,
    deploy_gzip: DeployGzipStats,
    completeness_report_path: str,
    smoke_tests: DeploySmokeTestStats,
) -> Path:
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    completeness_path = Path(completeness_report_path)
    payload = {
        "generated_at": now_utc_iso(),
        "version_label": version_label,
        "source_db": {
            "path": deploy_db.source_path,
            "size_bytes": deploy_db.source_size_bytes,
        },
        "deploy_db": {
            "path": deploy_db.output_path,
            "size_bytes": deploy_db.output_size_bytes,
            "sha256": deploy_gzip.source_sha256,
            "table_rows": {
                "instrument": deploy_db.instrument_rows,
                "issuer": deploy_db.issuer_rows,
                "listing": deploy_db.listing_rows,
                "product_profile": deploy_db.product_profile_rows,
                "instrument_taxonomy": deploy_db.instrument_taxonomy_rows,
                "cost_snapshot": deploy_db.cost_snapshot_rows,
            },
        },
        "deploy_gzip": {
            "path": deploy_gzip.gzip_path,
            "size_bytes": deploy_gzip.gzip_size_bytes,
            "sha256": deploy_gzip.gzip_sha256,
        },
        "size_reduction": {
            "deploy_db_pct_of_source": _safe_pct(deploy_db.output_size_bytes, deploy_db.source_size_bytes),
            "deploy_gzip_pct_of_source": _safe_pct(deploy_gzip.gzip_size_bytes, deploy_db.source_size_bytes),
        },
        "completeness_report": {
            "path": str(completeness_path),
            "summary": _load_completeness_summary(completeness_path),
        },
        "smoke_tests": asdict(smoke_tests),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return path


def build_deploy_artifact(
    *,
    source_db_path: str,
    deploy_db_path: str,
    deploy_gzip_path: str,
    manifest_path: str,
    completeness_report_path: str,
    version_label: str,
    venue: str,
    preferred_currency_order: str,
    top_n: int,
) -> DeployArtifactStats:
    resolved_version = version_label.strip() or dt.date.today().isoformat()
    deploy_db = build_deploy_db(source_db_path=source_db_path, output_db_path=deploy_db_path)
    smoke_tests = smoke_test_deploy_db(
        db_path=deploy_db.output_path,
        venue=venue,
        preferred_currency_order=preferred_currency_order,
        top_n=top_n,
    )
    deploy_gzip = gzip_deploy_db(source_path=deploy_db.output_path, gzip_path=deploy_gzip_path)
    manifest = write_deploy_manifest(
        manifest_path=manifest_path,
        version_label=resolved_version,
        deploy_db=deploy_db,
        deploy_gzip=deploy_gzip,
        completeness_report_path=completeness_report_path,
        smoke_tests=smoke_tests,
    )
    return DeployArtifactStats(
        version_label=resolved_version,
        deploy_db=deploy_db,
        deploy_gzip=deploy_gzip,
        smoke_tests=smoke_tests,
        manifest_path=str(manifest),
    )
