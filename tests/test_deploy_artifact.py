from __future__ import annotations

import json

from etf_app.deploy_artifact import build_deploy_artifact, compute_sha256

from tests.test_deploy_db import make_source_db


def test_build_deploy_artifact_writes_gzip_manifest_and_smoke_stats(tmp_path) -> None:
    source_db = make_source_db(tmp_path)
    deploy_db = tmp_path / "deploy.sqlite"
    deploy_gzip = tmp_path / "deploy.sqlite.gz"
    manifest_path = tmp_path / "artifacts" / "deploy_manifest.json"
    completeness_path = tmp_path / "artifacts" / "completeness_report.json"
    completeness_path.parent.mkdir(parents=True, exist_ok=True)
    completeness_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-09T00:00:00Z",
                "product_profile": {
                    "fields": {
                        "ongoing_charges": {"known": 4, "total": 4, "pct": 100.0},
                        "fund_size_value": {"known": 4, "total": 4, "pct": 100.0},
                    }
                },
                "taxonomy": {
                    "equity": {
                        "size_known": {"known": 2, "total": 2, "pct": 100.0},
                        "style_known": {"known": 2, "total": 2, "pct": 100.0},
                    }
                },
                "strategy_readiness": {
                    "strict_hard_filters": {"kept": 3, "considered": 3}
                },
                "gap_summary": {
                    "rows_with_gaps": 1,
                    "missing_field_counts": {"benchmark_name": 1},
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    stats = build_deploy_artifact(
        source_db_path=source_db,
        deploy_db_path=str(deploy_db),
        deploy_gzip_path=str(deploy_gzip),
        manifest_path=str(manifest_path),
        completeness_report_path=str(completeness_path),
        version_label="deploy-2026-03-09",
        venue="ALL",
        preferred_currency_order="USD,EUR,GBP",
        top_n=5,
    )

    assert deploy_db.exists()
    assert deploy_gzip.exists()
    assert manifest_path.exists()
    assert stats.deploy_gzip.source_sha256 == compute_sha256(deploy_db)
    assert stats.deploy_gzip.gzip_sha256 == compute_sha256(deploy_gzip)
    assert stats.smoke_tests.strategy_gold_row_count == 1
    assert stats.smoke_tests.custom_row_count > 0

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["version_label"] == "deploy-2026-03-09"
    assert manifest["deploy_db"]["sha256"] == stats.deploy_gzip.source_sha256
    assert manifest["deploy_gzip"]["sha256"] == stats.deploy_gzip.gzip_sha256
    assert manifest["smoke_tests"]["strategy_gold_row_count"] == 1
    assert manifest["completeness_report"]["summary"]["strict_candidates_kept"] == 3
