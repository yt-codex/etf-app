from __future__ import annotations

from types import SimpleNamespace

import pytest

from etf_app import cli


@pytest.mark.parametrize(
    ("argv", "target_name", "expected_args"),
    [
        (
            ["rebuild-derived-data", "--db-path", "db.sqlite", "--artifacts-dir", "artifacts-out"],
            "run_patch_data",
            ("db.sqlite", "artifacts-out"),
        ),
        (
            ["patch-data", "--db-path", "db.sqlite", "--artifacts-dir", "artifacts-out"],
            "run_patch_data",
            ("db.sqlite", "artifacts-out"),
        ),
        (
            ["refresh-data", "--db-path", "db.sqlite", "--artifacts-dir", "artifacts-out", "--skip-cboe"],
            "run_stage1_refresh",
            ("db.sqlite", "artifacts-out", True),
        ),
        (
            ["stage1-refresh", "--db-path", "db.sqlite", "--artifacts-dir", "artifacts-out"],
            "run_stage1_refresh",
            ("db.sqlite", "artifacts-out", False),
        ),
        (
            ["build-taxonomy", "--db-path", "db.sqlite"],
            "run_classify_taxonomy",
            ("db.sqlite",),
        ),
        (
            ["classify-taxonomy", "--db-path", "db.sqlite"],
            "run_classify_taxonomy",
            ("db.sqlite",),
        ),
        (
            ["report-completeness", "--db-path", "db.sqlite"],
            "run_completeness_report",
            ("db.sqlite", "artifacts", "ALL", "USD,EUR,GBP", 5, False, False),
        ),
        (
            ["normalize-issuers", "--db-path", "db.sqlite", "--only-missing-fees"],
            "run_issuer_normalization",
            ("db.sqlite", True),
        ),
        (
            ["backfill-issuer-fees", "--db-path", "db.sqlite", "--source", "spdr", "--source", "vaneck"],
            "run_issuer_fee_enrichment",
            ("db.sqlite", ["spdr", "vaneck"]),
        ),
        (
            [
                "backfill-ft-metadata",
                "--db-path",
                "db.sqlite",
                "--limit",
                "25",
                "--venue",
                "XETR",
                "--sleep-seconds",
                "0.5",
                "--ticker",
                "NASD",
                "--ticker",
                "ANXU",
                "--isin",
                "LU1829221024",
                "--commit-every",
                "20",
            ],
            "run_ft_enrichment",
            ("db.sqlite", 25, "XETR", 0.5, ["NASD", "ANXU"], ["LU1829221024"], 20),
        ),
        (
            ["build-deploy-db", "--db-path", "db.sqlite", "--output-path", "deploy.sqlite"],
            "run_build_deploy_db",
            ("db.sqlite", "deploy.sqlite"),
        ),
        (
            [
                "refresh-deploy-artifact",
                "--db-path",
                "db.sqlite",
                "--artifacts-dir",
                "artifacts-out",
                "--deploy-db-path",
                "deploy.sqlite",
                "--deploy-gzip-path",
                "deploy.sqlite.gz",
                "--manifest-path",
                "artifacts-out\\deploy_manifest.json",
                "--version-label",
                "deploy-2026-03-09",
                "--skip-refresh",
                "--skip-cboe",
                "--skip-ft",
                "--ft-limit",
                "0",
                "--ft-venue",
                "XETR",
                "--ft-sleep-seconds",
                "0.25",
                "--ft-commit-every",
                "10",
                "--skip-issuer-fees",
                "--issuer-fee-source",
                "spdr",
                "--venue",
                "XLON",
                "--preferred-currency-order",
                "EUR,USD,GBP",
                "--top-n",
                "7",
            ],
            "run_refresh_deploy_artifact",
            (
                "db.sqlite",
                "artifacts-out",
                "deploy.sqlite",
                "deploy.sqlite.gz",
                "artifacts-out\\deploy_manifest.json",
                "deploy-2026-03-09",
                True,
                True,
                True,
                0,
                "XETR",
                0.25,
                10,
                True,
                ["spdr"],
                "XLON",
                "EUR,USD,GBP",
                7,
            ),
        ),
        (
            ["serve-api", "--db-path", "db.sqlite", "--host", "0.0.0.0", "--port", "9000", "--refresh-derived-on-start"],
            "run_api",
            ("db.sqlite", "0.0.0.0", 9000, True),
        ),
        (
            ["recommend", "--db-path", "db.sqlite"],
            "run_recommend_strategies",
            ("db.sqlite", "ALL", "USD,EUR,GBP", 5, False, False, "artifacts"),
        ),
        (
            ["recommend-strategies", "--db-path", "db.sqlite"],
            "run_recommend_strategies",
            ("db.sqlite", "ALL", "USD,EUR,GBP", 5, False, False, "artifacts"),
        ),
    ],
)
def test_main_dispatches_new_and_legacy_command_names(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    target_name: str,
    expected_args: tuple[object, ...],
) -> None:
    calls: list[tuple[object, ...]] = []

    def fake_runner(*args: object) -> int:
        calls.append(args)
        return 17

    monkeypatch.setattr(cli, target_name, fake_runner)

    assert cli.main(argv) == 17
    assert calls == [expected_args]


def test_run_patch_data_also_generates_completeness_report(monkeypatch: pytest.MonkeyPatch) -> None:
    hygiene_calls: list[list[str]] = []
    refine_calls: list[list[str]] = []
    completeness_calls: list[dict[str, object]] = []

    monkeypatch.setattr(cli.listing_hygiene, "main", lambda argv: hygiene_calls.append(argv))
    monkeypatch.setattr(cli.universe_refine, "main", lambda argv: refine_calls.append(argv))

    def fake_run_completeness_report(**kwargs: object) -> int:
        completeness_calls.append(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_completeness_report", fake_run_completeness_report)

    assert cli.run_patch_data("db.sqlite", "artifacts-out") == 0
    assert hygiene_calls == [["--db-path", "db.sqlite", "--output-csv", "artifacts-out\\primary_listings.csv"]]
    assert refine_calls == [["--db-path", "db.sqlite", "--output-csv", "artifacts-out\\universe_mvp.csv"]]
    assert completeness_calls == [
        {
            "db_path": "db.sqlite",
            "artifacts_dir": "artifacts-out",
            "venue": "ALL",
            "preferred_currency_order": "USD,EUR,GBP",
            "top_n": 5,
            "allow_missing_fees": False,
            "allow_missing_currency": False,
        }
    ]


def test_run_classify_taxonomy_also_generates_completeness_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "db.sqlite"
    completeness_calls: list[dict[str, object]] = []

    monkeypatch.setattr(cli, "ensure_taxonomy_schema", lambda conn: None)
    monkeypatch.setattr(cli, "load_universe_rows", lambda conn: [{"instrument_id": 1}])
    monkeypatch.setattr(cli, "upsert_taxonomy", lambda conn, rows: 1)
    monkeypatch.setattr(cli, "print_taxonomy_stats", lambda conn: None)

    def fake_run_completeness_report(**kwargs: object) -> int:
        completeness_calls.append(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_completeness_report", fake_run_completeness_report)

    assert cli.run_classify_taxonomy(str(db_path)) == 0
    assert completeness_calls == [
        {
            "db_path": str(db_path),
            "artifacts_dir": "artifacts",
            "venue": "ALL",
            "preferred_currency_order": "USD,EUR,GBP",
            "top_n": 5,
            "allow_missing_fees": False,
            "allow_missing_currency": False,
        }
    ]


def test_run_refresh_deploy_artifact_orchestrates_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[tuple[str, object]] = []

    def fake_stage1_refresh(db_path: str, artifacts_dir: str, skip_cboe: bool, *, emit_completeness: bool = True) -> int:
        calls.append(("refresh", (db_path, artifacts_dir, skip_cboe, emit_completeness)))
        return 0

    def fake_ft(*, db_path: str, limit: int, venue: str, sleep_seconds: float, tickers: list[str], isins: list[str], commit_every: int) -> int:
        calls.append(("ft", (db_path, limit, venue, sleep_seconds, tickers, isins, commit_every)))
        return 0

    def fake_fees(db_path: str, source: list[str]) -> int:
        calls.append(("fees", (db_path, source)))
        return 0

    def fake_completeness(**kwargs: object):
        calls.append(("completeness", kwargs))
        return tmp_path / "artifacts" / "completeness_report.json"

    def fake_build_artifact(**kwargs: object):
        calls.append(("artifact", kwargs))
        return SimpleNamespace(
            version_label="deploy-2026-03-09",
            deploy_db=SimpleNamespace(output_path="deploy.sqlite"),
            deploy_gzip=SimpleNamespace(
                source_sha256="db-sha",
                gzip_path="deploy.sqlite.gz",
                gzip_sha256="gz-sha",
            ),
            manifest_path="artifacts-out\\deploy_manifest.json",
            smoke_tests=SimpleNamespace(
                explorer_total=10,
                strategy_gold_row_count=1,
                custom_row_count=2,
                completeness_strict_candidates=3,
            ),
        )

    monkeypatch.setattr(cli, "run_stage1_refresh", fake_stage1_refresh)
    monkeypatch.setattr(cli, "run_ft_enrichment", fake_ft)
    monkeypatch.setattr(cli, "run_issuer_fee_enrichment", fake_fees)
    monkeypatch.setattr(cli, "generate_completeness_report", fake_completeness)
    monkeypatch.setattr(cli, "build_deploy_artifact", fake_build_artifact)

    assert (
        cli.run_refresh_deploy_artifact(
            "db.sqlite",
            "artifacts-out",
            "deploy.sqlite",
            "deploy.sqlite.gz",
            "artifacts-out\\deploy_manifest.json",
            "deploy-2026-03-09",
            False,
            True,
            False,
            0,
            "ALL",
            0.0,
            100,
            False,
            [],
            "ALL",
            "USD,EUR,GBP",
            5,
        )
        == 0
    )
    assert calls == [
        ("refresh", ("db.sqlite", "artifacts-out", True, False)),
        ("ft", ("db.sqlite", 0, "ALL", 0.0, [], [], 100)),
        ("fees", ("db.sqlite", [])),
        (
            "completeness",
            {
                "db_path": "db.sqlite",
                "artifacts_dir": "artifacts-out",
                "venue": "ALL",
                "preferred_currency_order": "USD,EUR,GBP",
                "top_n": 5,
                "allow_missing_fees": False,
                "allow_missing_currency": False,
            },
        ),
        (
            "artifact",
            {
                "source_db_path": "db.sqlite",
                "deploy_db_path": "deploy.sqlite",
                "deploy_gzip_path": "deploy.sqlite.gz",
                "manifest_path": "artifacts-out\\deploy_manifest.json",
                "completeness_report_path": str(tmp_path / "artifacts" / "completeness_report.json"),
                "version_label": "deploy-2026-03-09",
                "venue": "ALL",
                "preferred_currency_order": "USD,EUR,GBP",
                "top_n": 5,
            },
        ),
    ]
