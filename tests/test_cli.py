from __future__ import annotations

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
            ["backfill-issuer-fees", "--db-path", "db.sqlite", "--source", "spdr", "--source", "jpmorgan"],
            "run_issuer_fee_enrichment",
            ("db.sqlite", ["spdr", "jpmorgan"]),
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
