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
