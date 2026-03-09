from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from etf_app.db_bootstrap import resolve_db_path


class _FakeResponse:
    def __init__(self, payload: bytes, *, json_payload: dict | None = None) -> None:
        self._payload = payload
        self._json_payload = json_payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        for idx in range(0, len(self._payload), chunk_size):
            yield self._payload[idx : idx + chunk_size]

    def json(self) -> dict:
        if self._json_payload is None:
            raise ValueError("json payload not set")
        return self._json_payload


def test_resolve_db_path_prefers_existing_local_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    local_db = tmp_path / "local.db"
    local_db.write_bytes(b"local-db")

    def _unexpected_get(*args, **kwargs):
        raise AssertionError("remote download should not be attempted")

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _unexpected_get)

    resolved = resolve_db_path(
        secrets={"db_path": str(local_db), "db_url": "https://example.com/db.sqlite"},
        env={},
        cache_root=tmp_path / "cache",
    )

    assert resolved == local_db


def test_resolve_db_path_downloads_remote_db_and_writes_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"remote-db-contents"
    expected_sha = hashlib.sha256(payload).hexdigest()
    calls: list[str] = []

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        calls.append(url)
        return _FakeResponse(payload)

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _fake_get)

    resolved = resolve_db_path(
        default_path="missing-stage1_etf.db",
        secrets={
            "db_url": "https://example.com/releases/stage1_etf.db",
            "db_version": "2026-03-08",
            "db_sha256": expected_sha,
        },
        env={},
        cache_root=tmp_path / "cache",
    )

    assert calls == ["https://example.com/releases/stage1_etf.db"]
    assert resolved.read_bytes() == payload

    metadata_path = Path(f"{resolved}.meta.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata == {
        "sha256": expected_sha,
        "source_key": "https://example.com/releases/stage1_etf.db",
        "url": "https://example.com/releases/stage1_etf.db",
        "version": "2026-03-08",
    }


def test_resolve_db_path_reuses_cached_remote_db_when_metadata_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"remote-db-contents"
    expected_sha = hashlib.sha256(payload).hexdigest()
    call_count = 0

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        nonlocal call_count
        call_count += 1
        return _FakeResponse(payload)

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _fake_get)

    settings = {
        "db_url": "https://example.com/releases/stage1_etf.db",
        "db_version": "2026-03-08",
        "db_sha256": expected_sha,
    }
    first = resolve_db_path(default_path="missing-stage1_etf.db", secrets=settings, env={}, cache_root=tmp_path / "cache")
    second = resolve_db_path(default_path="missing-stage1_etf.db", secrets=settings, env={}, cache_root=tmp_path / "cache")

    assert first == second
    assert call_count == 1


def test_resolve_db_path_downloads_and_decompresses_gzip_remote_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_payload = b"decompressed-db-contents"
    compressed_payload = gzip.compress(raw_payload)
    expected_sha = hashlib.sha256(raw_payload).hexdigest()
    calls: list[str] = []

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        calls.append(url)
        return _FakeResponse(compressed_payload)

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _fake_get)

    resolved = resolve_db_path(
        default_path="missing-stage1_etf.db",
        secrets={
            "db_url": "https://example.com/releases/deploy_stage1_etf.db.gz",
            "db_version": "2026-03-09",
            "db_sha256": expected_sha,
        },
        env={},
        cache_root=tmp_path / "cache",
    )

    assert calls == ["https://example.com/releases/deploy_stage1_etf.db.gz"]
    assert resolved.name == "deploy_stage1_etf.db"
    assert resolved.read_bytes() == raw_payload

    metadata_path = Path(f"{resolved}.meta.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata == {
        "compression": "gzip",
        "sha256": expected_sha,
        "source_key": "https://example.com/releases/deploy_stage1_etf.db.gz",
        "url": "https://example.com/releases/deploy_stage1_etf.db.gz",
        "version": "2026-03-09",
    }


def test_resolve_db_path_raises_on_sha_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        return _FakeResponse(b"wrong-db")

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _fake_get)

    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        resolve_db_path(
            default_path="missing-stage1_etf.db",
            secrets={
                "db_url": "https://example.com/releases/stage1_etf.db",
                "db_sha256": "0" * 64,
            },
            env={},
            cache_root=tmp_path / "cache",
        )


def test_resolve_db_path_reuses_cached_decompressed_gzip_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_payload = b"decompressed-db-contents"
    compressed_payload = gzip.compress(raw_payload)
    expected_sha = hashlib.sha256(raw_payload).hexdigest()
    call_count = 0

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        nonlocal call_count
        call_count += 1
        return _FakeResponse(compressed_payload)

    monkeypatch.setattr("etf_app.db_bootstrap.requests.get", _fake_get)

    settings = {
        "db_url": "https://example.com/releases/deploy_stage1_etf.db.gz",
        "db_version": "2026-03-09",
        "db_sha256": expected_sha,
    }
    first = resolve_db_path(default_path="missing-stage1_etf.db", secrets=settings, env={}, cache_root=tmp_path / "cache")
    second = resolve_db_path(default_path="missing-stage1_etf.db", secrets=settings, env={}, cache_root=tmp_path / "cache")

    assert first == second
    assert first.name == "deploy_stage1_etf.db"
    assert call_count == 1


def test_resolve_db_path_raises_without_local_or_remote_settings(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Set `ETF_APP_DB_PATH` or configure `db_url` / `ETF_APP_DB_URL`"):
        resolve_db_path(
            default_path="missing-stage1_etf.db",
            secrets={},
            env={},
            cache_root=tmp_path / "cache",
        )
