from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import quote

import requests


DEFAULT_DB_FILENAME = "stage1_etf.db"
DEFAULT_CACHE_DIRNAME = "ucits-etf-atlas"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def _normalized_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _mapping_get(mapping: Optional[Mapping[str, Any]], key: str) -> Optional[str]:
    if mapping is None:
        return None
    try:
        value = mapping.get(key)
    except Exception:
        value = None
    return _normalized_text(value)


def _setting_value(
    key: str,
    *,
    secrets: Optional[Mapping[str, Any]] = None,
    env: Optional[Mapping[str, str]] = None,
    env_key: Optional[str] = None,
) -> Optional[str]:
    secret_value = _mapping_get(secrets, key)
    if secret_value is not None:
        return secret_value
    return _mapping_get(env, env_key or key.upper())


def _candidate_local_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _metadata_path(target_path: Path) -> Path:
    return target_path.with_suffix(f"{target_path.suffix}.meta.json")


def _load_metadata(metadata_path: Path) -> dict[str, str]:
    if not metadata_path.exists():
        return {}
    try:
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(key): str(value) for key, value in data.items() if value is not None}


def _write_metadata(metadata_path: Path, metadata: dict[str, str]) -> None:
    metadata_path.write_text(json.dumps(metadata, sort_keys=True, indent=2), encoding="utf-8")


def _cache_target_path(
    *,
    url: str,
    default_name: str,
    secrets: Optional[Mapping[str, Any]],
    env: Optional[Mapping[str, str]],
    cache_root: Optional[Path],
) -> Path:
    cache_name = _setting_value("db_cache_name", secrets=secrets, env=env, env_key="ETF_APP_DB_CACHE_NAME")
    cache_dir_value = _setting_value("db_cache_dir", secrets=secrets, env=env, env_key="ETF_APP_DB_CACHE_DIR")
    if cache_root is not None:
        root = cache_root
    elif cache_dir_value is not None:
        root = _candidate_local_path(cache_dir_value)
    else:
        root = Path(tempfile.gettempdir()) / DEFAULT_CACHE_DIRNAME
    target_name = cache_name or Path(url.split("?", 1)[0]).name or default_name
    return root / target_name


def _metadata_matches(
    metadata: dict[str, str],
    *,
    source_key: str,
    version: Optional[str],
    sha256: Optional[str],
) -> bool:
    if metadata.get("source_key") != source_key:
        return False
    if version is not None and metadata.get("version") != version:
        return False
    if sha256 is not None and metadata.get("sha256") != sha256.lower():
        return False
    return True


def _persist_download_stream(
    *,
    stream_response: requests.Response,
    target_path: Path,
    version: Optional[str],
    sha256: Optional[str],
    metadata: dict[str, str],
    source_label: str,
) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(f"{target_path.suffix}.download")
    metadata_path = _metadata_path(target_path)
    expected_sha = sha256.lower() if sha256 is not None else None
    digest = hashlib.sha256() if expected_sha is not None else None

    try:
        stream_response.raise_for_status()
        with temp_path.open("wb") as handle:
            for chunk in stream_response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    handle.write(chunk)
                    if digest is not None:
                        digest.update(chunk)
    except (requests.RequestException, ValueError) as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to download the ETF database from `{source_label}`: {exc}") from exc
    except OSError as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to write the downloaded ETF database to `{target_path}`: {exc}") from exc

    if expected_sha is not None and digest is not None:
        actual_sha = digest.hexdigest().lower()
        if actual_sha != expected_sha:
            temp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Downloaded ETF database SHA-256 mismatch for `{source_label}`: expected {expected_sha}, got {actual_sha}."
            )

    temp_path.replace(target_path)
    persisted_metadata = {**metadata}
    persisted_metadata["source_key"] = metadata["source_key"]
    if version is not None:
        persisted_metadata["version"] = version
    if sha256 is not None:
        persisted_metadata["sha256"] = sha256.lower()
    _write_metadata(metadata_path, persisted_metadata)
    return target_path


def _download_remote_db(
    *,
    url: str,
    target_path: Path,
    version: Optional[str],
    sha256: Optional[str],
) -> Path:
    try:
        with requests.get(
            url,
            stream=True,
            timeout=(20, 300),
            headers={"User-Agent": "UCITS-ETF-Atlas/1.0"},
        ) as response:
            return _persist_download_stream(
                stream_response=response,
                target_path=target_path,
                version=version,
                sha256=sha256,
                metadata={"source_key": url, "url": url},
                source_label=url,
            )
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download the ETF database from `{url}`: {exc}") from exc


def _b2_setting(
    key: str,
    *,
    secrets: Optional[Mapping[str, Any]],
    env: Optional[Mapping[str, str]],
) -> Optional[str]:
    secret_key = f"b2_{key}"
    secret_value = _mapping_get(secrets, secret_key)
    if secret_value is not None:
        return secret_value
    return _mapping_get(env, f"B2_{key.upper()}")


def _download_backblaze_private_db(
    *,
    key_id: str,
    application_key: str,
    bucket: str,
    file_name: str,
    target_path: Path,
    version: Optional[str],
    sha256: Optional[str],
) -> Path:
    auth_url = "https://api.backblazeb2.com/b2api/v3/b2_authorize_account"
    try:
        auth_response = requests.get(
            auth_url,
            auth=(key_id, application_key),
            timeout=(20, 60),
            headers={"User-Agent": "UCITS-ETF-Atlas/1.0"},
        )
        auth_response.raise_for_status()
        auth_payload = auth_response.json()
    except (requests.RequestException, ValueError) as exc:
        raise RuntimeError(f"Failed to authorize Backblaze B2 application key: {exc}") from exc

    download_base_url = _normalized_text(auth_payload.get("downloadUrl"))
    authorization_token = _normalized_text(auth_payload.get("authorizationToken"))
    if download_base_url is None or authorization_token is None:
        raise RuntimeError("Backblaze B2 authorize response did not include `downloadUrl` and `authorizationToken`.")

    quoted_bucket = quote(bucket, safe="")
    quoted_file_name = quote(file_name, safe="/")
    source_key = f"b2://{bucket}/{file_name}"
    download_url = f"{download_base_url}/file/{quoted_bucket}/{quoted_file_name}"
    try:
        with requests.get(
            download_url,
            stream=True,
            timeout=(20, 300),
            headers={
                "Authorization": authorization_token,
                "User-Agent": "UCITS-ETF-Atlas/1.0",
            },
        ) as response:
            return _persist_download_stream(
                stream_response=response,
                target_path=target_path,
                version=version,
                sha256=sha256,
                metadata={
                    "source_key": source_key,
                    "b2_bucket": bucket,
                    "b2_file_name": file_name,
                },
                source_label=source_key,
            )
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download the ETF database from `{source_key}`: {exc}") from exc


def resolve_db_path(
    *,
    default_path: str = DEFAULT_DB_FILENAME,
    secrets: Optional[Mapping[str, Any]] = None,
    env: Optional[Mapping[str, str]] = None,
    cache_root: Optional[Path] = None,
) -> Path:
    explicit_path = _setting_value("db_path", secrets=secrets, env=env, env_key="ETF_APP_DB_PATH")
    local_candidate = _candidate_local_path(explicit_path or default_path)
    if local_candidate.exists():
        return local_candidate

    db_version = _setting_value("db_version", secrets=secrets, env=env, env_key="ETF_APP_DB_VERSION")
    db_sha256 = _setting_value("db_sha256", secrets=secrets, env=env, env_key="ETF_APP_DB_SHA256")
    db_url = _setting_value("db_url", secrets=secrets, env=env, env_key="ETF_APP_DB_URL")
    if db_url is not None:
        target_path = _cache_target_path(
            url=db_url,
            default_name=Path(default_path).name,
            secrets=secrets,
            env=env,
            cache_root=cache_root,
        )
        metadata = _load_metadata(_metadata_path(target_path))
        if target_path.exists() and _metadata_matches(metadata, source_key=db_url, version=db_version, sha256=db_sha256):
            return target_path
        return _download_remote_db(url=db_url, target_path=target_path, version=db_version, sha256=db_sha256)

    b2_key_id = _b2_setting("key_id", secrets=secrets, env=env)
    b2_application_key = _b2_setting("application_key", secrets=secrets, env=env)
    b2_bucket = _b2_setting("bucket", secrets=secrets, env=env)
    b2_file_name = _b2_setting("file_name", secrets=secrets, env=env) or Path(default_path).name
    if b2_key_id and b2_application_key and b2_bucket:
        source_key = f"b2://{b2_bucket}/{b2_file_name}"
        target_path = _cache_target_path(
            url=source_key,
            default_name=Path(b2_file_name).name,
            secrets=secrets,
            env=env,
            cache_root=cache_root,
        )
        metadata = _load_metadata(_metadata_path(target_path))
        if target_path.exists() and _metadata_matches(metadata, source_key=source_key, version=db_version, sha256=db_sha256):
            return target_path
        return _download_backblaze_private_db(
            key_id=b2_key_id,
            application_key=b2_application_key,
            bucket=b2_bucket,
            file_name=b2_file_name,
            target_path=target_path,
            version=db_version,
            sha256=db_sha256,
        )

    raise FileNotFoundError(
        f"Database not found at `{local_candidate}`. "
        "Set `ETF_APP_DB_PATH`, configure `db_url` / `ETF_APP_DB_URL`, "
        "or provide Backblaze B2 settings (`b2_key_id`, `b2_application_key`, `b2_bucket`)."
    )
