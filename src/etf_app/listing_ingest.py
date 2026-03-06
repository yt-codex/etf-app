#!/usr/bin/env python3
"""
Stage 1 ETF ingestion:
- Builds a UCITS-capable ETF instrument universe keyed by ISIN
- Ingests listings from LSE, Xetra, and Cboe Europe
- Stores data into a local SQLite DB (default: stage1_etf.db)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from etf_app.profile import detect_ucits_flag

try:
    import xlrd
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "Missing dependency 'xlrd'. Install with: python -m pip install xlrd==2.0.1"
    ) from exc


LSE_XLS_URL = (
    "https://docs.londonstockexchange.com/sites/default/files/documents/"
    "list_of_etfs_and_etps_securities_162.xls"
)
XETRA_MAIN_URL = (
    "https://www.cashmarket.deutsche-boerse.com/cash-en/trading/"
    "Tradable-Instruments-Xetra"
)
XETRA_DOWNLOADS_URL = (
    "https://www.cashmarket.deutsche-boerse.com/cash-en/trading/"
    "Tradable-Instruments-Xetra/Downloads/xetra-downloads"
)
CBOE_SYMBOLS_URL = "https://www.cboe.com/europe/equities/listings/symbols/"

DATA_SOURCE_LSE = "LSE_ETF_ETP_XLS"
DATA_SOURCE_XETRA = "XETRA_ALL_TRADABLE"
DATA_SOURCE_CBOE = "CBOE_LISTED_SECURITIES"

NON_ISO_CURRENCY_CODES = {"GBX"}
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


@dataclass
class SourceStats:
    parsed_rows: int = 0
    skipped_rows: int = 0
    issuer_inserted: int = 0
    instrument_inserted: int = 0
    instrument_updated: int = 0
    listing_inserted: int = 0
    listing_updated: int = 0
    listing_reactivated: int = 0
    listing_deactivated: int = 0
    instrument_deactivated: int = 0
    instrument_reactivated: int = 0


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def normalize_header(header: object) -> str:
    text = normalize_text(header) or ""
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def normalize_isin(value: object) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    isin = re.sub(r"\s+", "", text).upper()
    if ISIN_RE.match(isin):
        return isin
    return None


def clean_ticker(value: object) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    ticker = re.sub(r"\s+", "", text)
    return ticker or None


def normalize_currency(value: object) -> Optional[str]:
    raw = normalize_text(value)
    if not raw:
        return None
    if raw == "GBp":
        return None
    code = re.sub(r"[^A-Za-z]", "", raw).upper()
    if len(code) != 3:
        return None
    if code in NON_ISO_CURRENCY_CODES:
        return None
    return code


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class HttpFetcher:
    def __init__(self, delay_seconds: float = 0.6, timeout_seconds: int = 60) -> None:
        self.delay_seconds = delay_seconds
        self.timeout_seconds = timeout_seconds
        self.last_request_ts = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def _throttle(self) -> None:
        elapsed = time.time() - self.last_request_ts
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

    def get_response(self, url: str, source_label: str) -> requests.Response:
        self._throttle()
        response = self.session.get(url, timeout=self.timeout_seconds)
        self.last_request_ts = time.time()
        content_len = len(response.content or b"")
        log(
            f"{source_label} fetch: {url} -> HTTP {response.status_code}, "
            f"{content_len} bytes"
        )
        response.raise_for_status()
        return response

    def get_bytes(self, url: str, source_label: str) -> bytes:
        return self.get_response(url, source_label).content

    def get_text(self, url: str, source_label: str) -> str:
        response = self.get_response(url, source_label)
        return response.text


class IngestDB:
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.issuer_cache: dict[str, int] = {}

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS issuer (
                issuer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                issuer_name TEXT NOT NULL UNIQUE,
                website TEXT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS instrument (
                instrument_id INTEGER PRIMARY KEY AUTOINCREMENT,
                isin TEXT NOT NULL UNIQUE,
                instrument_name TEXT NULL,
                ucits_flag INTEGER NULL CHECK (ucits_flag IN (0,1)),
                issuer_id INTEGER NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id)
            );

            CREATE TABLE IF NOT EXISTS listing (
                listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id INTEGER NOT NULL,
                venue_mic TEXT NOT NULL,
                exchange_name TEXT NULL,
                ticker TEXT NOT NULL,
                trading_currency TEXT NULL,
                primary_flag INTEGER NOT NULL DEFAULT 0 CHECK (primary_flag IN (0,1)),
                status TEXT NOT NULL DEFAULT 'active',
                data_source TEXT NOT NULL,
                asof_date TEXT NOT NULL,
                UNIQUE (instrument_id, venue_mic, ticker),
                FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
            );

            CREATE TABLE IF NOT EXISTS source_run (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                asof_date TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NULL,
                status TEXT NOT NULL,
                rows_observed INTEGER NOT NULL DEFAULT 0,
                notes TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS source_listing_snapshot (
                run_id INTEGER NOT NULL,
                data_source TEXT NOT NULL,
                venue_mic TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                PRIMARY KEY (run_id, data_source, venue_mic, instrument_id, ticker),
                FOREIGN KEY (run_id) REFERENCES source_run(run_id),
                FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
            );

            CREATE TABLE IF NOT EXISTS lifecycle_event (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NULL,
                entity_type TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                listing_id INTEGER NULL,
                venue_mic TEXT NULL,
                ticker TEXT NULL,
                data_source TEXT NULL,
                previous_status TEXT NULL,
                new_status TEXT NOT NULL,
                event_type TEXT NOT NULL,
                asof_date TEXT NULL,
                created_at TEXT NOT NULL,
                details_json TEXT NULL,
                FOREIGN KEY (run_id) REFERENCES source_run(run_id),
                FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id),
                FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
            );
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lifecycle_event_run_id ON lifecycle_event(run_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lifecycle_event_instrument ON lifecycle_event(instrument_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lifecycle_event_entity ON lifecycle_event(entity_type, event_type)"
        )
        self.conn.commit()

    def start_source_run(self, source_name: str, asof_date: str) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO source_run(source_name, asof_date, started_at, completed_at, status, rows_observed, notes)
            VALUES (?, ?, ?, NULL, 'running', 0, NULL)
            """,
            (source_name, asof_date, now_utc_iso()),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def record_listing_observation(
        self,
        *,
        run_id: int,
        data_source: str,
        venue_mic: str,
        instrument_id: int,
        ticker: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO source_listing_snapshot(
                run_id,
                data_source,
                venue_mic,
                instrument_id,
                ticker
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, data_source, venue_mic, instrument_id, ticker),
        )

    def record_lifecycle_event(
        self,
        *,
        run_id: Optional[int],
        entity_type: str,
        instrument_id: int,
        listing_id: Optional[int],
        venue_mic: Optional[str],
        ticker: Optional[str],
        data_source: Optional[str],
        previous_status: Optional[str],
        new_status: str,
        event_type: str,
        asof_date: Optional[str],
        details: Optional[dict[str, object]] = None,
    ) -> None:
        details_json = json.dumps(details, ensure_ascii=True) if details is not None else None
        self.conn.execute(
            """
            INSERT INTO lifecycle_event(
                run_id,
                entity_type,
                instrument_id,
                listing_id,
                venue_mic,
                ticker,
                data_source,
                previous_status,
                new_status,
                event_type,
                asof_date,
                created_at,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                entity_type,
                instrument_id,
                listing_id,
                venue_mic,
                ticker,
                data_source,
                previous_status,
                new_status,
                event_type,
                asof_date,
                now_utc_iso(),
                details_json,
            ),
        )

    def reconcile_source_state(self, *, run_id: int, data_source: str, asof_date: str) -> dict[str, int]:
        listing_rows_to_deactivate = self.conn.execute(
            """
            SELECT listing_id, instrument_id, venue_mic, ticker, status
            FROM listing
            WHERE data_source = ?
              AND COALESCE(status, 'active') <> 'inactive'
              AND NOT EXISTS (
                    SELECT 1
                    FROM source_listing_snapshot s
                    WHERE s.run_id = ?
                      AND s.data_source = listing.data_source
                      AND s.venue_mic = listing.venue_mic
                      AND s.instrument_id = listing.instrument_id
                      AND s.ticker = listing.ticker
              )
            """,
            (data_source, run_id),
        ).fetchall()
        if listing_rows_to_deactivate:
            self.conn.executemany(
                """
                UPDATE listing
                SET status = 'inactive',
                    asof_date = ?
                WHERE listing_id = ?
                """,
                [(asof_date, int(row["listing_id"])) for row in listing_rows_to_deactivate],
            )
            for row in listing_rows_to_deactivate:
                self.record_lifecycle_event(
                    run_id=run_id,
                    entity_type="listing",
                    instrument_id=int(row["instrument_id"]),
                    listing_id=int(row["listing_id"]),
                    venue_mic=row["venue_mic"],
                    ticker=row["ticker"],
                    data_source=data_source,
                    previous_status=row["status"],
                    new_status="inactive",
                    event_type="deactivated",
                    asof_date=asof_date,
                    details=None,
                )
        listing_deactivated = len(listing_rows_to_deactivate)

        ts = now_utc_iso()
        instrument_rows_to_deactivate = self.conn.execute(
            """
            SELECT instrument_id, status
            FROM instrument
            WHERE COALESCE(status, 'active') <> 'inactive'
              AND NOT EXISTS (
                    SELECT 1
                    FROM listing
                    WHERE listing.instrument_id = instrument.instrument_id
                      AND COALESCE(listing.status, 'active') = 'active'
              )
            """
        ).fetchall()
        if instrument_rows_to_deactivate:
            self.conn.executemany(
                """
                UPDATE instrument
                SET status = 'inactive',
                    updated_at = ?
                WHERE instrument_id = ?
                """,
                [(ts, int(row["instrument_id"])) for row in instrument_rows_to_deactivate],
            )
            for row in instrument_rows_to_deactivate:
                self.record_lifecycle_event(
                    run_id=run_id,
                    entity_type="instrument",
                    instrument_id=int(row["instrument_id"]),
                    listing_id=None,
                    venue_mic=None,
                    ticker=None,
                    data_source=data_source,
                    previous_status=row["status"],
                    new_status="inactive",
                    event_type="deactivated",
                    asof_date=asof_date,
                    details=None,
                )
        instrument_deactivated = len(instrument_rows_to_deactivate)
        instrument_rows_to_reactivate = self.conn.execute(
            """
            SELECT instrument_id, status
            FROM instrument
            WHERE COALESCE(status, 'active') <> 'active'
              AND EXISTS (
                    SELECT 1
                    FROM listing
                    WHERE listing.instrument_id = instrument.instrument_id
                      AND COALESCE(listing.status, 'active') = 'active'
              )
            """
        ).fetchall()
        if instrument_rows_to_reactivate:
            self.conn.executemany(
                """
                UPDATE instrument
                SET status = 'active',
                    updated_at = ?
                WHERE instrument_id = ?
                """,
                [(ts, int(row["instrument_id"])) for row in instrument_rows_to_reactivate],
            )
            for row in instrument_rows_to_reactivate:
                self.record_lifecycle_event(
                    run_id=run_id,
                    entity_type="instrument",
                    instrument_id=int(row["instrument_id"]),
                    listing_id=None,
                    venue_mic=None,
                    ticker=None,
                    data_source=data_source,
                    previous_status=row["status"],
                    new_status="active",
                    event_type="reactivated",
                    asof_date=asof_date,
                    details=None,
                )
        instrument_reactivated = len(instrument_rows_to_reactivate)
        return {
            "listing_deactivated": listing_deactivated,
            "instrument_deactivated": instrument_deactivated,
            "instrument_reactivated": instrument_reactivated,
        }

    def finish_source_run(
        self,
        *,
        run_id: int,
        status: str,
        stats: SourceStats,
        notes: Optional[dict[str, int] | str] = None,
    ) -> None:
        notes_payload: Optional[str]
        if isinstance(notes, dict):
            notes_payload = json.dumps(notes, ensure_ascii=True)
        else:
            notes_payload = notes
        self.conn.execute(
            """
            UPDATE source_run
            SET completed_at = ?,
                status = ?,
                rows_observed = ?,
                notes = ?
            WHERE run_id = ?
            """,
            (now_utc_iso(), status, stats.parsed_rows, notes_payload, run_id),
        )

    def upsert_issuer(self, issuer_name: Optional[str], stats: SourceStats) -> Optional[int]:
        if not issuer_name:
            return None
        name = normalize_text(issuer_name)
        if not name:
            return None
        cached = self.issuer_cache.get(name)
        if cached is not None:
            return cached

        row = self.conn.execute(
            "SELECT issuer_id FROM issuer WHERE issuer_name = ?",
            (name,),
        ).fetchone()
        if row:
            issuer_id = int(row["issuer_id"])
            self.issuer_cache[name] = issuer_id
            return issuer_id

        created_at = now_utc_iso()
        cur = self.conn.execute(
            "INSERT INTO issuer (issuer_name, website, created_at) VALUES (?, NULL, ?)",
            (name, created_at),
        )
        issuer_id = int(cur.lastrowid)
        self.issuer_cache[name] = issuer_id
        stats.issuer_inserted += 1
        return issuer_id

    def upsert_instrument(
        self,
        isin: str,
        instrument_name: Optional[str],
        ucits_flag: Optional[int],
        issuer_id: Optional[int],
        run_id: Optional[int],
        data_source: str,
        asof_date: str,
        stats: SourceStats,
    ) -> int:
        row = self.conn.execute(
            """
            SELECT instrument_id, instrument_name, ucits_flag, issuer_id, status
            FROM instrument
            WHERE isin = ?
            """,
            (isin,),
        ).fetchone()
        ts = now_utc_iso()

        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO instrument
                    (isin, instrument_name, ucits_flag, issuer_id, status, created_at, updated_at)
                VALUES
                    (?, ?, ?, ?, 'active', ?, ?)
                """,
                (isin, instrument_name, ucits_flag, issuer_id, ts, ts),
            )
            stats.instrument_inserted += 1
            instrument_id = int(cur.lastrowid)
            self.record_lifecycle_event(
                run_id=run_id,
                entity_type="instrument",
                instrument_id=instrument_id,
                listing_id=None,
                venue_mic=None,
                ticker=None,
                data_source=data_source,
                previous_status=None,
                new_status="active",
                event_type="inserted",
                asof_date=asof_date,
                details={"isin": isin, "instrument_name": instrument_name},
            )
            return instrument_id

        instrument_id = int(row["instrument_id"])
        current_name = row["instrument_name"]
        current_ucits = row["ucits_flag"]
        current_issuer = row["issuer_id"]
        current_status = row["status"]

        next_name = current_name or instrument_name
        next_ucits = current_ucits
        if next_ucits is None and ucits_flag is not None:
            next_ucits = ucits_flag
        elif next_ucits == 0 and ucits_flag == 1:
            next_ucits = 1
        next_issuer = current_issuer if current_issuer is not None else issuer_id

        changed = (
            next_name != current_name
            or next_ucits != current_ucits
            or next_issuer != current_issuer
        )
        if changed:
            self.conn.execute(
                """
                UPDATE instrument
                SET instrument_name = ?, ucits_flag = ?, issuer_id = ?, status = 'active', updated_at = ?
                WHERE instrument_id = ?
                """,
                (next_name, next_ucits, next_issuer, ts, instrument_id),
            )
            stats.instrument_updated += 1
            if current_status != "active":
                stats.instrument_reactivated += 1
                self.record_lifecycle_event(
                    run_id=run_id,
                    entity_type="instrument",
                    instrument_id=instrument_id,
                    listing_id=None,
                    venue_mic=None,
                    ticker=None,
                    data_source=data_source,
                    previous_status=current_status,
                    new_status="active",
                    event_type="reactivated",
                    asof_date=asof_date,
                    details={"isin": isin, "instrument_name": next_name},
                )
        return instrument_id

    def upsert_listing(
        self,
        instrument_id: int,
        venue_mic: str,
        exchange_name: Optional[str],
        ticker: str,
        trading_currency: Optional[str],
        primary_flag: int,
        data_source: str,
        asof_date: str,
        run_id: Optional[int],
        stats: SourceStats,
    ) -> None:
        row = self.conn.execute(
            """
            SELECT listing_id, exchange_name, trading_currency, primary_flag, status, data_source, asof_date
            FROM listing
            WHERE instrument_id = ? AND venue_mic = ? AND ticker = ?
            """,
            (instrument_id, venue_mic, ticker),
        ).fetchone()

        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO listing
                    (
                        instrument_id, venue_mic, exchange_name, ticker, trading_currency,
                        primary_flag, status, data_source, asof_date
                    )
                VALUES
                    (?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    instrument_id,
                    venue_mic,
                    exchange_name,
                    ticker,
                    trading_currency,
                    primary_flag,
                    data_source,
                    asof_date,
                ),
            )
            stats.listing_inserted += 1
            self.record_lifecycle_event(
                run_id=run_id,
                entity_type="listing",
                instrument_id=instrument_id,
                listing_id=int(cur.lastrowid),
                venue_mic=venue_mic,
                ticker=ticker,
                data_source=data_source,
                previous_status=None,
                new_status="active",
                event_type="inserted",
                asof_date=asof_date,
                details={
                    "exchange_name": exchange_name,
                    "trading_currency": trading_currency,
                    "primary_flag": primary_flag,
                },
            )
            return

        listing_id = int(row["listing_id"])
        next_exchange_name = exchange_name or row["exchange_name"]
        next_currency = trading_currency or row["trading_currency"]
        next_primary_flag = 1 if row["primary_flag"] == 1 or primary_flag == 1 else 0
        changed = (
            next_exchange_name != row["exchange_name"]
            or next_currency != row["trading_currency"]
            or next_primary_flag != row["primary_flag"]
            or row["status"] != "active"
            or row["data_source"] != data_source
            or row["asof_date"] != asof_date
        )
        if changed:
            self.conn.execute(
                """
                UPDATE listing
                SET exchange_name = ?,
                    trading_currency = ?,
                    primary_flag = ?,
                    status = 'active',
                    data_source = ?,
                    asof_date = ?
                WHERE listing_id = ?
                """,
                (
                    next_exchange_name,
                    next_currency,
                    next_primary_flag,
                    data_source,
                    asof_date,
                    listing_id,
                ),
            )
            stats.listing_updated += 1
            if row["status"] != "active":
                stats.listing_reactivated += 1
                self.record_lifecycle_event(
                    run_id=run_id,
                    entity_type="listing",
                    instrument_id=instrument_id,
                    listing_id=listing_id,
                    venue_mic=venue_mic,
                    ticker=ticker,
                    data_source=data_source,
                    previous_status=row["status"],
                    new_status="active",
                    event_type="reactivated",
                    asof_date=asof_date,
                    details={
                        "exchange_name": next_exchange_name,
                        "trading_currency": next_currency,
                        "primary_flag": next_primary_flag,
                    },
                )


def find_header_index(headers: list[str], candidates: list[str]) -> Optional[int]:
    normalized = {normalize_header(name): idx for idx, name in enumerate(headers)}
    for candidate in candidates:
        idx = normalized.get(normalize_header(candidate))
        if idx is not None:
            return idx
    return None


def ingest_lse(db: IngestDB, fetcher: HttpFetcher, asof_date: str) -> SourceStats:
    stats = SourceStats()
    run_id = db.start_source_run(DATA_SOURCE_LSE, asof_date)
    content = fetcher.get_bytes(LSE_XLS_URL, DATA_SOURCE_LSE)
    workbook = xlrd.open_workbook(file_contents=content)
    sheet = workbook.sheet_by_index(0)

    header_row_idx: Optional[int] = None
    headers: list[str] = []
    for row_idx in range(min(sheet.nrows, 50)):
        row_values = [normalize_text(sheet.cell_value(row_idx, c)) or "" for c in range(sheet.ncols)]
        if "ISIN" in row_values:
            header_row_idx = row_idx
            headers = row_values
            break

    if header_row_idx is None:
        raise RuntimeError("Unable to locate header row in LSE XLS")

    isin_idx = find_header_index(headers, ["ISIN"])
    ticker_idx = find_header_index(headers, ["Mnemonic", "Ticker", "Symbol"])
    ccy_idx = find_header_index(headers, ["Currency"])
    name_long_idx = find_header_index(headers, ["Long Name", "Instrument Name", "Security Name"])
    name_short_idx = find_header_index(headers, ["Short Name", "Name"])
    issuer_idx = find_header_index(headers, ["Issuer Name", "Issuer"])
    mic_idx = find_header_index(headers, ["MIC Code", "MIC"])

    if isin_idx is None or ticker_idx is None:
        raise RuntimeError("LSE XLS missing required ISIN or ticker columns")

    try:
        with db.conn:
            for row_idx in range(header_row_idx + 1, sheet.nrows):
                row = [sheet.cell_value(row_idx, c) for c in range(sheet.ncols)]
                isin = normalize_isin(row[isin_idx] if isin_idx < len(row) else None)
                if not isin:
                    continue

                ticker = clean_ticker(row[ticker_idx] if ticker_idx < len(row) else None)
                if not ticker:
                    stats.skipped_rows += 1
                    continue

                instrument_name = None
                if name_long_idx is not None and name_long_idx < len(row):
                    instrument_name = normalize_text(row[name_long_idx])
                if not instrument_name and name_short_idx is not None and name_short_idx < len(row):
                    instrument_name = normalize_text(row[name_short_idx])
                issuer_name = (
                    normalize_text(row[issuer_idx])
                    if issuer_idx is not None and issuer_idx < len(row)
                    else None
                )
                currency = (
                    normalize_currency(row[ccy_idx])
                    if ccy_idx is not None and ccy_idx < len(row)
                    else None
                )
                venue_mic = (
                    normalize_text(row[mic_idx]).upper()
                    if mic_idx is not None and mic_idx < len(row) and normalize_text(row[mic_idx])
                    else "XLON"
                )

                issuer_id = db.upsert_issuer(issuer_name, stats)
                ucits_flag = detect_ucits_flag(instrument_name)
                instrument_id = db.upsert_instrument(
                    isin=isin,
                    instrument_name=instrument_name,
                    ucits_flag=ucits_flag,
                    issuer_id=issuer_id,
                    run_id=run_id,
                    data_source=DATA_SOURCE_LSE,
                    asof_date=asof_date,
                    stats=stats,
                )
                db.upsert_listing(
                    instrument_id=instrument_id,
                    venue_mic=venue_mic,
                    exchange_name="London Stock Exchange",
                    ticker=ticker,
                    trading_currency=currency,
                    primary_flag=0,
                    data_source=DATA_SOURCE_LSE,
                    asof_date=asof_date,
                    run_id=run_id,
                    stats=stats,
                )
                db.record_listing_observation(
                    run_id=run_id,
                    data_source=DATA_SOURCE_LSE,
                    venue_mic=venue_mic,
                    instrument_id=instrument_id,
                    ticker=ticker,
                )
                stats.parsed_rows += 1

            reconcile_stats = db.reconcile_source_state(
                run_id=run_id,
                data_source=DATA_SOURCE_LSE,
                asof_date=asof_date,
            )
            stats.listing_deactivated = reconcile_stats["listing_deactivated"]
            stats.instrument_deactivated = reconcile_stats["instrument_deactivated"]
            stats.instrument_reactivated += reconcile_stats["instrument_reactivated"]
            db.finish_source_run(
                run_id=run_id,
                status="succeeded",
                stats=stats,
                notes=reconcile_stats,
            )
    except Exception as exc:
        with db.conn:
            db.finish_source_run(run_id=run_id, status="failed", stats=stats, notes=str(exc))
        raise

    log(
        f"{DATA_SOURCE_LSE} parsed={stats.parsed_rows} skipped={stats.skipped_rows} "
        f"issuer_inserted={stats.issuer_inserted} instrument_inserted={stats.instrument_inserted} "
        f"instrument_updated={stats.instrument_updated} listing_inserted={stats.listing_inserted} "
        f"listing_updated={stats.listing_updated} listing_reactivated={stats.listing_reactivated} "
        f"listing_deactivated={stats.listing_deactivated} "
        f"instrument_deactivated={stats.instrument_deactivated} "
        f"instrument_reactivated={stats.instrument_reactivated}"
    )
    return stats


def resolve_xetra_csv_url(fetcher: HttpFetcher) -> str:
    candidate_pages = [XETRA_MAIN_URL, XETRA_DOWNLOADS_URL]
    pattern = re.compile(r"/resource/blob/[^\"']*allTradableInstruments\.csv", re.IGNORECASE)

    for page_url in candidate_pages:
        html = fetcher.get_text(page_url, "XETRA_PAGE")
        soup = BeautifulSoup(html, "lxml")
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "")
            text = (anchor.get_text(" ", strip=True) or "").lower()
            if "alltradableinstruments.csv" in href.lower():
                return urljoin(page_url, href)
            if "all tradable instruments" in text and href.lower().endswith(".csv"):
                return urljoin(page_url, href)

        match = pattern.search(html)
        if match:
            return urljoin(page_url, match.group(0))

    raise RuntimeError("Unable to resolve Xetra allTradableInstruments CSV URL")


def ingest_xetra(db: IngestDB, fetcher: HttpFetcher, asof_date: str) -> SourceStats:
    stats = SourceStats()
    run_id = db.start_source_run(DATA_SOURCE_XETRA, asof_date)
    csv_url = resolve_xetra_csv_url(fetcher)
    csv_bytes = fetcher.get_bytes(csv_url, DATA_SOURCE_XETRA)
    try:
        csv_text = csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        csv_text = csv_bytes.decode("latin-1")

    lines = csv_text.splitlines()
    header_idx: Optional[int] = None
    for idx, line in enumerate(lines[:20]):
        if line.startswith("Product Status;"):
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError("Xetra CSV header not found")

    reader = csv.DictReader(lines[header_idx:], delimiter=";")
    try:
        with db.conn:
            for row in reader:
                if not row:
                    continue
                instrument_type = (normalize_text(row.get("Instrument Type")) or "").upper()
                if instrument_type != "ETF":
                    continue

                product_status = (normalize_text(row.get("Product Status")) or "").upper()
                instrument_status = (normalize_text(row.get("Instrument Status")) or "").upper()
                if product_status != "ACTIVE" or instrument_status != "ACTIVE":
                    continue

                mic = (normalize_text(row.get("MIC Code")) or "XETR").upper()
                if mic != "XETR":
                    continue

                isin = normalize_isin(row.get("ISIN"))
                ticker = clean_ticker(row.get("Mnemonic"))
                if not isin or not ticker:
                    stats.skipped_rows += 1
                    continue

                instrument_name = normalize_text(row.get("Instrument"))
                currency = normalize_currency(row.get("Currency"))
                ucits_flag = detect_ucits_flag(instrument_name)

                instrument_id = db.upsert_instrument(
                    isin=isin,
                    instrument_name=instrument_name,
                    ucits_flag=ucits_flag,
                    issuer_id=None,
                    run_id=run_id,
                    data_source=DATA_SOURCE_XETRA,
                    asof_date=asof_date,
                    stats=stats,
                )
                db.upsert_listing(
                    instrument_id=instrument_id,
                    venue_mic="XETR",
                    exchange_name="Deutsche Boerse Xetra",
                    ticker=ticker,
                    trading_currency=currency,
                    primary_flag=0,
                    data_source=DATA_SOURCE_XETRA,
                    asof_date=asof_date,
                    run_id=run_id,
                    stats=stats,
                )
                db.record_listing_observation(
                    run_id=run_id,
                    data_source=DATA_SOURCE_XETRA,
                    venue_mic="XETR",
                    instrument_id=instrument_id,
                    ticker=ticker,
                )
                stats.parsed_rows += 1

            reconcile_stats = db.reconcile_source_state(
                run_id=run_id,
                data_source=DATA_SOURCE_XETRA,
                asof_date=asof_date,
            )
            stats.listing_deactivated = reconcile_stats["listing_deactivated"]
            stats.instrument_deactivated = reconcile_stats["instrument_deactivated"]
            stats.instrument_reactivated += reconcile_stats["instrument_reactivated"]
            db.finish_source_run(
                run_id=run_id,
                status="succeeded",
                stats=stats,
                notes=reconcile_stats,
            )
    except Exception as exc:
        with db.conn:
            db.finish_source_run(run_id=run_id, status="failed", stats=stats, notes=str(exc))
        raise

    log(
        f"{DATA_SOURCE_XETRA} parsed={stats.parsed_rows} skipped={stats.skipped_rows} "
        f"issuer_inserted={stats.issuer_inserted} instrument_inserted={stats.instrument_inserted} "
        f"instrument_updated={stats.instrument_updated} listing_inserted={stats.listing_inserted} "
        f"listing_updated={stats.listing_updated} listing_reactivated={stats.listing_reactivated} "
        f"listing_deactivated={stats.listing_deactivated} "
        f"instrument_deactivated={stats.instrument_deactivated} "
        f"instrument_reactivated={stats.instrument_reactivated}"
    )
    return stats


def _extract_table_headers(table) -> list[str]:
    header_cells = table.find_all("th")
    return [normalize_text(th.get_text(" ", strip=True)) or "" for th in header_cells]


def ingest_cboe(db: IngestDB, fetcher: HttpFetcher, asof_date: str) -> SourceStats:
    stats = SourceStats()
    run_id = db.start_source_run(DATA_SOURCE_CBOE, asof_date)
    html = fetcher.get_text(CBOE_SYMBOLS_URL, DATA_SOURCE_CBOE)
    soup = BeautifulSoup(html, "lxml")

    tables = soup.find_all("table")
    try:
        with db.conn:
            for table in tables:
                headers = _extract_table_headers(table)
                if not headers:
                    continue

                isin_idx = find_header_index(headers, ["ISIN"])
                ticker_idx = find_header_index(headers, ["Symbol", "Ticker"])
                ccy_idx = find_header_index(headers, ["Currency"])
                name_idx = find_header_index(headers, ["Name", "Stock", "Instrument Name"])
                if isin_idx is None or ticker_idx is None:
                    continue

                for tr in table.find_all("tr"):
                    cells = tr.find_all("td")
                    if not cells:
                        continue
                    values = [normalize_text(td.get_text(" ", strip=True)) or "" for td in cells]
                    max_required = max(isin_idx, ticker_idx, name_idx or 0, ccy_idx or 0)
                    if len(values) <= max_required:
                        continue

                    isin = normalize_isin(values[isin_idx])
                    ticker = clean_ticker(values[ticker_idx])
                    if not isin or not ticker:
                        stats.skipped_rows += 1
                        continue

                    instrument_name = values[name_idx] if name_idx is not None else None
                    currency = values[ccy_idx] if ccy_idx is not None else None
                    normalized_currency = normalize_currency(currency)
                    ucits_flag = detect_ucits_flag(instrument_name)

                    instrument_id = db.upsert_instrument(
                        isin=isin,
                        instrument_name=instrument_name,
                        ucits_flag=ucits_flag,
                        issuer_id=None,
                        run_id=run_id,
                        data_source=DATA_SOURCE_CBOE,
                        asof_date=asof_date,
                        stats=stats,
                    )
                    db.upsert_listing(
                        instrument_id=instrument_id,
                        venue_mic="CBOE_EU",
                        exchange_name="Cboe Europe",
                        ticker=ticker,
                        trading_currency=normalized_currency,
                        primary_flag=0,
                        data_source=DATA_SOURCE_CBOE,
                        asof_date=asof_date,
                        run_id=run_id,
                        stats=stats,
                    )
                    db.record_listing_observation(
                        run_id=run_id,
                        data_source=DATA_SOURCE_CBOE,
                        venue_mic="CBOE_EU",
                        instrument_id=instrument_id,
                        ticker=ticker,
                    )
                    stats.parsed_rows += 1

            reconcile_stats = db.reconcile_source_state(
                run_id=run_id,
                data_source=DATA_SOURCE_CBOE,
                asof_date=asof_date,
            )
            stats.listing_deactivated = reconcile_stats["listing_deactivated"]
            stats.instrument_deactivated = reconcile_stats["instrument_deactivated"]
            stats.instrument_reactivated += reconcile_stats["instrument_reactivated"]
            db.finish_source_run(
                run_id=run_id,
                status="succeeded",
                stats=stats,
                notes=reconcile_stats,
            )
    except Exception as exc:
        with db.conn:
            db.finish_source_run(run_id=run_id, status="failed", stats=stats, notes=str(exc))
        raise

    log(
        f"{DATA_SOURCE_CBOE} parsed={stats.parsed_rows} skipped={stats.skipped_rows} "
        f"issuer_inserted={stats.issuer_inserted} instrument_inserted={stats.instrument_inserted} "
        f"instrument_updated={stats.instrument_updated} listing_inserted={stats.listing_inserted} "
        f"listing_updated={stats.listing_updated} listing_reactivated={stats.listing_reactivated} "
        f"listing_deactivated={stats.listing_deactivated} "
        f"instrument_deactivated={stats.instrument_deactivated} "
        f"instrument_reactivated={stats.instrument_reactivated}"
    )
    return stats


def print_acceptance_outputs(conn: sqlite3.Connection) -> None:
    total_instruments = conn.execute("SELECT COUNT(*) AS c FROM instrument").fetchone()["c"]
    print("\n=== Summary Counts ===")
    print(f"# instruments total: {total_instruments}")

    print("# listings by venue_mic:")
    rows = conn.execute(
        """
        SELECT venue_mic, COUNT(*) AS c
        FROM listing
        GROUP BY venue_mic
        ORDER BY c DESC, venue_mic
        """
    ).fetchall()
    for row in rows:
        print(f"  {row['venue_mic']}: {row['c']}")

    multi_listed = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM (
            SELECT instrument_id
            FROM listing
            GROUP BY instrument_id
            HAVING COUNT(*) >= 2
        ) t
        """
    ).fetchone()["c"]
    print(f"# instruments with >=2 listings: {multi_listed}")

    print("\n=== First 20 Instruments With Listings ===")
    instruments = conn.execute(
        """
        SELECT instrument_id, isin, instrument_name
        FROM instrument
        ORDER BY isin
        LIMIT 20
        """
    ).fetchall()
    for inst in instruments:
        print(
            f"{inst['isin']} | {inst['instrument_name'] or ''}"
        )
        listings = conn.execute(
            """
            SELECT venue_mic, ticker, trading_currency
            FROM listing
            WHERE instrument_id = ?
            ORDER BY venue_mic, ticker
            """,
            (inst["instrument_id"],),
        ).fetchall()
        for listing in listings:
            print(
                f"  - {listing['venue_mic']} | {listing['ticker']} | "
                f"{listing['trading_currency'] or 'NULL'}"
            )

    print("\n=== Dedupe Checks ===")
    dup_isin_count = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM (
            SELECT isin
            FROM instrument
            GROUP BY isin
            HAVING COUNT(*) > 1
        ) d
        """
    ).fetchone()["c"]
    print(f"duplicate ISIN groups in instrument: {dup_isin_count}")

    dup_listing_count = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM (
            SELECT instrument_id, venue_mic, ticker
            FROM listing
            GROUP BY instrument_id, venue_mic, ticker
            HAVING COUNT(*) > 1
        ) d
        """
    ).fetchone()["c"]
    print(f"duplicate listing key groups: {dup_listing_count}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 1 ETF ingestion to SQLite.")
    parser.add_argument("--db-path", default="stage1_etf.db", help="SQLite DB path")
    parser.add_argument(
        "--skip-cboe",
        action="store_true",
        help="Skip optional Cboe ingestion",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    asof_date = dt.date.today().isoformat()

    db = IngestDB(args.db_path)
    fetcher = HttpFetcher(delay_seconds=0.6, timeout_seconds=60)
    try:
        db.init_schema()
        log(f"Initialized DB schema at {args.db_path}")

        ingest_lse(db, fetcher, asof_date)
        ingest_xetra(db, fetcher, asof_date)
        if not args.skip_cboe:
            ingest_cboe(db, fetcher, asof_date)
        else:
            log("Skipping Cboe ingestion by flag")

        print_acceptance_outputs(db.conn)
        db.conn.commit()
        log(f"Completed ingestion. DB persisted to {args.db_path}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
