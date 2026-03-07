from __future__ import annotations

import sqlite3

from etf_app.issuer_fee_enrich import (
    apply_fee_map,
    find_fee_after_isin,
    normalize_source_keys,
    select_missing_fee_targets,
    SUPPORTED_SOURCES,
)


def test_find_fee_after_isin_handles_ssga_cost_disclosure_text() -> None:
    text = (
        "SPDR MSCI Europe Consumer Discretionary UCITS ETF IE0005POVJH8 "
        "0.17 0.00 0.01 0.00 0.18 0.17 0.00 0.00"
    )

    fee, snippet = find_fee_after_isin(text, "IE0005POVJH8")

    assert fee == 0.17
    assert snippet is not None
    assert "IE0005POVJH8" in snippet


def test_find_fee_after_isin_handles_jpm_product_list_text() -> None:
    text = "JPM Europe Research Enhanced Index Equity Active UCITS ETF IE00004PGEY9 MSDEEMUN Article 8 0.25%"

    fee, _ = find_fee_after_isin(text, "IE00004PGEY9")

    assert fee == 0.25


def test_normalize_source_keys_defaults_to_all_supported_sources() -> None:
    assert normalize_source_keys([]) == [source.key for source in SUPPORTED_SOURCES]


def test_apply_fee_map_inserts_latest_fee_snapshot() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE stub_rows(
            instrument_id INTEGER,
            isin TEXT,
            instrument_name TEXT
        );
        CREATE TABLE cost_snapshot(
            cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER NOT NULL,
            asof_date TEXT NOT NULL,
            ongoing_charges REAL NULL,
            entry_costs REAL NULL,
            exit_costs REAL NULL,
            transaction_costs REAL NULL,
            doc_id INTEGER NULL,
            quality_flag TEXT NULL,
            raw_json TEXT NULL
        );
        INSERT INTO stub_rows(instrument_id, isin, instrument_name)
        VALUES (101, 'IE0005POVJH8', 'SPDR Example');
        """
    )
    rows = conn.execute("SELECT instrument_id, isin, instrument_name FROM stub_rows").fetchall()

    stats = apply_fee_map(
        conn,
        rows=rows,
        source=SUPPORTED_SOURCES[0],
        pdf_text="SPDR Example IE0005POVJH8 0.17 0.00 0.01",
        asof_date="2026-03-07",
    )

    saved = conn.execute("SELECT instrument_id, ongoing_charges, quality_flag FROM cost_snapshot").fetchone()
    assert stats == {"attempted": 1, "matched": 1, "inserted": 1}
    assert tuple(saved) == (101, 0.17, "ok")


def test_select_missing_fee_targets_filters_by_issuer() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE universe_mvp(
            instrument_id TEXT,
            isin TEXT,
            instrument_name TEXT,
            instrument_type TEXT,
            issuer_normalized TEXT
        );
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            issuer_id INTEGER
        );
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY,
            normalized_name TEXT
        );
        CREATE TABLE cost_snapshot(
            cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER NOT NULL,
            asof_date TEXT NOT NULL,
            ongoing_charges REAL NULL,
            entry_costs REAL NULL,
            exit_costs REAL NULL,
            transaction_costs REAL NULL,
            doc_id INTEGER NULL,
            quality_flag TEXT NULL,
            raw_json TEXT NULL
        );
        CREATE VIEW instrument_cost_current AS
        SELECT c.instrument_id, c.ongoing_charges
        FROM cost_snapshot c
        JOIN (
            SELECT instrument_id, MAX(cost_id) AS max_cost_id
            FROM cost_snapshot
            GROUP BY instrument_id
        ) latest ON latest.max_cost_id = c.cost_id;
        INSERT INTO issuer(issuer_id, normalized_name) VALUES
            (1, 'State Street / SPDR'),
            (2, 'JPMorgan');
        INSERT INTO instrument(instrument_id, issuer_id) VALUES
            (101, 1),
            (202, 2);
        INSERT INTO universe_mvp(instrument_id, isin, instrument_name, instrument_type, issuer_normalized) VALUES
            ('101', 'IE0005POVJH8', 'SPDR Example', 'ETF', 'State Street / SPDR'),
            ('202', 'IE00004PGEY9', 'JPM Example', 'ETF', 'JPMorgan');
        INSERT INTO cost_snapshot(instrument_id, asof_date, ongoing_charges, quality_flag, raw_json)
        VALUES (202, '2026-03-07', 0.25, 'ok', '{}');
        """
    )

    rows = select_missing_fee_targets(conn, ("State Street / SPDR",))

    assert [row["isin"] for row in rows] == ["IE0005POVJH8"]
