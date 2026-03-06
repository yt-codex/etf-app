from __future__ import annotations

import json
from pathlib import Path

from etf_app.listing_ingest import IngestDB, SourceStats


def run_source_sync(
    db: IngestDB,
    *,
    source_name: str,
    asof_date: str,
    observations: list[dict[str, object]],
) -> tuple[int, SourceStats]:
    stats = SourceStats()
    run_id = db.start_source_run(source_name, asof_date)
    with db.conn:
        for observation in observations:
            instrument_id = db.upsert_instrument(
                isin=str(observation["isin"]),
                instrument_name=str(observation["instrument_name"]),
                ucits_flag=int(observation.get("ucits_flag", 1)),
                issuer_id=None,
                run_id=run_id,
                data_source=source_name,
                asof_date=asof_date,
                stats=stats,
            )
            db.upsert_listing(
                instrument_id=instrument_id,
                venue_mic=str(observation.get("venue_mic", "XLON")),
                exchange_name=str(observation.get("exchange_name", "Test Exchange")),
                ticker=str(observation["ticker"]),
                trading_currency=observation.get("trading_currency"),
                primary_flag=int(observation.get("primary_flag", 0)),
                data_source=source_name,
                asof_date=asof_date,
                run_id=run_id,
                stats=stats,
            )
            db.record_listing_observation(
                run_id=run_id,
                data_source=source_name,
                venue_mic=str(observation.get("venue_mic", "XLON")),
                instrument_id=instrument_id,
                ticker=str(observation["ticker"]),
            )
            stats.parsed_rows += 1

        reconcile_stats = db.reconcile_source_state(
            run_id=run_id,
            data_source=source_name,
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
    return run_id, stats


def make_db(tmp_path: Path) -> IngestDB:
    db = IngestDB(str(tmp_path / "stage1_test.db"))
    db.init_schema()
    return db


def test_reconcile_handles_insertion_disappearance_reactivation_with_audit(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        run1_id, run1_stats = run_source_sync(
            db,
            source_name="TEST_SOURCE",
            asof_date="2026-03-07",
            observations=[
                {
                    "isin": "IE00TEST0001",
                    "instrument_name": "TEST WORLD UCITS ETF",
                    "ticker": "TWLD",
                    "trading_currency": "USD",
                    "venue_mic": "XLON",
                }
            ],
        )
        instrument = db.conn.execute(
            "SELECT instrument_id, status FROM instrument WHERE isin = 'IE00TEST0001'"
        ).fetchone()
        listing = db.conn.execute(
            """
            SELECT listing_id, status
            FROM listing
            WHERE instrument_id = ? AND venue_mic = 'XLON' AND ticker = 'TWLD'
            """,
            (int(instrument["instrument_id"]),),
        ).fetchone()

        assert run1_stats.instrument_inserted == 1
        assert run1_stats.listing_inserted == 1
        assert instrument["status"] == "active"
        assert listing["status"] == "active"

        run2_id, run2_stats = run_source_sync(
            db,
            source_name="TEST_SOURCE",
            asof_date="2026-03-08",
            observations=[],
        )
        instrument = db.conn.execute(
            "SELECT instrument_id, status FROM instrument WHERE isin = 'IE00TEST0001'"
        ).fetchone()
        listing = db.conn.execute(
            """
            SELECT listing_id, status, asof_date
            FROM listing
            WHERE instrument_id = ? AND venue_mic = 'XLON' AND ticker = 'TWLD'
            """,
            (int(instrument["instrument_id"]),),
        ).fetchone()

        assert run2_stats.listing_deactivated == 1
        assert run2_stats.instrument_deactivated == 1
        assert instrument["status"] == "inactive"
        assert listing["status"] == "inactive"
        assert listing["asof_date"] == "2026-03-08"

        run3_id, run3_stats = run_source_sync(
            db,
            source_name="TEST_SOURCE",
            asof_date="2026-03-09",
            observations=[
                {
                    "isin": "IE00TEST0001",
                    "instrument_name": "TEST WORLD UCITS ETF",
                    "ticker": "TWLD",
                    "trading_currency": "USD",
                    "venue_mic": "XLON",
                }
            ],
        )
        instrument = db.conn.execute(
            "SELECT instrument_id, status FROM instrument WHERE isin = 'IE00TEST0001'"
        ).fetchone()
        listing = db.conn.execute(
            """
            SELECT listing_id, status
            FROM listing
            WHERE instrument_id = ? AND venue_mic = 'XLON' AND ticker = 'TWLD'
            """,
            (int(instrument["instrument_id"]),),
        ).fetchone()

        assert run3_stats.listing_reactivated == 1
        assert run3_stats.instrument_reactivated == 1
        assert instrument["status"] == "active"
        assert listing["status"] == "active"

        events = db.conn.execute(
            """
            SELECT run_id, entity_type, event_type, previous_status, new_status, venue_mic, ticker
            FROM lifecycle_event
            ORDER BY event_id
            """
        ).fetchall()
        event_tuples = [
            (
                int(row["run_id"]) if row["run_id"] is not None else None,
                row["entity_type"],
                row["event_type"],
                row["previous_status"],
                row["new_status"],
                row["venue_mic"],
                row["ticker"],
            )
            for row in events
        ]
        assert event_tuples == [
            (run1_id, "instrument", "inserted", None, "active", None, None),
            (run1_id, "listing", "inserted", None, "active", "XLON", "TWLD"),
            (run2_id, "listing", "deactivated", "active", "inactive", "XLON", "TWLD"),
            (run2_id, "instrument", "deactivated", "active", "inactive", None, None),
            (run3_id, "listing", "reactivated", "inactive", "active", "XLON", "TWLD"),
            (run3_id, "instrument", "reactivated", "inactive", "active", None, None),
        ]

        run2_note = db.conn.execute(
            "SELECT notes FROM source_run WHERE run_id = ?",
            (run2_id,),
        ).fetchone()["notes"]
        assert json.loads(str(run2_note)) == {
            "listing_deactivated": 1,
            "instrument_deactivated": 1,
            "instrument_reactivated": 0,
        }
    finally:
        db.close()


def test_reconcile_keeps_instrument_active_when_other_source_listing_remains(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        run_source_sync(
            db,
            source_name="SOURCE_A",
            asof_date="2026-03-07",
            observations=[
                {
                    "isin": "IE00TEST0002",
                    "instrument_name": "TEST EUROPE UCITS ETF",
                    "ticker": "TEUR",
                    "trading_currency": "EUR",
                    "venue_mic": "XLON",
                }
            ],
        )
        run_source_sync(
            db,
            source_name="SOURCE_B",
            asof_date="2026-03-07",
            observations=[
                {
                    "isin": "IE00TEST0002",
                    "instrument_name": "TEST EUROPE UCITS ETF",
                    "ticker": "TEUX",
                    "trading_currency": "EUR",
                    "venue_mic": "XETR",
                }
            ],
        )

        run3_id, run3_stats = run_source_sync(
            db,
            source_name="SOURCE_A",
            asof_date="2026-03-08",
            observations=[],
        )

        instrument = db.conn.execute(
            "SELECT instrument_id, status FROM instrument WHERE isin = 'IE00TEST0002'"
        ).fetchone()
        listings = db.conn.execute(
            """
            SELECT venue_mic, ticker, status
            FROM listing
            WHERE instrument_id = ?
            ORDER BY venue_mic, ticker
            """,
            (int(instrument["instrument_id"]),),
        ).fetchall()

        assert run3_stats.listing_deactivated == 1
        assert run3_stats.instrument_deactivated == 0
        assert instrument["status"] == "active"
        assert [(row["venue_mic"], row["ticker"], row["status"]) for row in listings] == [
            ("XETR", "TEUX", "active"),
            ("XLON", "TEUR", "inactive"),
        ]

        events = db.conn.execute(
            """
            SELECT entity_type, event_type, run_id
            FROM lifecycle_event
            WHERE run_id = ?
            ORDER BY event_id
            """,
            (run3_id,),
        ).fetchall()
        assert [(row["entity_type"], row["event_type"]) for row in events] == [
            ("listing", "deactivated")
        ]
    finally:
        db.close()
