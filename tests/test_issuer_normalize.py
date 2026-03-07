from __future__ import annotations

import sqlite3

from etf_app.issuer_normalize import infer_issuer_from_name, infer_issuer_from_urls, normalize_unknown_issuers


def test_infer_issuer_from_name_handles_abbreviated_brands() -> None:
    assert infer_issuer_from_name("ISH.STOX.EUROPE 600 U.ETF").issuer_name == "BlackRock / iShares"
    assert infer_issuer_from_name("JPM-US EPI UE ADL").issuer_name == "JPMorgan"
    assert infer_issuer_from_name("AIS-AM.S+P500 SW.UETF EOC").issuer_name == "Amundi"
    assert infer_issuer_from_name("DEKA MSCI WORLD UCITS ETF").issuer_name == "Deka"


def test_infer_issuer_from_urls_prefers_official_domain_mapping() -> None:
    match = infer_issuer_from_urls("https://www.vaneck.com/globalassets/home/media/file.pdf")

    assert match is not None
    assert match.issuer_name == "VanEck"
    assert match.source == "issuer_normalize_domain"


def test_normalize_unknown_issuers_updates_missing_issuer_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE issuer(
            issuer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            issuer_name TEXT NOT NULL,
            website TEXT NULL,
            created_at TEXT NOT NULL,
            normalized_name TEXT NULL,
            domain TEXT NULL
        );
        CREATE TABLE instrument(
            instrument_id INTEGER PRIMARY KEY,
            isin TEXT NOT NULL,
            instrument_name TEXT NULL,
            issuer_id INTEGER NULL,
            issuer_source TEXT NULL,
            universe_mvp_flag INTEGER DEFAULT 1
        );
        CREATE TABLE universe_mvp(
            instrument_id TEXT,
            isin TEXT,
            instrument_name TEXT,
            issuer_normalized TEXT
        );
        CREATE TABLE document(
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            doc_type TEXT,
            url TEXT,
            retrieved_at TEXT,
            hash_sha256 TEXT,
            effective_date TEXT,
            language TEXT,
            parser_version TEXT
        );
        CREATE TABLE instrument_url_map(
            instrument_id INTEGER,
            url_type TEXT,
            url TEXT
        );
        INSERT INTO issuer(issuer_name, website, created_at, normalized_name, domain)
        VALUES ('BlackRock iShares', NULL, '2026-03-07T00:00:00Z', 'BlackRock / iShares', 'ishares.com');
        INSERT INTO instrument(instrument_id, isin, instrument_name, issuer_id, issuer_source, universe_mvp_flag) VALUES
            (101, 'DE0005933931', 'ISHS CORE DAX UC.ETF EOA', NULL, NULL, 1),
            (202, 'IE0005TF96I9', 'VANECK BIONIC ENGINEERING UCITS ETF', NULL, NULL, 1);
        INSERT INTO universe_mvp(instrument_id, isin, instrument_name, issuer_normalized) VALUES
            ('101', 'DE0005933931', 'ISHS CORE DAX UC.ETF EOA', NULL),
            ('202', 'IE0005TF96I9', 'VANECK BIONIC ENGINEERING UCITS ETF', NULL);
        INSERT INTO document(instrument_id, doc_type, url, retrieved_at, hash_sha256, effective_date, language, parser_version)
        VALUES
            (202, 'PRIIPS_KID', 'https://www.vaneck.com/globalassets/home/media/file.pdf', '2026-03-07T00:00:00Z', 'abc', NULL, NULL, 'v1');
        """
    )

    stats = normalize_unknown_issuers(conn)

    issuer_rows = conn.execute(
        "SELECT instrument_id, issuer_source, issuer_id FROM instrument ORDER BY instrument_id"
    ).fetchall()
    normalized_rows = conn.execute(
        "SELECT instrument_id, issuer_normalized FROM universe_mvp ORDER BY instrument_id"
    ).fetchall()

    assert stats["updated"] == 2
    assert [tuple(row) for row in issuer_rows] == [
        (101, "issuer_normalize_name", 1),
        (202, "issuer_normalize_domain", 2),
    ]
    assert [tuple(row) for row in normalized_rows] == [
        ("101", "BlackRock / iShares"),
        ("202", "VanEck"),
    ]
