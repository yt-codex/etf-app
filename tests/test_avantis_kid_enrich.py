from __future__ import annotations

import sqlite3

from etf_app.avantis_kid_enrich import (
    backfill_listing_aliases_from_fund_page,
    extract_listing_rows_from_fund_page,
)


FUND_PAGE_HTML = """
nodeType:"table-row",data:{},content:[
{nodeType:"table-header-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"Exchange",marks:[],data:{}}]}]},
{nodeType:"table-header-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"Ticker",marks:[],data:{}}]}]},
{nodeType:"table-header-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"Currency",marks:[],data:{}}]}]}
]},
{nodeType:"table-row",data:{},content:[
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"Xetra",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"AVWS",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"EUR",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"BR3Y7D9",marks:[],data:{}}]}]}
]},
{nodeType:"table-row",data:{},content:[
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"London Stock Exchange",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"AVGS",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"USD",marks:[],data:{}}]}]},
{nodeType:"table-cell",data:{},content:[{nodeType:"paragraph",data:{},content:[{nodeType:"text",value:"BT21VD3",marks:[],data:{}}]}]}
]}
"""


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE listing(
            listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER NOT NULL,
            venue_mic TEXT NOT NULL,
            exchange_name TEXT NULL,
            ticker TEXT NOT NULL,
            trading_currency TEXT NULL,
            primary_flag INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            data_source TEXT NULL,
            asof_date TEXT NULL
        )
        """
    )
    return conn


def test_extract_listing_rows_from_fund_page_parses_supported_exchange_rows() -> None:
    rows = sorted(
        extract_listing_rows_from_fund_page(FUND_PAGE_HTML),
        key=lambda row: (row["venue_mic"], row["ticker"]),
    )

    assert rows == [
        {
            "venue_mic": "XETR",
            "exchange_name": "Deutsche Boerse Xetra",
            "ticker": "AVWS",
            "trading_currency": "EUR",
        },
        {
            "venue_mic": "XLON",
            "exchange_name": "London Stock Exchange",
            "ticker": "AVGS",
            "trading_currency": "USD",
        },
    ]


def test_backfill_listing_aliases_from_fund_page_inserts_non_primary_aliases() -> None:
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO listing(
            instrument_id, venue_mic, exchange_name, ticker, trading_currency,
            primary_flag, status, data_source, asof_date
        ) VALUES (1, 'XETR', 'Deutsche Boerse Xetra', 'AVWS', 'EUR', 1, 'active', 'XETRA_ALL_TRADABLE', '2026-03-06')
        """
    )

    written = backfill_listing_aliases_from_fund_page(
        conn,
        instrument_id=1,
        html=FUND_PAGE_HTML,
        asof_date="2026-03-09",
    )

    rows = conn.execute(
        """
        SELECT venue_mic, exchange_name, ticker, trading_currency, primary_flag, status, data_source, asof_date
        FROM listing
        WHERE instrument_id = 1
        ORDER BY primary_flag DESC, venue_mic, ticker
        """
    ).fetchall()

    assert written == 1
    assert [tuple(row) for row in rows] == [
        ("XETR", "Deutsche Boerse Xetra", "AVWS", "EUR", 1, "active", "XETRA_ALL_TRADABLE", "2026-03-06"),
        ("XLON", "London Stock Exchange", "AVGS", "USD", 0, "active", "AVANTIS_FUND_PAGE", "2026-03-09"),
    ]
