from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from etf_app.profile import ensure_product_profile_schema, refresh_product_profile
from etf_app.taxonomy import ensure_taxonomy_schema, load_universe_rows, upsert_taxonomy


FT_SOURCE = "ft_tearsheet"
PARSER_VERSION = "ft_tearsheet_v2"
REQUEST_TIMEOUT_SECONDS = 20
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
FT_EXCHANGE_BY_VENUE = {
    "XLON": "LSE",
    "XETR": "GER",
}
SUPPORTED_VENUES = tuple(FT_EXCHANGE_BY_VENUE)


@dataclass
class FtBackfillStats:
    attempted: int = 0
    resolved: int = 0
    summary_parsed: int = 0
    holdings_parsed: int = 0
    snapshots_inserted: int = 0
    profile_rows_upserted: int = 0
    taxonomy_rows_updated: int = 0


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_iso() -> str:
    return dt.date.today().isoformat()


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def venue_scope(arg: str) -> list[str]:
    if arg == "XLON":
        return ["XLON"]
    if arg == "XETR":
        return ["XETR"]
    return list(SUPPORTED_VENUES)


def normalize_identifier_values(values: Optional[list[str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        normalized = normalize_space(value).upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_metadata_snapshot(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            asof_date TEXT,
            source TEXT,
            source_url TEXT,
            ter REAL NULL,
            use_of_income TEXT NULL,
            ucits_compliant INTEGER NULL,
            quality_flag TEXT,
            raw_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_issuer_metadata_snapshot_instrument ON issuer_metadata_snapshot(instrument_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instrument_url_map(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            url_type TEXT,
            url TEXT,
            UNIQUE(instrument_id, url_type)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_instrument_url_map_instrument ON instrument_url_map(instrument_id)")


def load_targets(
    conn: sqlite3.Connection,
    limit: int,
    venue: str,
    *,
    tickers: Optional[list[str]] = None,
    isins: Optional[list[str]] = None,
) -> list[sqlite3.Row]:
    venues = venue_scope(venue)
    normalized_tickers = normalize_identifier_values(tickers)
    normalized_isins = normalize_identifier_values(isins)
    placeholders = ",".join("?" for _ in venues)
    taxonomy_join = ""
    taxonomy_filter = ""
    taxonomy_order = ""
    identifier_filter = ""
    identifier_params: list[object] = []
    identifier_clauses: list[str] = []
    if normalized_isins:
        isin_placeholders = ",".join("?" for _ in normalized_isins)
        identifier_clauses.append(f"UPPER(i.isin) IN ({isin_placeholders})")
        identifier_params.extend(normalized_isins)
    if normalized_tickers:
        ticker_placeholders = ",".join("?" for _ in normalized_tickers)
        identifier_clauses.append(
            f"""
            EXISTS (
                SELECT 1
                FROM listing lf
                WHERE lf.instrument_id = i.instrument_id
                  AND COALESCE(lf.status, 'active') = 'active'
                  AND UPPER(TRIM(lf.ticker)) IN ({ticker_placeholders})
            )
            """
        )
        identifier_params.extend(normalized_tickers)
    if identifier_clauses:
        identifier_filter = f"\n          AND ({' OR '.join(identifier_clauses)})"
    if table_exists(conn, "instrument_taxonomy"):
        taxonomy_join = "LEFT JOIN instrument_taxonomy t ON t.instrument_id = i.instrument_id"
        taxonomy_filter = """
          OR t.instrument_id IS NULL
          OR t.geography_region IS NULL
          OR t.equity_size IS NULL
          OR t.equity_style IS NULL
          OR t.sector IS NULL
        """
        taxonomy_order = """
            CASE WHEN t.instrument_id IS NULL THEN 0 ELSE 1 END,
            CASE WHEN t.geography_region IS NULL THEN 0 ELSE 1 END,
            CASE WHEN t.equity_size IS NULL THEN 0 ELSE 1 END,
            CASE WHEN t.equity_style IS NULL THEN 0 ELSE 1 END,
            CASE WHEN t.sector IS NULL THEN 0 ELSE 1 END,
        """
    sql = f"""
        WITH ranked_listings AS (
            SELECT
                l.instrument_id,
                l.ticker,
                l.trading_currency,
                l.venue_mic,
                l.listing_id,
                ROW_NUMBER() OVER (
                    PARTITION BY l.instrument_id
                    ORDER BY
                        CASE WHEN COALESCE(l.primary_flag, 0) = 1 THEN 0 ELSE 1 END,
                        CASE l.venue_mic
                            WHEN 'XLON' THEN 0
                            WHEN 'XETR' THEN 1
                            ELSE 9
                        END,
                        l.listing_id
                ) AS rn
            FROM listing l
            WHERE COALESCE(l.status, 'active') = 'active'
              AND l.venue_mic IN ({placeholders})
              AND l.ticker IS NOT NULL
              AND TRIM(l.ticker) <> ''
              AND l.trading_currency IS NOT NULL
              AND TRIM(l.trading_currency) <> ''
        )
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            rl.ticker,
            rl.trading_currency,
            rl.venue_mic
        FROM instrument i
        JOIN ranked_listings rl
          ON rl.instrument_id = i.instrument_id
         AND rl.rn = 1
        LEFT JOIN product_profile p
          ON p.instrument_id = i.instrument_id
        {taxonomy_join}
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
          AND COALESCE(i.status, 'active') = 'active'
          AND UPPER(COALESCE(i.instrument_type, '')) = 'ETF'
          {identifier_filter}
          AND (
              p.instrument_id IS NULL
              OR p.benchmark_name IS NULL
              OR TRIM(p.benchmark_name) = ''
              OR p.distribution_policy IS NULL
              OR p.fund_size_value IS NULL
              OR p.equity_size_hint IS NULL
              OR p.equity_style_hint IS NULL
              OR p.sector_hint IS NULL
              {taxonomy_filter}
          )
        ORDER BY
            CASE WHEN p.instrument_id IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.benchmark_name IS NULL OR TRIM(p.benchmark_name) = '' THEN 0 ELSE 1 END,
            CASE WHEN p.distribution_policy IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.fund_size_value IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.equity_size_hint IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.equity_style_hint IS NULL THEN 0 ELSE 1 END,
            CASE WHEN p.sector_hint IS NULL THEN 0 ELSE 1 END,
            {taxonomy_order}
            i.isin
        LIMIT ?
    """
    params: list[object] = [*venues, *identifier_params, limit]
    return conn.execute(sql, params).fetchall()


def load_symbol_candidates(conn: sqlite3.Connection, instrument_id: int, venue: str) -> list[str]:
    venues = venue_scope(venue)
    placeholders = ",".join("?" for _ in venues)
    rows = conn.execute(
        f"""
        SELECT ticker, trading_currency, venue_mic, listing_id
        FROM listing
        WHERE instrument_id = ?
          AND COALESCE(status, 'active') = 'active'
          AND venue_mic IN ({placeholders})
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
          AND trading_currency IS NOT NULL
          AND TRIM(trading_currency) <> ''
        ORDER BY
            CASE WHEN COALESCE(primary_flag, 0) = 1 THEN 0 ELSE 1 END,
            CASE venue_mic
                WHEN 'XLON' THEN 0
                WHEN 'XETR' THEN 1
                ELSE 9
            END,
            listing_id
        """,
        (instrument_id, *venues),
    ).fetchall()
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        exchange = FT_EXCHANGE_BY_VENUE.get(str(row["venue_mic"]))
        ticker = normalize_space(row["ticker"]).upper()
        currency = normalize_space(row["trading_currency"]).upper()
        if not exchange or not ticker or not currency:
            continue
        symbol = f"{ticker}:{exchange}:{currency}"
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    existing_summary = find_existing_url(conn, instrument_id, "ft_summary_page")
    if existing_summary:
        symbol = symbol_from_url(existing_summary)
        if symbol and symbol not in seen:
            out.insert(0, symbol)
    return out


def find_existing_url(conn: sqlite3.Connection, instrument_id: int, url_type: str) -> Optional[str]:
    try:
        row = conn.execute(
            """
            SELECT url
            FROM instrument_url_map
            WHERE instrument_id = ?
              AND url_type = ?
            """,
            (instrument_id, url_type),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row and row["url"]:
        return str(row["url"])
    return None


def symbol_from_url(url: str) -> Optional[str]:
    query = parse_qs(urlparse(url).query)
    symbol = query.get("s")
    if not symbol:
        return None
    value = normalize_space(symbol[0])
    return value or None


def summary_url(symbol: str) -> str:
    return f"https://markets.ft.com/data/etfs/tearsheet/summary?s={quote(symbol, safe=':')}"


def holdings_url(symbol: str) -> str:
    return f"https://markets.ft.com/data/etfs/tearsheet/holdings?s={quote(symbol, safe=':')}"


def search_url(query: str) -> str:
    return f"https://markets.ft.com/data/search?query={quote(query, safe='')}"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def fetch_html(session: requests.Session, url: str) -> Optional[str]:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return None
    text = response.text or ""
    if not text or "Sorry, no data" in text:
        return None
    return text


def parse_date_value(value: str) -> Optional[str]:
    text = normalize_space(value)
    if not text or text == "--":
        return None
    for prefix in ("As of ",):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    for fmt in ("%b %d %Y", "%B %d %Y", "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_percent_value(value: str) -> Optional[float]:
    text = normalize_space(value).replace("%", "")
    if not text or text == "--":
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def parse_scaled_amount(value: str) -> Optional[float]:
    text = normalize_space(value)
    match = re.search(
        r"([0-9][0-9,]*\.?[0-9]*)\s*(tn|trn|trillion|bn|billion|b|mn|million|m|k|thousand)?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    number = match.group(1).replace(",", "")
    try:
        base = float(number)
    except ValueError:
        return None
    suffix = (match.group(2) or "").lower()
    multiplier = {
        "tn": 1_000_000_000_000.0,
        "trn": 1_000_000_000_000.0,
        "trillion": 1_000_000_000_000.0,
        "bn": 1_000_000_000.0,
        "billion": 1_000_000_000.0,
        "b": 1_000_000_000.0,
        "mn": 1_000_000.0,
        "million": 1_000_000.0,
        "m": 1_000_000.0,
        "k": 1_000.0,
        "thousand": 1_000.0,
    }.get(suffix, 1.0)
    return base * multiplier


def normalize_income_treatment(value: str) -> Optional[str]:
    text = normalize_space(value)
    if not text or text == "--":
        return None
    upper = text.upper()
    if "ACCUM" in upper:
        return "Accumulating"
    if "DIST" in upper or "INCOME" in upper:
        return "Distributing"
    return text


def normalize_equity_size(value: str) -> Optional[str]:
    text = normalize_space(value).lower()
    if not text:
        return None
    if "large" in text or "giant" in text:
        return "large"
    if "mid" in text or "medium" in text:
        return "mid"
    if "small" in text or "micro" in text:
        return "small"
    return text


def normalize_equity_style(value: str) -> Optional[str]:
    text = normalize_space(value).lower()
    if not text:
        return None
    if "value" in text:
        return "value"
    if "growth" in text:
        return "growth"
    if "blend" in text or "core" in text:
        return "blend"
    return text


def normalize_sector_name(value: str) -> Optional[str]:
    text = normalize_space(value)
    if not text or text == "--" or text.lower() == "other":
        return None
    mapping = {
        "Technology": "technology",
        "Healthcare": "health_care",
        "Health Care": "health_care",
        "Financial Services": "financials",
        "Energy": "energy",
        "Utilities": "utilities",
        "Industrials": "industrials",
        "Real Estate": "real_estate",
        "Basic Materials": "materials",
        "Materials": "materials",
        "Communication Services": "communication",
        "Consumer Cyclical": "consumer_cyclical",
        "Consumer Defensive": "consumer_defensive",
    }
    return mapping.get(text, text.lower().replace(" ", "_"))


def extract_objective_text(soup: BeautifulSoup) -> Optional[str]:
    for module in soup.select("div.mod-aside__module"):
        heading = module.find(["h2", "h3"])
        heading_text = normalize_space(heading.get_text(" ", strip=True)) if heading else ""
        if heading_text.lower() != "objective":
            continue
        text = normalize_space(module.get_text(" ", strip=True))
        return text or None
    return None


def parse_benchmark_name_from_objective(value: str) -> Optional[str]:
    text = normalize_space(value)
    if not text:
        return None
    patterns = (
        r"Benchmark Index\s*\(being the\s+([^)]+?)\)",
        r"track both the upward and the downward evolution of the\s+(.+?)\s+\(the\s+[\"“]?Index[\"”]?\)",
        r"track the performance of\s+(.+?)\s+\(the\s+[\"“]?Index[\"”]?\)",
        r"deliver the net total return performance of the\s+(.+?),\s+less the fees",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = normalize_space(match.group(1))
        if not candidate:
            continue
        candidate = re.sub(r"^(?:the\s+)", "", candidate, flags=re.IGNORECASE)
        if candidate.lower() == "index":
            continue
        return candidate
    return None


def extract_fact_rows(soup: BeautifulSoup) -> dict[str, BeautifulSoup]:
    facts: dict[str, BeautifulSoup] = {}
    for row in soup.select("table.mod-ui-table tr"):
        header = row.find("th")
        value = row.find("td")
        if not header or not value:
            continue
        label = normalize_space(header.get_text(" ", strip=True))
        if label:
            facts[label] = value
    return facts


def find_fact_cell(
    facts: dict[str, BeautifulSoup],
    *labels: str,
    prefix: str | None = None,
) -> Optional[BeautifulSoup]:
    normalized_lookup = {normalize_space(label).lower(): value for label, value in facts.items()}
    for label in labels:
        match = normalized_lookup.get(normalize_space(label).lower())
        if match is not None:
            return match
    if prefix is not None:
        prefix_lower = normalize_space(prefix).lower()
        for label, value in facts.items():
            if normalize_space(label).lower().startswith(prefix_lower):
                return value
    return None


def parse_amount_cell(cell) -> tuple[Optional[float], Optional[str], Optional[str]]:
    text = normalize_space(cell.get_text(" ", strip=True))
    if not text or text == "--":
        return None, None, None
    amount = parse_scaled_amount(text)
    currency_match = re.search(r"\b([A-Z]{3})\b", text)
    currency = currency_match.group(1) if currency_match else None
    asof_match = re.search(r"As of ([A-Za-z]{3,9} \d{1,2}(?:,)? \d{4})", text)
    asof = parse_date_value(asof_match.group(0)) if asof_match else None
    return amount, currency, asof


def extract_ft_search_symbols(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    symbols: list[str] = []
    seen: set[str] = set()
    for link in soup.select('a[href*="/data/etfs/tearsheet/summary?s="]'):
        href = normalize_space(link.get("href"))
        if not href:
            continue
        symbol = symbol_from_url(href if href.startswith("http") else f"https://markets.ft.com{href}")
        if symbol is None or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def load_search_queries(conn: sqlite3.Connection, instrument_id: int, expected_isin: str) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def add_query(value: object) -> None:
        text = normalize_space(value)
        if not text or text in seen:
            return
        seen.add(text)
        queries.append(text)

    add_query(expected_isin)
    row = conn.execute(
        """
        SELECT instrument_name
        FROM instrument
        WHERE instrument_id = ?
        """,
        (instrument_id,),
    ).fetchone()
    listing_rows = conn.execute(
        """
        SELECT ticker
        FROM listing
        WHERE instrument_id = ?
          AND COALESCE(status, 'active') = 'active'
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
        ORDER BY COALESCE(primary_flag, 0) DESC, listing_id
        """,
        (instrument_id,),
    ).fetchall()
    for listing_row in listing_rows:
        add_query(listing_row["ticker"])
    if row is not None:
        add_query(row["instrument_name"])
    return queries


def parse_ft_summary_html(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "lxml")
    facts = extract_fact_rows(soup)
    objective_text = extract_objective_text(soup)
    instrument_name = normalize_space(
        soup.select_one(".mod-tearsheet-overview__header__name").get_text(" ", strip=True)
        if soup.select_one(".mod-tearsheet-overview__header__name")
        else ""
    )
    benchmark_name = parse_benchmark_name_from_objective(objective_text or "")
    investment_style_text = find_fact_cell(facts, "Investment style (stocks)", prefix="Investment style")
    equity_size_hint: Optional[str] = None
    equity_style_hint: Optional[str] = None
    if investment_style_text is not None:
        for line in investment_style_text.get_text("\n", strip=True).splitlines():
            text = normalize_space(line)
            if text.lower().startswith("market cap:"):
                equity_size_hint = normalize_equity_size(text.split(":", 1)[1])
            elif text.lower().startswith("investment style:"):
                equity_style_hint = normalize_equity_style(text.split(":", 1)[1])
        flat_style_text = normalize_space(investment_style_text.get_text(" ", strip=True))
        if equity_size_hint is None:
            equity_size_hint = normalize_equity_size(flat_style_text)
        if equity_style_hint is None:
            equity_style_hint = normalize_equity_style(flat_style_text)

    fund_size_value: Optional[float] = None
    fund_size_currency: Optional[str] = None
    fund_size_asof: Optional[str] = None
    fund_size_scope: Optional[str] = None
    if (fund_size_cell := find_fact_cell(facts, "Fund size", prefix="Fund size")) is not None:
        fund_size_value, fund_size_currency, fund_size_asof = parse_amount_cell(fund_size_cell)
        if fund_size_value is not None:
            fund_size_scope = "fund"
    if fund_size_value is None and (share_class_size_cell := find_fact_cell(facts, "Share class size", prefix="Share class size")) is not None:
        fund_size_value, fund_size_currency, fund_size_asof = parse_amount_cell(share_class_size_cell)
        if fund_size_value is not None:
            fund_size_scope = "share_class"

    ongoing_charge_cell = find_fact_cell(facts, "Ongoing charge", prefix="Ongoing charge")
    ongoing_charge = parse_percent_value(ongoing_charge_cell.get_text(" ", strip=True)) if ongoing_charge_cell is not None else None
    income_cell = find_fact_cell(facts, "Income treatment", "Use of income", prefix="Income")
    income_treatment = normalize_income_treatment(income_cell.get_text(" ", strip=True)) if income_cell is not None else None
    domicile_cell = find_fact_cell(facts, "Domicile", prefix="Domicile")
    domicile_country = normalize_space(domicile_cell.get_text(" ", strip=True)) if domicile_cell is not None else None
    isin_cell = find_fact_cell(facts, "ISIN", prefix="ISIN")
    isin = normalize_space(isin_cell.get_text(" ", strip=True)) if isin_cell is not None else None
    ucits_compliant = 1 if "UCITS" in instrument_name.upper() else None

    parsed: dict[str, object] = {
        "instrument_name": instrument_name or None,
        "isin": isin or None,
        "ter": ongoing_charge,
        "use_of_income": income_treatment,
        "ucits_compliant": ucits_compliant,
        "benchmark_name": benchmark_name,
        "asset_class_hint": "Equity" if equity_size_hint or equity_style_hint else None,
        "domicile_country": domicile_country or None,
        "fund_size_value": fund_size_value,
        "fund_size_currency": fund_size_currency,
        "fund_size_asof": fund_size_asof,
        "fund_size_scope": fund_size_scope,
        "equity_size_hint": equity_size_hint,
        "equity_style_hint": equity_style_hint,
    }
    return parsed


def parse_ft_holdings_html(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "lxml")
    table = None
    sector_section = soup.select_one('.mod-diversification__column[data-mod-section="Sector"]')
    if sector_section is not None:
        table = sector_section.select_one("table.mod-ui-table")
    if table is None:
        table = soup.select_one(".mod-weightings__sectors__table table.mod-ui-table")
    if table is None:
        for candidate in soup.select("table.mod-ui-table"):
            header = normalize_space(candidate.find("thead").get_text(" ", strip=True) if candidate.find("thead") else "")
            if header.startswith("Sector % Net assets"):
                table = candidate
                break
    if table is None:
        return {}
    sector_rows: list[dict[str, object]] = []
    for row in table.select("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = normalize_space(cells[0].get_text(" ", strip=True))
        weight = parse_percent_value(cells[1].get_text(" ", strip=True))
        normalized = normalize_sector_name(label)
        if normalized is None or weight is None:
            continue
        sector_rows.append({"label": label, "sector": normalized, "weight": weight})
    if not sector_rows:
        return {}
    dominant = max(sector_rows, key=lambda item: float(item["weight"]))
    return {
        "sector_weights": sector_rows,
        "sector_hint": dominant["sector"],
        "sector_weight": dominant["weight"],
    }


def insert_issuer_metadata_snapshot(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    asof_date: str,
    source_url: str,
    ter: Optional[float],
    use_of_income: Optional[str],
    ucits_compliant: Optional[int],
    quality_flag: str,
    raw_json: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO issuer_metadata_snapshot(
            instrument_id,
            asof_date,
            source,
            source_url,
            ter,
            use_of_income,
            ucits_compliant,
            quality_flag,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instrument_id,
            asof_date,
            FT_SOURCE,
            source_url,
            ter,
            use_of_income,
            ucits_compliant,
            quality_flag,
            json.dumps(raw_json, ensure_ascii=True),
        ),
    )


def upsert_url_map(conn: sqlite3.Connection, instrument_id: int, url_type: str, url: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_url_map(instrument_id, url_type, url)
        VALUES (?, ?, ?)
        ON CONFLICT(instrument_id, url_type) DO UPDATE SET
            url = excluded.url
        """,
        (instrument_id, url_type, url),
    )


def resolve_symbol(
    conn: sqlite3.Connection,
    session: requests.Session,
    *,
    instrument_id: int,
    expected_isin: str,
    venue: str,
) -> tuple[Optional[str], Optional[str], dict[str, object]]:
    attempted_symbols: set[str] = set()

    def try_symbol(symbol: str) -> tuple[Optional[str], Optional[str], dict[str, object]]:
        if symbol in attempted_symbols:
            return None, None, {}
        attempted_symbols.add(symbol)
        html = fetch_html(session, summary_url(symbol))
        if html is None:
            return None, None, {}
        parsed = parse_ft_summary_html(html)
        if normalize_space(parsed.get("isin")) != expected_isin:
            return None, None, {}
        return symbol, html, parsed

    for symbol in load_symbol_candidates(conn, instrument_id, venue):
        matched_symbol, html, parsed = try_symbol(symbol)
        if matched_symbol is not None:
            return matched_symbol, html, parsed

    for query in load_search_queries(conn, instrument_id, expected_isin):
        search_html = fetch_html(session, search_url(query))
        if search_html is None:
            continue
        for symbol in extract_ft_search_symbols(search_html):
            matched_symbol, html, parsed = try_symbol(symbol)
            if matched_symbol is not None:
                return matched_symbol, html, parsed
    return None, None, {}


def flush_derived_tables(conn: sqlite3.Connection, stats: FtBackfillStats) -> None:
    profile_stats = refresh_product_profile(conn)
    stats.profile_rows_upserted = profile_stats.product_profile_rows_upserted
    stats.taxonomy_rows_updated = upsert_taxonomy(conn, load_universe_rows(conn))
    conn.commit()


def run_ft_metadata_backfill(
    *,
    db_path: str,
    limit: int,
    venue: str,
    sleep_seconds: float,
    tickers: Optional[list[str]] = None,
    isins: Optional[list[str]] = None,
    commit_every: int = 0,
) -> FtBackfillStats:
    conn = sqlite3.connect(str(Path(db_path)))
    conn.row_factory = sqlite3.Row
    stats = FtBackfillStats()
    try:
        ensure_product_profile_schema(conn)
        ensure_taxonomy_schema(conn)
        ensure_tables(conn)

        session = build_session()
        targets = load_targets(conn, limit=limit, venue=venue, tickers=tickers, isins=isins)
        pending_flush = 0
        for row in targets:
            stats.attempted += 1
            instrument_id = int(row["instrument_id"])
            isin = str(row["isin"])
            symbol, summary_html, summary_parsed = resolve_symbol(
                conn,
                session,
                instrument_id=instrument_id,
                expected_isin=isin,
                venue=venue,
            )
            if not symbol or summary_html is None:
                continue
            stats.resolved += 1
            stats.summary_parsed += 1

            summary_page_url = summary_url(symbol)
            holdings_page_url = holdings_url(symbol)
            holdings_html = fetch_html(session, holdings_page_url)
            holdings_parsed = parse_ft_holdings_html(holdings_html) if holdings_html else {}
            if holdings_parsed:
                stats.holdings_parsed += 1

            profile_metadata = {
                "benchmark_name": summary_parsed.get("benchmark_name"),
                "asset_class_hint": summary_parsed.get("asset_class_hint"),
                "domicile_country": summary_parsed.get("domicile_country"),
                "fund_size_value": summary_parsed.get("fund_size_value"),
                "fund_size_currency": summary_parsed.get("fund_size_currency"),
                "fund_size_asof": summary_parsed.get("fund_size_asof"),
                "fund_size_scope": summary_parsed.get("fund_size_scope"),
                "equity_size_hint": summary_parsed.get("equity_size_hint"),
                "equity_style_hint": summary_parsed.get("equity_style_hint"),
                "sector_hint": holdings_parsed.get("sector_hint"),
                "sector_weight": holdings_parsed.get("sector_weight"),
            }
            raw_json = {
                "source": FT_SOURCE,
                "parser_version": PARSER_VERSION,
                "symbol": symbol,
                "summary_url": summary_page_url,
                "holdings_url": holdings_page_url,
                "profile_metadata": {key: value for key, value in profile_metadata.items() if value is not None},
            }
            if summary_parsed.get("instrument_name"):
                raw_json["instrument_name"] = summary_parsed["instrument_name"]
            if summary_parsed.get("isin"):
                raw_json["isin"] = summary_parsed["isin"]
            if holdings_parsed.get("sector_weights"):
                raw_json["sector_weights"] = holdings_parsed["sector_weights"]

            insert_issuer_metadata_snapshot(
                conn,
                instrument_id=instrument_id,
                asof_date=str(summary_parsed.get("fund_size_asof") or today_iso()),
                source_url=summary_page_url,
                ter=summary_parsed.get("ter"),
                use_of_income=summary_parsed.get("use_of_income"),
                ucits_compliant=summary_parsed.get("ucits_compliant"),
                quality_flag="ft_tearsheet_ok",
                raw_json=raw_json,
            )
            upsert_url_map(conn, instrument_id, "ft_summary_page", summary_page_url)
            upsert_url_map(conn, instrument_id, "ft_holdings_page", holdings_page_url)
            stats.snapshots_inserted += 1
            pending_flush += 1

            if commit_every > 0 and pending_flush >= commit_every:
                flush_derived_tables(conn, stats)
                pending_flush = 0

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        flush_derived_tables(conn, stats)
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill ETF metadata from FT ETF tearsheets")
    parser.add_argument("--db-path", default="stage1_etf.db", help="Path to SQLite DB")
    parser.add_argument("--limit", type=int, default=100, help="Maximum instruments to attempt")
    parser.add_argument(
        "--venue",
        choices=["XLON", "XETR", "ALL"],
        default="ALL",
        help="Supported FT venue scope",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between resolved FT fetches",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Optional ticker filter; repeatable and matched case-insensitively",
    )
    parser.add_argument(
        "--isin",
        action="append",
        default=[],
        help="Optional ISIN filter; repeatable and matched case-insensitively",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=0,
        help="Flush product_profile/taxonomy and commit every N resolved FT snapshots (0 means only at the end)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    stats = run_ft_metadata_backfill(
        db_path=args.db_path,
        limit=args.limit,
        venue=args.venue,
        sleep_seconds=args.sleep_seconds,
        tickers=args.ticker,
        isins=args.isin,
        commit_every=max(0, int(args.commit_every)),
    )
    print(
        f"ft metadata backfill: attempted={stats.attempted} resolved={stats.resolved} "
        f"summary_parsed={stats.summary_parsed} holdings_parsed={stats.holdings_parsed} "
        f"snapshots_inserted={stats.snapshots_inserted} "
        f"profile_rows_upserted={stats.profile_rows_upserted} "
        f"taxonomy_rows_updated={stats.taxonomy_rows_updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
