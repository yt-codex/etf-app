from __future__ import annotations

import datetime as dt
import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional


TAXONOMY_VERSION = "taxonomy_v2"
LEGACY_CLASSIFIER_VERSION = "stage4_classify_v4"

DOMICILE_BY_ISIN_PREFIX = {
    "IE": "Ireland",
    "LU": "Luxembourg",
    "FR": "France",
    "DE": "Germany",
    "GB": "United Kingdom",
    "JE": "Jersey",
    "CH": "Switzerland",
    "NL": "Netherlands",
}

ASIA_COUNTRIES = {"China", "India", "Japan", "South Korea", "Taiwan"}
EUROPE_COUNTRIES = {
    "Austria",
    "Bulgaria",
    "Croatia",
    "Czech Republic",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Luxembourg",
    "North Macedonia",
    "Poland",
    "Serbia",
    "Slovakia",
    "Slovenia",
    "Switzerland",
    "United Kingdom",
}

SPECIAL_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    (r"\bAM\.CE\.I-AV\.AM\.E\b", "US EQUITY"),
    (r"\bAM\.CE\.I-AV\.EU\.E\b", "EUROPE EQUITY"),
    (r"\bAM\.CE\.I-AV\.PA\.E\b", "PACIFIC EQUITY"),
    (r"\bGLSMCV\b", "GLOBAL SMALL CAP VALUE"),
    (r"\bGLBL\b", "GLOBAL"),
    (r"\bGLB\b", "GLOBAL"),
    (r"\bGL[\.\s]EQ\b", "GLOBAL EQUITY"),
    (r"\bEM[\.\s]EQ\b", "EMERGING MARKETS EQUITY"),
    (r"\bGEM\b", "EMERGING"),
    (r"\bWRLD\b", "WORLD"),
    (r"\bWLD\b", "WORLD"),
    (r"\bSP500\b", "S P 500"),
    (r"\bS&P500\b", "S P 500"),
    (r"\bS P500\b", "S P 500"),
    (r"\bNIK400\b", "NIKKEI 400"),
    (r"\bTBILLS\b", "TREASURY BILLS"),
    (r"\bTRES\b", "TREASURY"),
    (r"\bBND\b", "BOND"),
    (r"\bCRP\b", "CORP"),
    (r"\bHY\b", "HIGH YIELD"),
    (r"\bSHRT\b", "SHORT"),
    (r"\bST[\.\s]DUR\b", "SHORT DURATION"),
    (r"\bGVT\b", "GOVT"),
    (r"\bEPE\b", "EUROPE"),
    (r"\bERPE\b", "EUROPE"),
    (r"\bSWITIT[0-9A-Z]*\b", "SWITZERLAND"),
    (r"S\+P", "S P"),
    (r"S&P", "S P"),
    (r"\bEUROSTOXX([0-9]+)\b", r"EURO STOXX \1"),
    (r"\bEOSTXX([0-9]+)\b", r"EURO STOXX \1"),
    (r"\bEOSTXX\b", "EURO STOXX"),
    (r"\bESTX\b", "EURO STOXX"),
    (r"\bSTOX\b", "STOXX"),
    (r"\bDIVDAX2\b", "DIVDAX"),
    (r"\bDJ\b", "DOW JONES"),
    (r"\bEO\b", "EURO"),
    (r"\bEUROP\b", "EUROPE"),
    (r"\bMSCIWS\b", "MSCI WORLD SCREENED"),
    (r"\bSPWLDCONDISSCR\b", "S P WORLD CONSUMER DISCRETIONARY SCREENED"),
    (r"\bSPWLDHLTHCARESCR\b", "S P WORLD HEALTH CARE SCREENED"),
    (r"\bSPWLDINF\.?TECHSCR\b", "S P WORLD INFORMATION TECHNOLOGY SCREENED"),
)

PATTERNS = {
    "multi": [r"\bMULTI ASSET\b", r"\bBALANCED\b", r"\bPORTFOLIO\b", r"\bLIFECYCLE\b"],
    "cash": [r"\bMONEY MARKET\b", r"\bTREASURY BILL\b", r"\bT[\s-]?BILL\b", r"\bULTRA SHORT\b", r"\bOVERNIGHT\b", r"\b0[\s/-]?1\b", r"\bCASH\b"],
    "commodity_gold": [r"\bPHYSICAL GOLD\b", r"\bGOLD\b", r"\bBULLION\b"],
    "commodity_silver": [r"\bSILVER\b"],
    "commodity_energy": [r"\bCRUDE\b", r"\bWTI\b", r"\bBRENT\b", r"\bNATURAL GAS\b", r"\bENERGY\b"],
    "commodity_agriculture": [r"\bWHEAT\b", r"\bSOY\b", r"\bCORN\b", r"\bSUGAR\b", r"\bCOTTON\b", r"\bAGRICULTURE\b"],
    "commodity_industrial_metals": [r"\bCOPPER\b", r"\bALUMIN(?:IUM|UM)\b", r"\bNICKEL\b", r"\bZINC\b"],
    "commodity_precious_metals": [r"\bPLATINUM\b", r"\bPALLADIUM\b"],
    "commodity_broad": [r"\bCOMMOD(?:ITY|ITIES)?\b", r"\bCMCI\b"],
    "bond_core": [r"\bBOND(?:S)?\b", r"\bTREAS(?:URY)?\b", r"\bGOV(?:ERNMENT)?\b", r"\bGOVT\b", r"\bGILT(?:S)?\b", r"\bCORP(?:ORATE)?\b", r"\bCREDIT\b", r"\bSOVEREIGN\b", r"\bAGG(?:REGATE)?\b", r"\bLINK(?:ED|ERS?)?\b", r"\bTIPS\b", r"\bREXX\b", r"\bPFANDBRIEFE\b", r"\bCOVERED\b", r"\bFLOAT(?:ING)? RATE\b", r"\bFLOT RATE\b"],
    "bond_special": [r"I\.EB\.R\.", r"\bT[\s-]?BOND\b", r"\bT[\s-]?BND\b", r"\bTR BOND\b", r"\bTR BND\b"],
    "bond_type_linkers": [r"\bINFLATION\b", r"\bLINK(?:ED|ERS?)?\b", r"\bTIPS\b"],
    "bond_type_govt": [r"\bTREAS(?:URY)?\b", r"\bTRSY\b", r"\bTSY\b", r"\bGOV(?:ERNMENT)?\b", r"\bGOVT\b", r"\bGILT(?:S)?\b", r"\bSOVEREIGN\b", r"\bUST\b", r"\bJGB\b", r"\bBUND\b", r"\bOAT\b", r"\bBTP\b", r"\bTREASURY BILL\b", r"\bT[\s-]?BILL\b", r"\bPFANDBRIEFE\b", r"\bREXX\b", r"I\.EB\.R\.", r"\bTR BOND\b", r"\bTR BND\b"],
    "bond_type_aggregate": [r"\bAGG(?:REGATE)?\b", r"\bTOTAL BOND\b", r"\bAGG BOND\b"],
    "bond_type_corp": [r"\bCORP(?:ORATE)?\b", r"\bCREDIT\b", r"\bHIGH YIELD\b", r"\bINVESTMENT GRADE\b", r"\bCORP BOND\b"],
    "equity_hint": [r"\bMSCI\b", r"\bFTSE\b", r"\bSTOXX\b", r"\bEURO STOXX", r"\bS P\b", r"\bNASDAQ\b", r"\bRUSSELL\b", r"\bNIKKEI\b", r"\bTOPIX\b", r"\bDAX\b", r"\bDAX[0-9A-Z]*\b", r"\bMDAX\b", r"\bSDAX\b", r"\bTECDAX\b", r"\bDIVDAX\b", r"\bATX\b", r"\bSLI\b", r"\bSMI\b", r"\bSOFIX\b", r"\bCROBEX\b", r"\bPX\b", r"\bASE\b", r"\bBUX\b", r"\bMBI10\b", r"\bWIG20\b", r"\bSAX\b", r"\bSBI\b", r"\bBELEX\b", r"\bMIB\b", r"\bEMU\b", r"\bBRAZIL\b", r"\bSPAIN\b", r"\bUK\b", r"\bEU S 50\b", r"\bDOW JONES\b", r"\bDJIA\b", r"\bEQUITY\b", r"\bEQ\b", r"\bSHARES?\b", r"\bSTOCK\b", r"\bWORLD\b", r"\bGLOBAL\b", r"\bEMERGING\b", r"\bSMALL CAP\b", r"\bMID CAP\b", r"\bLARGE CAP\b", r"\bVALUE\b", r"\bGROWTH\b", r"\bQUALITY\b", r"\bMOMENTUM\b", r"\bDIVIDEND\b", r"\bEPI\b"],
    "region_global": [r"\bWORLD\b", r"\bGLOBAL\b", r"\bACWI\b", r"\bALL COUNTRY\b", r"\bALL WORLD\b"],
    "region_north_america": [r"\bNORTH AMERICA\b"],
    "region_em": [r"\bEMERGING\b", r"\bLATIN AMERICA\b", r"\bEM IMI\b", r"\bEM MARKETS?\b", r"\bMSCI EM\b", r"\bEM EQ\b"],
    "region_europe": [r"\bEUROPE\b", r"\bEU\b", r"\bEURO STOXX", r"\bEUROZONE\b", r"\bSTOXX EUROPE\b", r"\bEURO ST\b", r"\bEU 600\b", r"\bS E 600\b", r"\bEU S 50\b", r"\bEMU\b", r"\bMIB\b", r"\bEURO PRIME\b", r"\bSPAIN\b", r"\bUK\b"],
    "region_asia": [r"\bASIA\b", r"\bPACIFIC\b", r"\bASIA PACIFIC\b"],
    "sector_technology": [r"\bTECH(?:NOLOGY)?\b", r"\bTECDAX\b", r"\bSEMICONDUCTOR\b"],
    "sector_health_care": [r"\bHEALTH(?: CARE|CARE)?\b", r"\bBIOTECH\b"],
    "sector_financials": [r"\bFINANCIAL(?:S)?\b", r"\bBANK(?:S)?\b"],
    "sector_energy": [r"\bENERGY\b"],
    "sector_utilities": [r"\bUTILIT(?:Y|IES)\b"],
    "sector_industrials": [r"\bINDUSTRIAL(?:S)?\b"],
    "sector_real_estate": [r"\bREAL ESTATE\b", r"\bPROPERTY\b"],
    "sector_materials": [r"\bMATERIALS\b", r"\bCHEM(?:ICALS?)?\b"],
    "sector_communication": [r"\bCOMMUNICATION\b", r"\bTELECOM\b", r"\bMEDIA\b"],
    "theme_robotics": [r"\bROBOTICS\b"],
    "theme_ai": [r"\bAI\b", r"\bARTIFICIAL INTELLIGENCE\b"],
    "theme_esg": [r"\bESG\b", r"\bSUSTAIN(?:ABLE|ABILITY)?\b", r"\bPARIS ALIGNED\b", r"\bSCR(?:EENED)?\b"],
    "factor_quality": [r"\bQUALITY\b"],
    "factor_momentum": [r"\bMOMENTUM\b"],
    "factor_dividend": [r"\bDIVIDEND\b", r"\bINCOME\b", r"\bEPI\b", r"\bSELECT DIV\b"],
    "factor_min_vol": [r"\bMIN(?:IMUM)? VOL(?:ATILITY)?\b", r"\bLOW VOL(?:ATILITY)?\b"],
    "size_small": [r"\bSMALL CAP\b", r"\bSMALL\b", r"\bSDAX\b", r"\bSMALL 200\b"],
    "size_mid": [r"\bMID CAP\b", r"\bMDAX\b", r"\bMID 200\b"],
    "size_large": [r"\bLARGE CAP\b", r"\bDAX\b", r"\bS P 500\b", r"\bEURO STOXX 50\b", r"\bTOP 20\b"],
    "style_value": [r"\bVALUE\b", r"\bVAL\b"],
    "style_growth": [r"\bGROWTH\b"],
    "duration_short": [r"\b0[\s/-]?1\b", r"\b1[\s/-]?3\b", r"\b0[\s/-]?6M\b", r"\b3[\s/-]?6M\b", r"\bSHORT\b", r"\bULTRA SHORT\b", r"\bT[\s-]?BILL\b", r"\bFLOAT(?:ING)? RATE\b", r"\bFLOT RATE\b", r"\bFRN\b"],
    "duration_intermediate": [r"\b3[\s/-]?7\b", r"\b5[\s/-]?10\b", r"\b7[\s/-]?10\b", r"\bINTERMEDIATE\b"],
    "duration_long": [r"\b10\+\b", r"\b15\+\b", r"\b20\+\b", r"\b30\+\b", r"\bLONG\b", r"\bLONG DATED\b"],
}

COUNTRY_RULES = (
    ("United States", "us", [r"\bUSA\b", r"\bUS\b", r"\bS P 500\b", r"\bSP 500\b", r"\bS AND P 500\b", r"\bNASDAQ\b", r"\bRUSSELL\b", r"\bDOW JONES\b", r"\bDJIA\b"]),
    ("Germany", "europe", [r"\bDAX\b", r"\bDAX[0-9A-Z]*\b", r"\bMDAX\b", r"\bSDAX\b", r"\bTECDAX\b", r"\bDIVDAX\b", r"\bPFANDBRIEFE\b", r"\bREXX\b"]),
    ("Austria", "europe", [r"\bATX\b"]),
    ("Switzerland", "europe", [r"\bSLI\b", r"\bSMI\b", r"\bSWITZERLAND\b", r"\bSWISS\b"]),
    ("Luxembourg", "europe", [r"\bLUX(?:EMBOURG)?\b"]),
    ("Netherlands", "europe", [r"\bAEX\b"]),
    ("Italy", "europe", [r"\bMIB\b"]),
    ("Bulgaria", "europe", [r"\bSOFIX\b", r"\bBULGARIA\b"]),
    ("Croatia", "europe", [r"\bCROBEX\b", r"\bCROAT(?:IA)?\b"]),
    ("Czech Republic", "europe", [r"\bCZECH\b", r"\bPX\b"]),
    ("Greece", "europe", [r"\bGREECE\b", r"\bASE\b"]),
    ("Hungary", "europe", [r"\bHUNG(?:ARY)?\b", r"\bBUX\b"]),
    ("North Macedonia", "europe", [r"\bMAC(?:EDONIA)?\b", r"\bMBI10\b"]),
    ("Poland", "europe", [r"\bPOLAND\b", r"\bWIG20\b"]),
    ("Slovakia", "europe", [r"\bSLOVAK(?:IA)?\b", r"\bSAX\b"]),
    ("Slovenia", "europe", [r"\bSLOV\.\b", r"\bSBI TOP\b", r"\bSLOVENIA\b"]),
    ("Serbia", "europe", [r"\bSERB(?:IA)?\b", r"\bBELEX\b"]),
    ("Spain", "europe", [r"\bSPAIN\b", r"\bIBEX\b"]),
    ("United Kingdom", "europe", [r"\bUK\b", r"\bFTSE 100\b", r"\bFTSE 250\b"]),
    ("Brazil", "em", [r"\bBRAZIL\b"]),
    ("Japan", "asia", [r"\bJAPAN\b", r"\bNIKKEI\b", r"\bTOPIX\b"]),
    ("South Africa", "em", [r"\bSOUTH AFRICA\b", r"\bMSCI SA\b"]),
    ("China", "asia", [r"\bCHINA\b", r"\bCSI\b", r"\bHANG SENG\b"]),
    ("India", "asia", [r"\bINDIA\b"]),
    ("South Korea", "asia", [r"\bKOREA\b", r"\bKOSPI\b"]),
    ("Taiwan", "asia", [r"\bTAIWAN\b"]),
)

HEDGED_TARGETS = (
    ("USD", [r"\bUSD HEDGED\b", r"\bUSDH\b"]),
    ("EUR", [r"\bEUR HEDGED\b", r"\bEURH\b", r"\bEUR-?H\b"]),
    ("GBP", [r"\bGBP HEDGED\b", r"\bGBPH\b", r"\bGBP-?H\b"]),
    ("JPY", [r"\bJPY HEDGED\b", r"\bJPYH\b"]),
    ("CHF", [r"\bCHF HEDGED\b", r"\bCHFH\b"]),
)


@dataclass
class TaxonomyResult:
    asset_class: str
    geography_scope: Optional[str]
    geography_region: Optional[str]
    geography_country: Optional[str]
    equity_size: Optional[str]
    equity_style: Optional[str]
    factor: Optional[str]
    sector: Optional[str]
    theme: Optional[str]
    bond_type: Optional[str]
    duration_bucket: Optional[str]
    duration_years_low: Optional[float]
    duration_years_high: Optional[float]
    commodity_type: Optional[str]
    cash_proxy_flag: int
    gold_flag: int
    cash_flag: int
    govt_bond_flag: int
    hedged_flag: int
    hedged_target: Optional[str]
    domicile_country: Optional[str]
    distribution_policy: Optional[str]
    evidence_json: str
    asset_bucket: str
    equity_region: Optional[str]


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _compile_patterns(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(pattern, flags=re.IGNORECASE) for pattern in patterns]


COMPILED_PATTERNS = {key: _compile_patterns(value) for key, value in PATTERNS.items()}
COMPILED_COUNTRY_RULES = [(country, region, _compile_patterns(patterns)) for country, region, patterns in COUNTRY_RULES]
COMPILED_HEDGED_TARGETS = [(target, _compile_patterns(patterns)) for target, patterns in HEDGED_TARGETS]
GOLD_EQUITY_PROXY_PATTERNS = _compile_patterns([r"\bMINERS?\b", r"\bMINING\b", r"\bPRODUCERS?\b", r"\bBUGS\b", r"\bROYALT(?:Y|IES)\b"])


def _has_any(text: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _match_country(text: str) -> tuple[Optional[str], Optional[str]]:
    for country, region, patterns in COMPILED_COUNTRY_RULES:
        if _has_any(text, patterns):
            return country, region
    return None, None


def _normalize_name(name: str | None) -> str:
    if not name:
        return ""
    text = name.upper().replace("&", " AND ")
    for pattern, replacement in SPECIAL_REPLACEMENTS:
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"[^A-Z0-9+]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_gold_equity_proxy(source_text: str) -> bool:
    return _has_any(source_text, GOLD_EQUITY_PROXY_PATTERNS)


def _has_gold_commodity_exposure(source_text: str) -> bool:
    return (not _is_gold_equity_proxy(source_text)) and _has_any(source_text, COMPILED_PATTERNS["commodity_gold"])


def _normalize_duration_source(name: str | None) -> str:
    if not name:
        return ""
    text = name.upper()
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    return re.sub(r"\s+", " ", text).strip()


def _parse_duration_bounds(source_text: str) -> tuple[Optional[float], Optional[float]]:
    month_range_match = re.search(
        r"\b(0|1|3|6|9|12)\s*[-/]\s*(1|3|6|9|12)\s*M(?:ONTHS?)?\b",
        source_text,
    )
    if month_range_match:
        low = float(month_range_match.group(1)) / 12.0
        high = float(month_range_match.group(2)) / 12.0
        if low <= high:
            return low, high
    range_match = re.search(
        r"\b(0|1|3|5|7|10|15|20|25|30)\s*[-/]\s*(1|3|5|7|10|15|20|25|30)\s*(?:Y(?:EARS?)?|YR|Y)?\b",
        source_text,
    )
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        if low <= high:
            return low, high
    plus_match = re.search(r"\b(10|15|20|30)\s*\+\s*(?:Y(?:EARS?)?|YR|Y)?\b", source_text)
    if plus_match:
        return float(plus_match.group(1)), None
    return None, None


def _derive_duration_bucket(
    *,
    source_text: str,
    normalized_text: str,
    low: Optional[float],
    high: Optional[float],
    cash_proxy_flag: int,
) -> str:
    has_long_keyword = ("LONG DATED" in source_text) or ("LONG DURATION" in source_text)
    has_short_keyword = bool(re.search(r"\b(ULTRASHORT|ULTRA SHORT|SHORT)\b", source_text))
    has_explicit_short_range = bool(re.search(r"\b(0\s*[-/]\s*1|1\s*[-/]\s*3)\b", source_text))
    if low is not None:
        if low >= 10 or has_long_keyword:
            return "long"
        if high is not None and high <= 3:
            return "short"
        if high is not None and low <= 10 and high >= 3:
            return "intermediate"
    if has_long_keyword or _has_any(normalized_text, COMPILED_PATTERNS["duration_long"]):
        return "long"
    if cash_proxy_flag == 1 or has_explicit_short_range or has_short_keyword or _has_any(normalized_text, COMPILED_PATTERNS["duration_short"]):
        return "short"
    if _has_any(normalized_text, COMPILED_PATTERNS["duration_intermediate"]):
        return "intermediate"
    return "unknown"


def _domicile_country(isin: str | None) -> Optional[str]:
    if not isin or len(isin) < 2:
        return None
    return DOMICILE_BY_ISIN_PREFIX.get(isin[:2].upper())


def _legacy_equity_region(
    geography_region: Optional[str],
    geography_country: Optional[str],
    sector: Optional[str],
) -> str:
    if sector:
        return "sector"
    if geography_region == "global":
        return "global"
    if geography_region == "north_america":
        return "us"
    if geography_country == "United States" or geography_region == "us":
        return "us"
    if geography_region == "em":
        return "em"
    if geography_region == "asia" or geography_country in ASIA_COUNTRIES:
        return "asia"
    if geography_region == "europe" or geography_country in EUROPE_COUNTRIES:
        return "europe"
    return "unknown"


def _normalize_asset_class_hint(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if any(token in text for token in ("BOND", "FIXED INCOME", "TREASURY", "CREDIT", "GILT")):
        return "bond"
    if any(token in text for token in ("COMMODITY", "ETC", "BULLION", "PRECIOUS METAL")):
        return "commodity"
    if any(token in text for token in ("MONEY MARKET", "CASH")):
        return "cash"
    if any(token in text for token in ("MULTI", "BALANCED", "PORTFOLIO")):
        return "multi"
    if any(token in text for token in ("EQUITY", "SHARE", "STOCK")):
        return "equity"
    return None


def classify_instrument(
    *,
    isin: str | None,
    instrument_name: str | None,
    instrument_type: str | None,
    distribution_policy: str | None,
    benchmark_name: str | None = None,
    asset_class_hint: str | None = None,
    replication_method: str | None = None,
    hedged_flag: int | None = None,
    hedged_target: str | None = None,
    domicile_country: str | None = None,
) -> TaxonomyResult:
    original_name = instrument_name or ""
    benchmark_text = benchmark_name or ""
    combined_name = " ".join(part for part in (instrument_name, benchmark_name) if part)
    text = _normalize_name(original_name)
    analysis_text = _normalize_name(combined_name)
    source_text = _normalize_duration_source(original_name)
    analysis_source_text = _normalize_duration_source(combined_name)
    evidence: list[str] = []
    metadata_asset_class = _normalize_asset_class_hint(asset_class_hint)
    if benchmark_text:
        evidence.append("profile:benchmark_name")
    if metadata_asset_class:
        evidence.append(f"profile:asset_class_hint={metadata_asset_class}")
    if replication_method:
        evidence.append(f"profile:replication={str(replication_method).lower()}")

    gold_keyword_detected = _has_any(source_text, COMPILED_PATTERNS["commodity_gold"])
    gold_flag = 0
    if _has_gold_commodity_exposure(source_text):
        gold_flag = 1
        evidence.append("commodity:gold")
    elif gold_keyword_detected and _is_gold_equity_proxy(source_text):
        evidence.append("commodity:gold_proxy_equity_excluded")

    commodity_type: Optional[str] = None
    for commodity_name, key in (
        ("silver", "commodity_silver"),
        ("energy", "commodity_energy"),
        ("agriculture", "commodity_agriculture"),
        ("industrial_metals", "commodity_industrial_metals"),
        ("precious_metals", "commodity_precious_metals"),
        ("broad_commodities", "commodity_broad"),
    ):
        if gold_flag == 1:
            commodity_type = "gold"
            break
        if _has_any(analysis_text, COMPILED_PATTERNS[key]):
            commodity_type = commodity_name
            evidence.append(f"commodity:{commodity_name}")
            break

    cash_proxy_flag = 1 if _has_any(analysis_source_text, COMPILED_PATTERNS["cash"]) or _has_any(analysis_text, COMPILED_PATTERNS["cash"]) else 0
    multi_flag = metadata_asset_class == "multi" or _has_any(analysis_text, COMPILED_PATTERNS["multi"])
    bond_flag = (
        metadata_asset_class == "bond"
        or _has_any(analysis_text, COMPILED_PATTERNS["bond_core"])
        or _has_any(analysis_source_text.upper(), COMPILED_PATTERNS["bond_special"])
    )
    govt_bond_flag = 1 if _has_any(analysis_text, COMPILED_PATTERNS["bond_type_govt"]) or _has_any(analysis_source_text.upper(), COMPILED_PATTERNS["bond_special"]) else 0
    low, high = _parse_duration_bounds(analysis_source_text)
    duration_bucket = _derive_duration_bucket(
        source_text=analysis_source_text,
        normalized_text=analysis_text,
        low=low,
        high=high,
        cash_proxy_flag=cash_proxy_flag,
    )

    sector: Optional[str] = None
    for sector_name, key in (
        ("technology", "sector_technology"),
        ("health_care", "sector_health_care"),
        ("financials", "sector_financials"),
        ("energy", "sector_energy"),
        ("utilities", "sector_utilities"),
        ("industrials", "sector_industrials"),
        ("real_estate", "sector_real_estate"),
        ("materials", "sector_materials"),
        ("communication", "sector_communication"),
    ):
        if _has_any(analysis_text, COMPILED_PATTERNS[key]):
            sector = sector_name
            evidence.append(f"sector:{sector_name}")
            break

    theme: Optional[str] = None
    for theme_name, key in (("robotics", "theme_robotics"), ("ai", "theme_ai"), ("esg", "theme_esg")):
        if _has_any(analysis_text, COMPILED_PATTERNS[key]):
            theme = theme_name
            evidence.append(f"theme:{theme_name}")
            break

    factor: Optional[str] = None
    for factor_name, key in (
        ("quality", "factor_quality"),
        ("momentum", "factor_momentum"),
        ("dividend_income", "factor_dividend"),
        ("minimum_volatility", "factor_min_vol"),
    ):
        if _has_any(analysis_text, COMPILED_PATTERNS[key]):
            factor = factor_name
            evidence.append(f"factor:{factor_name}")
            break

    equity_size: Optional[str] = None
    for size_name, key in (("small", "size_small"), ("mid", "size_mid"), ("large", "size_large")):
        if _has_any(analysis_text, COMPILED_PATTERNS[key]):
            equity_size = size_name
            evidence.append(f"size:{size_name}")
            break

    equity_style: Optional[str] = None
    if _has_any(analysis_text, COMPILED_PATTERNS["style_value"]):
        equity_style = "value"
        evidence.append("style:value")
    elif _has_any(analysis_text, COMPILED_PATTERNS["style_growth"]):
        equity_style = "growth"
        evidence.append("style:growth")

    geography_country, region_from_country = _match_country(analysis_text)
    geography_region: Optional[str] = None
    geography_scope: Optional[str] = None
    if geography_country:
        geography_region = region_from_country
        geography_scope = "country"
        evidence.append(f"country:{geography_country}")
    elif _has_any(analysis_text, COMPILED_PATTERNS["region_global"]):
        geography_region = "global"
        geography_scope = "global"
        evidence.append("region:global")
    elif _has_any(analysis_text, COMPILED_PATTERNS["region_north_america"]):
        geography_region = "north_america"
        geography_scope = "regional"
        evidence.append("region:north_america")
    elif _has_any(analysis_text, COMPILED_PATTERNS["region_em"]):
        geography_region = "em"
        geography_scope = "regional"
        evidence.append("region:em")
    elif _has_any(analysis_text, COMPILED_PATTERNS["region_europe"]):
        geography_region = "europe"
        geography_scope = "regional"
        evidence.append("region:europe")
    elif _has_any(analysis_text, COMPILED_PATTERNS["region_asia"]):
        geography_region = "asia"
        geography_scope = "regional"
        evidence.append("region:asia")

    equity_hint = metadata_asset_class == "equity" or _has_any(analysis_text, COMPILED_PATTERNS["equity_hint"]) or bool(
        geography_region or geography_country or sector or factor or equity_size or equity_style or theme
    )
    if original_name.upper().startswith("JPM-") and " EQ " in f" {analysis_text} ":
        equity_hint = True
        evidence.append("equity:jpm_eq_abbrev")

    asset_class = "unknown"
    if gold_flag == 1 or commodity_type:
        asset_class = "commodity"
    elif metadata_asset_class == "commodity":
        asset_class = "commodity"
    elif bond_flag:
        asset_class = "bond"
    elif metadata_asset_class == "cash" or cash_proxy_flag == 1:
        asset_class = "cash"
    elif multi_flag:
        asset_class = "multi"
    elif metadata_asset_class == "equity" or equity_hint or str(instrument_type or "").upper() == "ETF":
        asset_class = "equity"

    cash_flag = 1 if asset_class == "cash" else cash_proxy_flag

    if asset_class == "equity" and geography_scope is None:
        geography_scope = "sector" if sector else "thematic" if theme else "unknown"
        if geography_scope == "unknown":
            geography_region = "unknown"
        else:
            geography_region = geography_region or "global"

    if asset_class not in {"bond", "cash"}:
        duration_bucket = None
        low = None
        high = None
    if asset_class != "bond":
        govt_bond_flag = 0

    bond_type: Optional[str] = None
    if asset_class == "bond":
        if _has_any(analysis_text, COMPILED_PATTERNS["bond_type_linkers"]):
            bond_type = "linkers"
        elif govt_bond_flag == 1:
            bond_type = "govt"
        elif _has_any(analysis_text, COMPILED_PATTERNS["bond_type_aggregate"]):
            bond_type = "aggregate"
        elif _has_any(analysis_text, COMPILED_PATTERNS["bond_type_corp"]):
            bond_type = "corp"
        else:
            bond_type = "unknown"
        evidence.append(f"bond_type:{bond_type}")

    resolved_hedged_flag = int(hedged_flag) if hedged_flag in {0, 1} else None
    resolved_hedged_target = hedged_target
    final_hedged_flag = 0
    final_hedged_target: Optional[str] = None
    if resolved_hedged_flag is not None or resolved_hedged_target is not None:
        final_hedged_flag = resolved_hedged_flag if resolved_hedged_flag is not None else 1
        final_hedged_target = resolved_hedged_target
        evidence.append("profile:hedged")
        if final_hedged_target:
            evidence.append(f"hedged:{final_hedged_target}")
    else:
        for target, patterns in COMPILED_HEDGED_TARGETS:
            if _has_any(analysis_text, patterns):
                final_hedged_flag = 1
                final_hedged_target = target
                evidence.append(f"hedged:{target}")
                break
        if final_hedged_flag == 0 and "HEDGED" in analysis_text:
            final_hedged_flag = 1
            evidence.append("hedged:explicit")

    resolved_domicile_country = domicile_country or _domicile_country(isin)
    if resolved_domicile_country:
        evidence.append(f"domicile:{resolved_domicile_country}")
    if distribution_policy:
        evidence.append(f"distribution:{distribution_policy}")

    equity_region = _legacy_equity_region(geography_region, geography_country, sector) if asset_class == "equity" else None
    return TaxonomyResult(
        asset_class=asset_class,
        geography_scope=geography_scope,
        geography_region=geography_region,
        geography_country=geography_country,
        equity_size=equity_size,
        equity_style=equity_style,
        factor=factor,
        sector=sector,
        theme=theme,
        bond_type=bond_type,
        duration_bucket=duration_bucket,
        duration_years_low=low,
        duration_years_high=high,
        commodity_type="gold" if gold_flag == 1 else commodity_type,
        cash_proxy_flag=cash_proxy_flag,
        gold_flag=gold_flag,
        cash_flag=cash_flag,
        govt_bond_flag=govt_bond_flag,
        hedged_flag=final_hedged_flag,
        hedged_target=final_hedged_target,
        domicile_country=resolved_domicile_country,
        distribution_policy=distribution_policy,
        evidence_json=json.dumps({"rules": evidence, "normalized_name": analysis_text}, ensure_ascii=True),
        asset_bucket=asset_class,
        equity_region=equity_region,
    )


def ensure_taxonomy_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instrument_taxonomy(
            instrument_id INTEGER PRIMARY KEY,
            asset_class TEXT,
            geography_scope TEXT NULL,
            geography_region TEXT NULL,
            geography_country TEXT NULL,
            equity_size TEXT NULL,
            equity_style TEXT NULL,
            factor TEXT NULL,
            sector TEXT NULL,
            theme TEXT NULL,
            bond_type TEXT NULL,
            duration_bucket TEXT NULL,
            duration_years_low REAL NULL,
            duration_years_high REAL NULL,
            commodity_type TEXT NULL,
            cash_proxy_flag INTEGER DEFAULT 0,
            gold_flag INTEGER DEFAULT 0,
            cash_flag INTEGER DEFAULT 0,
            govt_bond_flag INTEGER DEFAULT 0,
            hedged_flag INTEGER DEFAULT 0,
            hedged_target TEXT NULL,
            domicile_country TEXT NULL,
            distribution_policy TEXT NULL,
            taxonomy_version TEXT,
            evidence_json TEXT NULL,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instrument_classification(
            instrument_id INTEGER PRIMARY KEY,
            asset_bucket TEXT,
            equity_region TEXT NULL,
            bond_type TEXT NULL,
            duration_bucket TEXT NULL,
            gold_flag INTEGER DEFAULT 0,
            cash_flag INTEGER DEFAULT 0,
            govt_bond_flag INTEGER DEFAULT 0,
            duration_years_low REAL NULL,
            duration_years_high REAL NULL,
            cash_proxy_flag INTEGER DEFAULT 0,
            classifier_version TEXT,
            updated_at TEXT
        )
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(instrument_classification)").fetchall()}
    for column_name, column_type, default_sql in (
        ("govt_bond_flag", "INTEGER", "0"),
        ("duration_years_low", "REAL", None),
        ("duration_years_high", "REAL", None),
        ("cash_proxy_flag", "INTEGER", "0"),
    ):
        if column_name in cols:
            continue
        default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
        conn.execute(f"ALTER TABLE instrument_classification ADD COLUMN {column_name} {column_type}{default_clause}")


def load_universe_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT
            i.instrument_id,
            i.isin,
            i.instrument_name,
            i.instrument_type,
            pp.distribution_policy,
            pp.benchmark_name,
            pp.asset_class_hint,
            pp.domicile_country,
            pp.replication_method,
            pp.hedged_flag,
            pp.hedged_target
        FROM instrument i
        JOIN listing l
          ON l.instrument_id = i.instrument_id
         AND COALESCE(l.primary_flag, 0) = 1
        LEFT JOIN product_profile pp
          ON pp.instrument_id = i.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
        GROUP BY
            i.instrument_id,
            i.isin,
            i.instrument_name,
            i.instrument_type,
            pp.distribution_policy,
            pp.benchmark_name,
            pp.asset_class_hint,
            pp.domicile_country,
            pp.replication_method,
            pp.hedged_flag,
            pp.hedged_target
        ORDER BY i.isin
        """
    ).fetchall()


def upsert_taxonomy(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> int:
    ts = now_utc_iso()
    updated = 0
    for row in rows:
        result = classify_instrument(
            isin=row["isin"],
            instrument_name=row["instrument_name"],
            instrument_type=row["instrument_type"],
            distribution_policy=row["distribution_policy"],
            benchmark_name=row["benchmark_name"],
            asset_class_hint=row["asset_class_hint"],
            replication_method=row["replication_method"],
            hedged_flag=row["hedged_flag"],
            hedged_target=row["hedged_target"],
            domicile_country=row["domicile_country"],
        )
        conn.execute(
            """
            INSERT INTO instrument_taxonomy(
                instrument_id, asset_class, geography_scope, geography_region, geography_country,
                equity_size, equity_style, factor, sector, theme, bond_type, duration_bucket,
                duration_years_low, duration_years_high, commodity_type, cash_proxy_flag, gold_flag,
                cash_flag, govt_bond_flag, hedged_flag, hedged_target, domicile_country,
                distribution_policy, taxonomy_version, evidence_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                asset_class=excluded.asset_class,
                geography_scope=excluded.geography_scope,
                geography_region=excluded.geography_region,
                geography_country=excluded.geography_country,
                equity_size=excluded.equity_size,
                equity_style=excluded.equity_style,
                factor=excluded.factor,
                sector=excluded.sector,
                theme=excluded.theme,
                bond_type=excluded.bond_type,
                duration_bucket=excluded.duration_bucket,
                duration_years_low=excluded.duration_years_low,
                duration_years_high=excluded.duration_years_high,
                commodity_type=excluded.commodity_type,
                cash_proxy_flag=excluded.cash_proxy_flag,
                gold_flag=excluded.gold_flag,
                cash_flag=excluded.cash_flag,
                govt_bond_flag=excluded.govt_bond_flag,
                hedged_flag=excluded.hedged_flag,
                hedged_target=excluded.hedged_target,
                domicile_country=excluded.domicile_country,
                distribution_policy=excluded.distribution_policy,
                taxonomy_version=excluded.taxonomy_version,
                evidence_json=excluded.evidence_json,
                updated_at=excluded.updated_at
            """,
            (
                row["instrument_id"], result.asset_class, result.geography_scope, result.geography_region, result.geography_country,
                result.equity_size, result.equity_style, result.factor, result.sector, result.theme, result.bond_type,
                result.duration_bucket, result.duration_years_low, result.duration_years_high, result.commodity_type,
                result.cash_proxy_flag, result.gold_flag, result.cash_flag, result.govt_bond_flag, result.hedged_flag,
                result.hedged_target, result.domicile_country, result.distribution_policy, TAXONOMY_VERSION,
                result.evidence_json, ts,
            ),
        )
        conn.execute(
            """
            INSERT INTO instrument_classification(
                instrument_id, asset_bucket, equity_region, bond_type, duration_bucket, gold_flag,
                cash_flag, govt_bond_flag, duration_years_low, duration_years_high, cash_proxy_flag,
                classifier_version, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                asset_bucket=excluded.asset_bucket,
                equity_region=excluded.equity_region,
                bond_type=excluded.bond_type,
                duration_bucket=excluded.duration_bucket,
                gold_flag=excluded.gold_flag,
                cash_flag=excluded.cash_flag,
                govt_bond_flag=excluded.govt_bond_flag,
                duration_years_low=excluded.duration_years_low,
                duration_years_high=excluded.duration_years_high,
                cash_proxy_flag=excluded.cash_proxy_flag,
                classifier_version=excluded.classifier_version,
                updated_at=excluded.updated_at
            """,
            (
                row["instrument_id"], result.asset_bucket, result.equity_region, result.bond_type, result.duration_bucket,
                result.gold_flag, result.cash_flag, result.govt_bond_flag, result.duration_years_low,
                result.duration_years_high, result.cash_proxy_flag, LEGACY_CLASSIFIER_VERSION, ts,
            ),
        )
        updated += 1
    return updated


def print_taxonomy_stats(conn: sqlite3.Connection) -> None:
    print("\n=== Taxonomy Coverage ===")
    total = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM instrument_taxonomy t
        JOIN instrument i ON i.instrument_id = t.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
        """
    ).fetchone()["c"]
    print(f"taxonomy rows: {total}")
    print("\nasset_class distribution:")
    for row in conn.execute(
        """
        SELECT COALESCE(asset_class, 'NULL') AS asset_class, COUNT(*) AS c
        FROM instrument_taxonomy t
        JOIN instrument i ON i.instrument_id = t.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
        GROUP BY COALESCE(asset_class, 'NULL')
        ORDER BY c DESC, asset_class
        """
    ):
        print(f"  {row['asset_class']}: {row['c']}")
    coverage = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN COALESCE(geography_region, 'unknown') <> 'unknown' OR geography_country IS NOT NULL THEN 1 ELSE 0 END) AS known
        FROM instrument_taxonomy t
        JOIN instrument i ON i.instrument_id = t.instrument_id
        WHERE COALESCE(i.universe_mvp_flag, 0) = 1
          AND t.asset_class = 'equity'
        """
    ).fetchone()
    total_equity = int(coverage["total"] or 0)
    known_equity = int(coverage["known"] or 0)
    pct = (100.0 * known_equity / total_equity) if total_equity else 0.0
    print(f"\nequity geography known: {known_equity}/{total_equity} ({pct:.2f}%)")
