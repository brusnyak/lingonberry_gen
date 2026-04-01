"""
Niche × location query matrix for daily lead scraping.

Usage:
    from scrapers.queries import get_daily_queries
    queries = get_daily_queries(n=8)  # returns 8 queries, rotating to avoid repeats
    queries = get_daily_queries(n=6, source="hipages")  # AU trades only
    queries = get_daily_queries(n=8, source="web_search")  # DDG queries
"""

import json
import os
from datetime import date
from pathlib import Path

_HERE = Path(__file__).parent

# EU/SK/CZ/AT — Google Maps queries
QUERY_MATRIX = [
    # UK Trades (Priority 1)
    "plumber london",
    "plumber manchester",
    "plumber birmingham",
    "plumber leeds",
    "electrician london",
    "electrician manchester",
    "electrician birmingham",
    "electrician leeds",
    "hvac london",
    "hvac manchester",
    "air conditioning london",
    "air conditioning manchester",
    "roofer london",
    "roofer manchester",
    "carpenter london",
    "carpenter manchester",
    # Dental / medical
    "dentist vienna",
    "dentist bratislava",
    "dentist prague",
    "dental clinic vienna",
    "dental clinic bratislava",
    "dental clinic prague",
    "medical clinic vienna",
    "medical clinic bratislava",
    "medical clinic prague",
    "physiotherapist vienna",
    "physiotherapist bratislava",
    "physiotherapist prague",
    # Real estate
    "real estate agent vienna",
    "real estate agent bratislava",
    "real estate agent prague",
    "property agency vienna",
    "property agency bratislava",
    "property agency prague",
    "realtor vienna",
    "realtor bratislava",
    # Accounting / finance
    "accounting firm vienna",
    "accounting firm bratislava",
    "accounting firm prague",
    "accountant vienna",
    "accountant bratislava",
    "accountant prague",
    "tax advisor vienna",
    "tax advisor bratislava",
    "tax advisor prague",
    "steuerberater wien",
    "ucetni firma praha",
    # Barbershops / salons
    "barbershop vienna",
    "barbershop bratislava",
    "barbershop prague",
    "hair salon vienna",
    "hair salon bratislava",
    "hair salon prague",
    "beauty salon vienna",
    "beauty salon bratislava",
    "nail salon vienna",
    "nail salon prague",
    # Local e-commerce / retail
    "local boutique vienna",
    "local boutique prague",
    "florist vienna",
    "florist bratislava",
    "florist prague",
    "jewellery shop vienna",
    "jewellery shop prague",
    "gift shop vienna",
    "gift shop bratislava",
]

# AU trades — hipages scraper queries: (trade_key, location)
AU_HIPAGES_MATRIX: list[tuple[str, str]] = [
    ("plumber", "sydney"),
    ("plumber", "melbourne"),
    ("plumber", "brisbane"),
    ("plumber", "perth"),
    ("electrician", "sydney"),
    ("electrician", "melbourne"),
    ("electrician", "brisbane"),
    ("electrician", "perth"),
    ("hvac", "sydney"),
    ("hvac", "melbourne"),
    ("hvac", "brisbane"),
    ("locksmith", "sydney"),
    ("locksmith", "melbourne"),
    ("locksmith", "brisbane"),
    ("carpenter", "sydney"),
    ("carpenter", "melbourne"),
    ("painter", "sydney"),
    ("painter", "melbourne"),
    ("tiler", "sydney"),
    ("tiler", "brisbane"),
]

# DDG web search queries — broader, catches businesses without Maps presence
# Keep queries clean — no "contact email" spam triggers, just trade + location
WEB_SEARCH_MATRIX: list[tuple[str, str]] = [
    # AU trades
    ("plumber sydney", "plumber"),
    ("electrician melbourne", "electrician"),
    ("plumber brisbane", "plumber"),
    ("hvac sydney air conditioning", "hvac"),
    ("locksmith perth 24 hour", "locksmith"),
    ("plumber perth", "plumber"),
    ("electrician brisbane", "electrician"),
    ("carpenter sydney", "carpenter"),
    # EU niches
    ("dentist bratislava", "dental"),
    ("real estate agent vienna", "real_estate"),
    ("accountant prague", "accounting"),
    ("physiotherapist vienna", "physiotherapy"),
    ("hair salon bratislava", "beauty"),
    ("dental clinic prague", "dental"),
    ("steuerberater wien", "accounting"),
    ("immobilienmakler wien", "real_estate"),
]

# Facebook search queries — finds businesses with FB presence but no website
FACEBOOK_MATRIX: list[tuple[str, str]] = [
    ("plumber sydney", "plumber"),
    ("electrician melbourne", "electrician"),
    ("plumber brisbane", "plumber"),
    ("dentist bratislava", "dental"),
    ("real estate agent vienna", "real_estate"),
    ("accountant prague", "accounting"),
    ("hair salon bratislava", "beauty"),
    ("physiotherapist vienna", "physiotherapy"),
]

_STATE_FILE = _HERE.parent / "data" / "query_rotation.json"


def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_index": 0, "hipages_index": 0, "web_search_index": 0, "facebook_index": 0, "last_run": ""}


def _save_state(state: dict) -> None:
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps(state, indent=2))


def get_daily_queries(n: int = 8, source: str = "google_maps") -> list:
    """
    Return n queries from the appropriate matrix, rotating from where we left off.
    Saves position so each run picks up where the last one ended.

    Args:
        n: number of queries to return
        source: "google_maps" | "hipages" | "web_search" | "facebook"

    Returns:
        For google_maps/web_search: list[str]
        For hipages: list[tuple[str, str]] — (trade, location)
        For facebook: list[tuple[str, str]] — (query, category)
    """
    state = _load_state()

    if source == "google_maps":
        key = "last_index"
        matrix = QUERY_MATRIX
    elif source == "hipages":
        key = "hipages_index"
        matrix = AU_HIPAGES_MATRIX
    elif source == "web_search":
        key = "web_search_index"
        matrix = WEB_SEARCH_MATRIX
    elif source == "facebook":
        key = "facebook_index"
        matrix = FACEBOOK_MATRIX
    else:
        raise ValueError(f"Unknown source: {source}")

    idx = state.get(key, 0) % len(matrix)
    queries = []
    for i in range(n):
        queries.append(matrix[(idx + i) % len(matrix)])
    state[key] = (idx + n) % len(matrix)
    state["last_run"] = date.today().isoformat()
    _save_state(state)
    return queries
