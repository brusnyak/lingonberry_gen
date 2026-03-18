"""
Niche × location query matrix for daily lead scraping.

Usage:
    from scrapers.queries import get_daily_queries
    queries = get_daily_queries(n=8)  # returns 8 queries, rotating to avoid repeats
"""

import json
import os
from datetime import date
from pathlib import Path

_HERE = Path(__file__).parent

# Full matrix — niche × location
QUERY_MATRIX = [
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

_STATE_FILE = _HERE.parent / "data" / "query_rotation.json"


def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_index": 0, "last_run": ""}


def _save_state(state: dict) -> None:
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps(state, indent=2))


def get_daily_queries(n: int = 8) -> list[str]:
    """
    Return n queries from the matrix, rotating from where we left off.
    Saves position so each run picks up where the last one ended.
    """
    state = _load_state()
    idx = state.get("last_index", 0) % len(QUERY_MATRIX)
    queries = []
    for i in range(n):
        queries.append(QUERY_MATRIX[(idx + i) % len(QUERY_MATRIX)])
    state["last_index"] = (idx + n) % len(QUERY_MATRIX)
    state["last_run"] = date.today().isoformat()
    _save_state(state)
    return queries
