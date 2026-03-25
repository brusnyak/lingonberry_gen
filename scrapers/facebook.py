"""
leadgen/scrapers/facebook.py

Facebook business page scraper via public search.
Uses DDG to find Facebook business pages, then scrapes public page data.

Strategy:
  1. DDG search: site:facebook.com/pages "[trade] [city]"
  2. For each result, scrape the public FB page for:
     - business name
     - category
     - address / location
     - phone
     - website link
     - about text (for name extraction)
     - review count + rating (if visible)

No login required — public pages only.
Playwright with human delays to avoid rate limiting.
"""
from __future__ import annotations

import re
import time
import random
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse, quote_plus

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

_DDG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

_GENERIC_EMAILS = {
    "info", "contact", "hello", "admin", "support", "office", "mail",
    "team", "sales", "enquiries", "noreply", "reception", "booking",
}


def _human_delay(min_s: float = 1.0, max_s: float = 2.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _find_fb_pages_via_ddg(query: str, max_results: int = 10) -> list[str]:
    """
    Use DDG to find Facebook business page URLs for a given query.
    Returns list of facebook.com/... URLs.
    """
    search_query = f'site:facebook.com "{query}"'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"

    try:
        resp = requests.get(url, headers=_DDG_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[facebook] DDG search failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []

    for result in soup.select(".result"):
        title_el = result.select_one(".result__a")
        if not title_el:
            continue
        href = title_el.get("href", "")
        if "uddg=" in href:
            from urllib.parse import unquote
            match = re.search(r"uddg=([^&]+)", href)
            if match:
                href = unquote(match.group(1))
            else:
                continue

        if "facebook.com" not in href:
            continue
        # Only business pages, not profiles/groups/events
        if any(x in href for x in ["/groups/", "/events/", "/photos/", "/videos/"]):
            continue
        if href not in urls:
            urls.append(href)
        if len(urls) >= max_results:
            break

    return urls


def _scrape_fb_page(page: Page, url: str) -> Optional[dict]:
    """
    Scrape a public Facebook business page.
    Returns lead dict or None if page is inaccessible / not a business.
    """
    # Normalize URL — use /about for more structured data
    base_url = re.sub(r"\?.*$", "", url.rstrip("/"))
    about_url = base_url + "/about"

    try:
        page.goto(about_url, timeout=20000, wait_until="domcontentloaded")
        _human_delay(1.5, 3.0)
    except PWTimeout:
        return None

    # Check for login wall — if present, try the base page
    content = page.content()
    if "log in" in content.lower() and "create new account" in content.lower():
        try:
            page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
            _human_delay(1.0, 2.0)
            content = page.content()
        except PWTimeout:
            return None

    soup = BeautifulSoup(content, "html.parser")

    # Business name — try multiple selectors
    name = ""
    for selector in ["h1", "[data-testid='page-title']", "title"]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            # Strip " | Facebook" suffix
            text = re.sub(r"\s*[|\-–]\s*Facebook.*$", "", text, flags=re.I).strip()
            if text and len(text) > 2:
                name = text
                break

    if not name:
        return None

    # Extract structured data from page text
    page_text = soup.get_text(" ", strip=True)

    # Phone
    phone = ""
    phone_match = re.search(r"(\+?[\d\s\-().]{8,18})", page_text)
    if phone_match:
        phone = phone_match.group(1).strip()

    # Website
    website = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "facebook.com" not in href and "fbclid" not in href:
            website = href.split("?")[0]
            break

    # Address — look for structured address patterns
    address = ""
    addr_patterns = [
        r"\d+\s+[A-Z][a-z]+\s+(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Lane|Ln)",
        r"[A-Z][a-z]+,\s+(?:NSW|VIC|QLD|WA|SA|NT|ACT|TAS)",
        r"[A-Z][a-z]+,\s+(?:Vienna|Bratislava|Prague|Wien)",
    ]
    for pattern in addr_patterns:
        m = re.search(pattern, page_text)
        if m:
            address = m.group(0)
            break

    # Category
    category = ""
    cat_patterns = [
        r"(?:Category|Type)[:\s]+([A-Za-z\s&/]{3,40}?)(?:\n|\.|\|)",
        r"(?:Local Business|Service|Company)[:\s]+([A-Za-z\s&/]{3,40}?)(?:\n|\.|\|)",
    ]
    for pattern in cat_patterns:
        m = re.search(pattern, page_text, re.I)
        if m:
            category = m.group(1).strip()
            break

    # About text for name extraction
    about_text = ""
    about_section = soup.find("div", {"data-testid": "about-section"}) or soup.find("div", id=re.compile("about", re.I))
    if about_section:
        about_text = about_section.get_text(" ", strip=True)[:1000]

    # Rating + reviews
    rating = 0.0
    reviews_count = 0
    rating_match = re.search(r"([\d.]+)\s*(?:out of 5|stars?|rating)", page_text, re.I)
    if rating_match:
        try:
            rating = float(rating_match.group(1))
        except ValueError:
            pass
    reviews_match = re.search(r"([\d,]+)\s*(?:reviews?|ratings?)", page_text, re.I)
    if reviews_match:
        try:
            reviews_count = int(reviews_match.group(1).replace(",", ""))
        except ValueError:
            pass

    # Email from page
    email = ""
    email_matches = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", page_text)
    for m in email_matches:
        prefix = m.split("@")[0].lower()
        if prefix not in _GENERIC_EMAILS:
            email = m
            break
    if not email and email_matches:
        email = email_matches[0]

    place_id = f"fb_{re.sub(r'[^a-z0-9]', '_', name.lower()[:40])}"

    return {
        "place_id": place_id,
        "name": name,
        "category": category or "business",
        "rating": rating,
        "reviews_count": reviews_count,
        "address": address,
        "phone": phone,
        "website": website,
        "email_maps": email,
        "hours": "",
        "maps_url": url,
        "query": f"facebook:{name}",
        "collected_at": datetime.utcnow().isoformat(),
        "about_text": about_text,
        "_source": "facebook",
    }


def scrape_facebook(
    query: str,
    max_results: int = 15,
    headless: bool = True,
    category: str = "",
) -> list[dict]:
    """
    Find and scrape Facebook business pages matching a query.

    Args:
        query: search query e.g. "plumber sydney"
        max_results: max leads to return
        headless: run browser headless
        category: niche label to tag leads with
    """
    print(f"[facebook] Searching for: {query}")
    fb_urls = _find_fb_pages_via_ddg(query, max_results=max_results * 2)
    print(f"[facebook] Found {len(fb_urls)} Facebook page URLs")

    if not fb_urls:
        return []

    leads = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        for i, url in enumerate(fb_urls[:max_results]):
            _human_delay(2.0, 4.0)
            lead = _scrape_fb_page(page, url)
            if lead:
                if category:
                    lead["category"] = category
                leads.append(lead)
                print(f"[facebook] {i+1}/{len(fb_urls)} — {lead['name']}")
            else:
                print(f"[facebook] {i+1}/{len(fb_urls)} — skipped")

        browser.close()

    return leads
