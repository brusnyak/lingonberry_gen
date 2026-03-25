"""
leadgen/scrapers/hipages.py

Scrapes hipages.com.au for AU trades leads.
Returns same shape as google_maps scraper so it feeds the same pipeline.

hipages profile pages expose:
  - business name
  - trade category
  - suburb/state
  - phone (sometimes)
  - website (sometimes)
  - review count + rating
  - owner first name (often in "About" blurb)

No API key needed — public pages, Playwright with human delays.
"""
from __future__ import annotations

import re
import time
import random
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

BASE_URL = "https://www.hipages.com.au"

# Trade category slugs on hipages
TRADE_SLUGS = {
    "plumber": "plumbers",
    "electrician": "electricians",
    "hvac": "air-conditioning",
    "locksmith": "locksmiths",
    "carpenter": "carpenters",
    "painter": "painters",
    "tiler": "tilers",
    "landscaper": "landscapers",
    "cleaner": "cleaners",
    "builder": "builders",
}

_GENERIC_EMAILS = {
    "info", "contact", "hello", "admin", "support", "office", "mail",
    "team", "sales", "enquiries", "noreply", "reception", "booking",
}


def _human_delay(min_s: float = 0.8, max_s: float = 2.2) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _extract_email_from_text(text: str) -> str:
    matches = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    for m in matches:
        prefix = m.split("@")[0].lower()
        if prefix not in _GENERIC_EMAILS:
            return m
    return matches[0] if matches else ""


def _extract_name_from_about(text: str) -> str:
    patterns = [
        re.compile(r"(?:hi,?\s+)?i'?m\s+([A-Z][a-z]{2,15})\b", re.I),
        re.compile(r"my name is\s+([A-Z][a-z]{2,15})\b", re.I),
        re.compile(r"^[-–]\s*([A-Z][a-z]{2,15}),\s+\w", re.M),
        re.compile(r"(?:owner|founder|director|manager)[:\s]+([A-Z][a-z]{2,15})\b", re.I),
    ]
    skip = {"the", "and", "for", "our", "your", "this", "that", "with", "from",
            "call", "email", "free", "best", "top", "pro", "ltd", "pty"}
    for p in patterns:
        m = p.search(text)
        if m:
            candidate = m.group(1).strip().capitalize()
            if candidate.lower() not in skip and len(candidate) >= 3:
                return candidate
    return ""


def _parse_profile_page(page: Page, url: str) -> Optional[dict]:
    """Extract lead data from a single hipages business profile page."""
    try:
        page.goto(url, timeout=20000, wait_until="domcontentloaded")
        _human_delay(0.6, 1.4)
    except PWTimeout:
        return None

    try:
        name = page.locator("h1").first.inner_text(timeout=5000).strip()
    except Exception:
        return None

    if not name:
        return None

    # Category / trade
    category = ""
    try:
        cat_el = page.locator("[data-testid='business-category'], .business-category, h2").first
        category = cat_el.inner_text(timeout=3000).strip()
    except Exception:
        pass

    # Address / suburb
    address = ""
    try:
        addr_el = page.locator("[data-testid='business-location'], .location, address").first
        address = addr_el.inner_text(timeout=3000).strip()
    except Exception:
        pass

    # Rating + review count
    rating = 0.0
    reviews_count = 0
    try:
        rating_text = page.locator("[data-testid='rating'], .rating-score, .star-rating").first.inner_text(timeout=3000)
        rating = float(re.search(r"[\d.]+", rating_text).group())
    except Exception:
        pass
    try:
        rev_text = page.locator("[data-testid='review-count'], .review-count").first.inner_text(timeout=3000)
        reviews_count = int(re.search(r"\d+", rev_text).group())
    except Exception:
        pass

    # Website
    website = ""
    try:
        website_el = page.locator("a[href*='http']:not([href*='hipages'])").first
        website = website_el.get_attribute("href", timeout=3000) or ""
        if website and not website.startswith("http"):
            website = ""
    except Exception:
        pass

    # Phone (often hidden behind "Show number" button — try clicking)
    phone = ""
    try:
        show_btn = page.locator("button:has-text('Show'), button:has-text('Call')").first
        show_btn.click(timeout=3000)
        _human_delay(0.4, 0.8)
        phone_el = page.locator("a[href^='tel:']").first
        phone = phone_el.inner_text(timeout=3000).strip()
    except Exception:
        try:
            phone_el = page.locator("a[href^='tel:']").first
            phone = phone_el.inner_text(timeout=3000).strip()
        except Exception:
            pass

    # About text (for name extraction + pain signals)
    about_text = ""
    try:
        about_el = page.locator("[data-testid='about-section'], .about-section, .business-description").first
        about_text = about_el.inner_text(timeout=4000).strip()
    except Exception:
        pass

    # Contact name from about text
    contact_name = _extract_name_from_about(about_text)

    # Email from page text (rare on hipages but worth trying)
    email = _extract_email_from_text(page.content())

    place_id = f"hipages_{re.sub(r'[^a-z0-9]', '_', name.lower()[:40])}_{re.sub(r'[^a-z0-9]', '', address.lower()[:20])}"

    return {
        "place_id": place_id,
        "name": name,
        "category": category or "trades",
        "rating": rating,
        "reviews_count": reviews_count,
        "address": address,
        "phone": phone,
        "website": website,
        "email_maps": email,
        "hours": "",
        "maps_url": url,
        "query": f"hipages:{category}",
        "collected_at": datetime.utcnow().isoformat(),
        "contact_name": contact_name,
        "about_text": about_text,
        "_source": "hipages",
    }


def _get_listing_urls(page: Page, trade_slug: str, location: str, max_results: int = 20) -> list[str]:
    """Scrape the search results page for profile URLs."""
    search_url = f"{BASE_URL}/connect/{trade_slug}/{location.lower().replace(' ', '-')}"
    try:
        page.goto(search_url, timeout=20000, wait_until="domcontentloaded")
        _human_delay(1.0, 2.0)
    except PWTimeout:
        return []

    urls = []
    # Scroll to load more results
    for _ in range(3):
        page.evaluate("window.scrollBy(0, window.innerHeight)")
        _human_delay(0.5, 1.0)

    links = page.locator("a[href*='/connect/']").all()
    for link in links:
        href = link.get_attribute("href") or ""
        if href and "/connect/" in href and href.count("/") >= 3:
            full = href if href.startswith("http") else BASE_URL + href
            if full not in urls:
                urls.append(full)
        if len(urls) >= max_results:
            break

    return urls


def scrape_hipages(
    trade: str,
    location: str,
    max_results: int = 20,
    headless: bool = True,
) -> list[dict]:
    """
    Scrape hipages.com.au for a given trade + location.
    Returns list of lead dicts compatible with the main pipeline.

    Args:
        trade: trade key from TRADE_SLUGS (e.g. "plumber") or raw slug
        location: suburb or city (e.g. "sydney", "melbourne")
        max_results: max profiles to scrape
        headless: run browser headless
    """
    trade_slug = TRADE_SLUGS.get(trade.lower(), trade.lower())
    leads = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        print(f"[hipages] Searching: {trade_slug} in {location}")
        urls = _get_listing_urls(page, trade_slug, location, max_results)
        print(f"[hipages] Found {len(urls)} profile URLs")

        for i, url in enumerate(urls[:max_results]):
            _human_delay(1.0, 2.5)
            lead = _parse_profile_page(page, url)
            if lead:
                leads.append(lead)
                print(f"[hipages] {i+1}/{len(urls)} — {lead['name']}")
            else:
                print(f"[hipages] {i+1}/{len(urls)} — skipped (parse failed)")

        browser.close()

    return leads
