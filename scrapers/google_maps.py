import hashlib
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def _clean(text: Optional[str]) -> str:
    return (text or "").strip()


def _extract_rating(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"([0-9]+\.?[0-9]*)", text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _extract_reviews(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"([0-9][0-9,]*)", text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _safe_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _get_detail_field(page, data_item_id: str) -> str:
    try:
        el = page.locator(f"[data-item-id='{data_item_id}']")
        if el.count() == 0:
            return ""
        return _clean(el.first.inner_text())
    except Exception:
        return ""


def _get_name(page) -> str:
    try:
        h1 = page.locator("h1")
        if h1.count() == 0:
            return ""
        return _clean(h1.first.inner_text())
    except Exception:
        return ""


def _get_category(page) -> str:
    selectors = [
        "button[jsaction*='pane.rating.category']",
        "button[aria-label^='Category']",
        "div[aria-label^='Category']",
        "button[data-item-id='category']",
    ]
    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                return _clean(el.first.inner_text())
        except Exception:
            continue
    return ""


def _get_rating_and_reviews(page) -> Tuple[Optional[float], Optional[int]]:
    rating = None
    reviews = None

    try:
        rating_el = page.locator("span[aria-label*='stars']")
        if rating_el.count():
            rating = _extract_rating(rating_el.first.get_attribute("aria-label") or "")
    except Exception:
        pass

    try:
        reviews_el = page.locator("button[aria-label*='reviews']")
        if reviews_el.count():
            reviews = _extract_reviews(reviews_el.first.get_attribute("aria-label") or "")
    except Exception:
        pass

    return rating, reviews


def _get_website(page) -> str:
    text = _get_detail_field(page, "authority")
    if text.startswith("http"):
        return text
    try:
        link = page.locator("a[data-item-id='authority']")
        if link.count() > 0:
            href = link.first.get_attribute("href")
            return _clean(href)
    except Exception:
        pass
    return _clean(text)


def _get_place_id(page) -> Optional[str]:
    # Google Maps URL often includes place_id in the query string.
    url = page.url or ""
    m = re.search(r"1s([A-Za-z0-9_-]+)", url)
    if m:
        return m.group(1)
    return None


def _human_delay(min_s: float, max_s: float) -> None:
    if max_s <= 0:
        return
    time.sleep(random.uniform(min_s, max_s))


def scrape_google_maps(
    query: str,
    max_results: int = 50,
    headless: bool = True,
    slow_mo: int = 0,
    min_delay_s: float = 0.8,
    max_delay_s: float = 2.0,
    user_data_dir: Optional[str] = None,
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    seen = set()

    with sync_playwright() as p:
        launch_args = ["--disable-blink-features=AutomationControlled"]
        if user_data_dir:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                slow_mo=slow_mo,
                args=launch_args,
            )
            page = context.new_page()
        else:
            browser = p.chromium.launch(headless=headless, slow_mo=slow_mo, args=launch_args)
            page = browser.new_page()

        page.goto("https://www.google.com/maps", wait_until="domcontentloaded")

        page.wait_for_selector("input#searchboxinput", timeout=20000)
        page.fill("input#searchboxinput", query)
        page.click("button#searchbox-searchbutton")

        page.wait_for_selector("div[role='feed']", timeout=20000)
        feed = page.locator("div[role='feed']")

        last_count = 0
        same_count_rounds = 0

        while len(results) < max_results:
            cards = feed.locator("div[role='article']")
            card_count = cards.count()
            if card_count == 0:
                break

            for i in range(card_count):
                if len(results) >= max_results:
                    break
                card = cards.nth(i)
                try:
                    card.click()
                except Exception:
                    continue

                try:
                    page.wait_for_selector("h1", timeout=10000)
                except PlaywrightTimeoutError:
                    continue

                name = _get_name(page)
                category = _get_category(page)
                rating, reviews_count = _get_rating_and_reviews(page)
                address = _get_detail_field(page, "address")
                phone = _get_detail_field(page, "phone")
                website = _get_website(page)
                maps_url = page.url
                place_id = _get_place_id(page) or _safe_hash("|".join([name, address, maps_url]))

                if place_id in seen:
                    continue
                seen.add(place_id)

                results.append({
                    "place_id": place_id,
                    "name": name,
                    "category": category,
                    "rating": rating,
                    "reviews_count": reviews_count,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "maps_url": maps_url,
                    "query": query,
                    "collected_at": datetime.utcnow().isoformat(),
                })

                _human_delay(min_delay_s, max_delay_s)

            if card_count == last_count:
                same_count_rounds += 1
            else:
                same_count_rounds = 0
            last_count = card_count

            if same_count_rounds >= 3:
                break

            try:
                page.evaluate("(feed) => { feed.scrollBy(0, feed.scrollHeight); }", feed)
            except Exception:
                break

            _human_delay(min_delay_s, max_delay_s)

        if user_data_dir:
            context.close()
        else:
            browser.close()

    return results
