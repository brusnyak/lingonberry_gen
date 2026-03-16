import hashlib
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# - helpers -

def _clean(text):
    if not text:
        return ""
    # strip Maps icon chars (private-use unicode block e000-f8ff)
    text = re.sub(r"[\ue000-\uf8ff]", "", text)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _safe_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

def _human_delay(min_s, max_s):
    if max_s > 0:
        time.sleep(random.uniform(min_s, max_s))

def _debug_screenshot(page, label):
    try:
        Path("data/debug").mkdir(parents=True, exist_ok=True)
        page.screenshot(path=f"data/debug/{label}_{int(time.time())}.png", full_page=True)
    except Exception:
        pass

def _hide_webdriver(page):
    """Mask navigator.webdriver to suppress the automation banner."""
    try:
        page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "window.chrome={runtime:{}};"
        )
    except Exception:
        pass

# - junk filter -

_JUNK_NAMES = {
    "results", "výsledky", "search results",
    "sponzorované", "sponsored", "advertisement", "reklama",
}

# - field extractors -

def _get_name(page):
    for sel in ["h1.DUwDvf", "h1[class*='fontHeadlineLarge']", "h1"]:
        try:
            el = page.locator(sel)
            if el.count() == 0:
                continue
            name = _clean(el.first.inner_text())
            if name and name.lower() not in _JUNK_NAMES:
                return name
        except Exception:
            continue
    return ""

_CATEGORY_JUNK = {"reviews aren", "google checks", "learn more", "not verified"}

def _get_category(page):
    for sel in [
        "button[jsaction*='pane.rating.category']",
        "button[aria-label^='Category']",
        "button[data-item-id='category']",
        "div.fontBodyMedium span",
    ]:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                text = _clean(el.first.inner_text())
                if text and not any(j in text.lower() for j in _CATEGORY_JUNK):
                    return text
        except Exception:
            continue
    return ""

def _get_rating_reviews(page):
    rating = None
    reviews = None
    try:
        el = page.locator("span[aria-label*='stars'], span[aria-label*='hviezd']")
        if el.count():
            m = re.search(r"([\d.]+)", el.first.get_attribute("aria-label") or "")
            if m:
                rating = float(m.group(1))
    except Exception:
        pass
    try:
        el = page.locator("button[aria-label*='reviews'], button[aria-label*='recenzií']")
        if el.count():
            m = re.search(r"([\d,]+)", el.first.get_attribute("aria-label") or "")
            if m:
                reviews = int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return rating, reviews

def _get_website(page):
    for sel in [
        "a[data-item-id='authority']",
        "a[aria-label*='Website']",
        "a[aria-label*='Webová stránka']",
        "a[aria-label*='Webové stránky']",
    ]:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                href = el.first.get_attribute("href") or ""
                if href.startswith("http"):
                    return _clean(href)
                text = _clean(el.first.inner_text())
                if text.startswith("http"):
                    return text
        except Exception:
            continue
    return ""

def _get_phone(page):
    for sel in [
        "[data-item-id='phone']",
        "button[aria-label*='Phone']",
        "button[aria-label*='Telefón']",
        "div[aria-label*='Phone']",
    ]:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                t = _clean(el.first.inner_text())
                if t:
                    return t
        except Exception:
            continue
    return ""

def _get_address(page):
    for sel in [
        "button[data-item-id='address']",
        "[data-item-id='address']",
        "button[aria-label*='Address']",
        "button[aria-label*='Adresa']",
    ]:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                t = _clean(el.first.inner_text())
                if t:
                    return t
        except Exception:
            continue
    return ""

def _get_email_from_panel(page):
    EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
    try:
        panel = page.locator("div[role='main']")
        text = _clean(panel.inner_text()) if panel.count() > 0 else ""
        emails = [e for e in EMAIL_RE.findall(text)
                  if "google" not in e.lower() and "gstatic" not in e.lower()]
        if emails:
            return emails[0]
    except Exception:
        pass
    return ""

def _get_place_id_from_url(url):
    m = re.search(r"place/[^/]+/([^/?@]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"1s([A-Za-z0-9_\-:]+)", url)
    if m:
        return m.group(1)
    return None

# - about tab -

_ABOUT_LABELS = ["About", "Informácie", "Info", "Über"]

def _extract_about_tab(page):
    result = {"email_maps": "", "hours": ""}
    for label in _ABOUT_LABELS:
        for sel in [
            f"button[role='tab']:has-text('{label}')",
            f"div[role='tab']:has-text('{label}')",
        ]:
            try:
                el = page.locator(sel)
                if el.count() > 0:
                    el.first.click(timeout=2000)
                    page.wait_for_timeout(700)
                    panel_text = _clean(page.locator("div[role='main']").inner_text())
                    EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
                    emails = [e for e in EMAIL_RE.findall(panel_text)
                              if "google" not in e.lower() and "gstatic" not in e.lower()]
                    if emails:
                        result["email_maps"] = emails[0]
                    hm = re.search(
                        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|"
                        r"Pondelok|Utorok|Streda|Štvrtok|Piatok|Sobota|Nedeľa)[^\n]{0,200}",
                        panel_text, re.I
                    )
                    if hm:
                        result["hours"] = hm.group(0)[:300]
                    return result
            except Exception:
                continue
    return result

# - wait helpers -

def _wait_for_detail_panel(page, timeout_ms=12000):
    for sel in [
        "h1.DUwDvf",
        "h1[class*='fontHeadlineLarge']",
        "button[data-item-id='address']",
        "a[data-item-id='authority']",
        "button[data-item-id='phone']",
    ]:
        try:
            page.wait_for_selector(sel, timeout=timeout_ms)
            return True
        except PlaywrightTimeoutError:
            continue
    return False

def _accept_consent(page):
    for sel in [
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "form [type='submit']:has-text('I agree')",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                return
        except Exception:
            continue

# - phase 1: collect card URLs by scrolling feed -

def _collect_card_urls(feed, max_needed):
    """
    Scroll the results feed and harvest href links from cards.
    Returns a deduplicated list of Maps place URLs.
    Separating URL collection from data extraction avoids stale element refs.
    """
    urls = []
    seen_urls = set()
    stale_rounds = 0
    last_count = 0

    while len(urls) < max_needed * 2:  # collect extra to account for dupes/skips
        cards = feed.locator("div[role='article'] a.hfpxzc")
        count = cards.count()

        for i in range(count):
            try:
                href = cards.nth(i).get_attribute("href") or ""
                if href and href not in seen_urls and "/place/" in href:
                    seen_urls.add(href)
                    urls.append(href)
            except Exception:
                continue

        if count == last_count:
            stale_rounds += 1
        else:
            stale_rounds = 0
        last_count = count

        if stale_rounds >= 3:
            break

        try:
            feed.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
        except Exception:
            break
        time.sleep(1.0)

    return urls

# - main scraper -

def scrape_google_maps(
    query,
    max_results=50,
    headless=True,
    slow_mo=0,
    min_delay_s=1.0,
    max_delay_s=2.5,
    user_data_dir=None,
    max_retries=2,
    review_mode=False,
):
    results = []
    seen = set()

    with sync_playwright() as p:
        if user_data_dir:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                slow_mo=slow_mo,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
            )
        else:
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=slow_mo,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            )

        page = context.new_page()
        _hide_webdriver(page)

        # - load search results -
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}?hl=en&gl=en"
        loaded = False

        for attempt in range(1, max_retries + 2):
            page.goto(search_url, wait_until="domcontentloaded")
            _accept_consent(page)
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            try:
                page.wait_for_selector("div[role='feed']", timeout=20000)
                loaded = True
                break
            except PlaywrightTimeoutError:
                if attempt > max_retries:
                    _debug_screenshot(page, "maps_no_feed")
                    print(f"[maps] ERROR: feed not found for: {query}")
                    context.close()
                    return results
                _human_delay(2.0, 4.0)

        if not loaded:
            context.close()
            return results

        feed = page.locator("div[role='feed']")

        # - phase 1: scroll feed, collect all card URLs -
        print(f"[maps] Scrolling feed to collect URLs (target: {max_results})...")
        card_urls = _collect_card_urls(feed, max_results)
        print(f"[maps] Collected {len(card_urls)} URLs — visiting each listing...")

        # - phase 2: visit each URL, extract all fields -
        for url in card_urls:
            if len(results) >= max_results:
                break

            try:
                page.goto(url, wait_until="domcontentloaded")
            except Exception as e:
                print(f"[maps] nav error: {e}")
                continue

            if not _wait_for_detail_panel(page, timeout_ms=12000):
                _debug_screenshot(page, "maps_no_detail")
                continue

            _human_delay(0.3, 0.6)

            name = _get_name(page)
            if not name:
                continue

            category        = _get_category(page)
            rating, reviews = _get_rating_reviews(page)
            address         = _get_address(page)
            phone           = _get_phone(page)
            website         = _get_website(page)
            maps_url        = page.url
            email_panel     = _get_email_from_panel(page)

            about_data  = _extract_about_tab(page)
            email_maps  = about_data["email_maps"] or email_panel
            hours       = about_data["hours"]

            place_id = _get_place_id_from_url(maps_url) or _safe_hash("|".join([name, address]))
            if place_id in seen:
                continue
            seen.add(place_id)

            if review_mode:
                print(f"[review] {name} | {category} | {address}")
                inp = input("Enter to accept, 's' to skip: ").strip().lower()
                if inp == "s":
                    continue

            results.append({
                "place_id":      place_id,
                "name":          name,
                "category":      category,
                "rating":        rating,
                "reviews_count": reviews,
                "address":       address,
                "phone":         phone,
                "website":       website,
                "email_maps":    email_maps,
                "hours":         hours,
                "maps_url":      maps_url,
                "query":         query,
                "collected_at":  datetime.utcnow().isoformat(),
            })

            print(
                f"[maps] {len(results):>3}. {name} | {category} | "
                f"{phone or '-'} | {website or '-'} | {email_maps or '-'}"
            )
            _human_delay(min_delay_s, max_delay_s)

        context.close()

    print(f"[maps] Done. Collected {len(results)} leads.")
    return results
