import hashlib
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def _clean(text: Optional[str]) -> str:
    return (text or "").strip()


def _safe_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).digest().hex()[:16]


def _human_delay(min_s: float = 1.0, max_s: float = 2.5) -> None:
    if max_s > min_s > 0:
        time.sleep(random.uniform(min_s, max_s))


def _debug_screenshot(page, label: str = "debug") -> None:
    try:
        Path("debug").mkdir(parents=True, exist_ok=True)
        path = f"debug/{label}_{int(time.time() * 1000)}.png"
        page.screenshot(path=path, full_page=True)
    except Exception:
        pass


def _hide_webdriver(page) -> None:
    """Mask navigator.webdriver to avoid detection banners."""
    try:
        page.evaluate(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            """
        )
    except Exception:
        pass


# ── Field extractors ─────────────────────────────────────────────────────────


_JUNK_NAMES = {
    "results",
    "výsledky",
    "search results",
    "sponzorované",
    "sponsored",
    "advertisement",
    "reklama",
}


def _get_name(page) -> str:
    selectors = [
        "h1.DUwDvf",
        "h1.fontHeadlineLarge",
        "div.fontHeadlineLarge",
        "h1",
    ]

    for sel in selectors:
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


def _get_category(page) -> str:
    selectors = [
        "button[jsaction*='pane.rating.category']",
        "button[aria-label^='Category']",
        "button[data-item-id='category']",
        "div.fontBodyMedium span",
        "span.fontBodyMedium",
    ]

    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                text = _clean(el.first.inner_text())
                if text and "·" in text:
                    return text.split("·")[0].strip()
                if text:
                    return text
        except Exception:
            continue
    return ""


def _get_rating_reviews(page) -> Tuple[Optional[float], Optional[int]]:
    rating = None
    reviews = None

    try:
        el = page.locator("[aria-label*='hviezd'], [aria-label*='stars']")
        if el.count():
            m = re.search(r"(\d+[.,]?\d?)[^\d]*★", el.first.get_attribute("aria-label") or "")
            if m:
                rating = float(m.group(1).replace(",", "."))
    except Exception:
        pass

    try:
        el = page.locator("[aria-label*='recenzií'], [aria-label*='reviews']")
        if el.count():
            m = re.search(r"(\d+(?:,\d+)?)", el.first.get_attribute("aria-label") or "")
            if m:
                reviews = int(m.group(1).replace(",", ""))
    except Exception:
        pass

    return rating, reviews


def _get_website(page) -> str:
    selectors = [
        "a[data-item-id='authority']",
        "a[aria-label*='Website']",
        "a[aria-label*='Webová stránka']",
        "a[aria-label*='Webseite']",
    ]

    for sel in selectors:
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


def _get_phone(page) -> str:
    selectors = [
        "[data-item-id='phone']",
        "button[aria-label*='Phone']",
        "button[aria-label*='Telefón']",
        "div[aria-label*='Phone']",
    ]

    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                text = _clean(el.first.inner_text())
                if text:
                    return text
        except Exception:
            continue
    return ""


def _get_address(page) -> str:
    selectors = [
        "[data-item-id='address']",
        "button[aria-label*='Address']",
        "button[aria-label*='Adresa']",
    ]

    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                text = _clean(el.first.inner_text())
                if text:
                    return text
        except Exception:
            continue
    return ""


def _get_email_from_panel(page) -> str:
    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[A-Z0-9-]+\.[A-Z]{2,}", re.I)

    try:
        panel = page.locator("div[role='main']")
        if panel.count() == 0:
            return ""
        text = panel.inner_text()
        emails = [e for e in email_pattern.findall(text) if "google" not in e.lower() and "gstatic" not in e.lower()]
        if emails:
            return emails[0]
    except Exception:
        pass
    return ""


def _get_place_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"place/[^/]+/([^/?@]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"1s([A-Za-z0-9_\-:]+)", url)
    if m:
        return m.group(1)
    return None


# ── About tab (email + opening hours) ────────────────────────────────────────

_ABOUT_TAB_LABELS = ["About", "Informácie", "Info", "Über", "O nás"]


def _extract_about_tab(page) -> Dict[str, str]:
    result = {"email_maps": "", "hours": ""}

    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[A-Z0-9-]+\.[A-Z]{2,}", re.I)
    hours_pattern = re.compile(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|"
        r"Pondelok|Utorok|Streda|Štvrtok|Piatok|Sobota|Nedeľa)"
        r".*?(\d{1,2}[:.]\d{2})\s*–\s*(\d{1,2}[:.]\d{2})",
        re.I | re.DOTALL,
    )

    for label in _ABOUT_TAB_LABELS:
        for sel in [
            f"button[aria-label*='{label}']",
            f"div[role='tab']:has-text('{label}')",
            f"button:has-text('{label}')",
        ]:
            try:
                el = page.locator(sel)
                if el.count() == 0:
                    continue
                el.first.click(timeout=3000)
                page.wait_for_timeout(900)

                panel_text = page.inner_text("div[role='main']") or ""

                emails = [
                    e
                    for e in email_pattern.findall(panel_text)
                    if "google" not in e.lower() and "gstatic" not in e.lower()
                ]
                if emails:
                    result["email_maps"] = emails[0]

                hours_match = hours_pattern.search(panel_text)
                if hours_match:
                    result["hours"] = hours_match.group(0).strip()

                if result["email_maps"] or result["hours"]:
                    return result

            except Exception:
                continue

    return result


# ── Wait & consent helpers ───────────────────────────────────────────────────


def _wait_for_detail_panel(page, timeout_ms: int = 15000) -> bool:
    selectors = [
        "h1.DUwDvf",
        "h1.fontHeadlineLarge",
        "button[data-item-id='address']",
        "a[data-item-id='authority']",
        "button[data-item-id='phone']",
    ]

    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=timeout_ms)
            return True
        except PlaywrightTimeoutError:
            continue
    return False


def _accept_consent(page) -> None:
    selectors = [
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('同意')",
        "#L2AGLb",  # common consent button id
    ]

    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count() > 0:
                btn.first.click(timeout=4000)
                page.wait_for_timeout(600)
                return
        except Exception:
            continue


def _wait_for_feed(page, timeout_ms: int = 20000) -> bool:
    try:
        page.wait_for_selector("div[role='feed']", timeout=timeout_ms)
        return True
    except PlaywrightTimeoutError:
        return False


# ── Card URL collector ───────────────────────────────────────────────────────


def _collect_card_urls(feed, max_needed: int) -> List[str]:
    urls: List[str] = []
    seen_urls: set = set()
    stale_rounds = 0
    last_count = 0

    while len(urls) < max_needed:
        try:
            cards = feed.locator("a[href*='/place/'], a[href*='1s0x']")
            count = cards.count()

            for i in range(count):
                try:
                    href = cards.nth(i).get_attribute("href") or ""
                    if href and href not in seen_urls and "/place/" in href:
                        seen_urls.add(href)
                        urls.append(href)
                        if len(urls) >= max_needed:
                            return urls[:max_needed]
                except Exception:
                    continue

            if count == last_count:
                stale_rounds += 1
            else:
                stale_rounds = 0
            last_count = count

            if stale_rounds >= 4:
                break

            feed.evaluate("(el) => el.scrollBy(0, el.scrollHeight + 300)")
            time.sleep(1.1 + random.uniform(0, 0.7))

        except Exception:
            break

    return urls[:max_needed]


# ── Main scraping function ───────────────────────────────────────────────────


def scrape_google_maps(
    query: str,
    max_results: int = 50,
    headless: bool = True,
    slow_mo: int = 0,
    min_delay_s: float = 1.0,
    max_delay_s: float = 2.8,
    user_data_dir: Optional[str] = None,
    max_retries: int = 2,
    review_mode: bool = False,
) -> List[Dict]:
    results: List[Dict] = []
    seen: set = set()

    with sync_playwright() as p:
        # Launch browser
        if user_data_dir:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                slow_mo=slow_mo,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
                ignore_default_args=["--enable-automation"],
            )
        else:
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=slow_mo,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
                ignore_default_args=["--enable-automation"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )

        page = context.new_page()
        _hide_webdriver(page)

        # Load search results
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}?hl=en"
        loaded = False

        for attempt in range(1, max_retries + 2):
            page.goto(search_url, wait_until="domcontentloaded")
            _accept_consent(page)

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            if _wait_for_feed(page):
                loaded = True
                break

            if attempt > max_retries:
                _debug_screenshot(page, "maps_no_feed")
                print(f"[maps] ERROR: feed not found after {attempt} tries")
                break

            _human_delay(2.5, 5.0)

        if not loaded:
            context.close()
            return results

        feed = page.locator("div[role='feed']")

        print(f"[maps] Collecting up to {max_results} card URLs...")
        card_urls = _collect_card_urls(feed, max_results)
        print(f"[maps] Found {len(card_urls)} unique place URLs")

        # ── Visit each place ────────────────────────────────────────────────
        for idx, url in enumerate(card_urls, 1):
            if len(results) >= max_results:
                break

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
                _human_delay(0.6, 1.4)

                if not _wait_for_detail_panel(page):
                    _debug_screenshot(page, f"no_detail_{idx}")
                    continue

                name = _get_name(page)
                if not name:
                    continue

                category = _get_category(page)
                rating, reviews = _get_rating_reviews(page)
                address = _get_address(page)
                phone = _get_phone(page)
                website = _get_website(page)
                maps_url = page.url.strip()

                email_panel = _get_email_from_panel(page)
                about_data = _extract_about_tab(page)

                email = about_data["email_maps"] or email_panel
                hours = about_data["hours"]

                place_id = _get_place_id_from_url(maps_url) or _safe_hash(f"{name}|{address}")
                if place_id in seen:
                    continue
                seen.add(place_id)

                if review_mode:
                    print(f"\n[review {idx}/{max_results}] {name}")
                    print(f"  {category or '?'}  |  {address or '?'}")
                    print(f"  Phone: {phone or '-'}  |  Web: {website or '-'}")
                    print(f"  Email: {email or '-'}")
                    choice = input("  → Enter = accept, s = skip, q = quit: ").strip().lower()
                    if choice in ("q", "quit"):
                        break
                    if choice == "s":
                        continue

                entry = {
                    "place_id": place_id,
                    "name": name,
                    "category": category,
                    "rating": rating,
                    "reviews_count": reviews,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "email": email,
                    "hours": hours,
                    "maps_url": maps_url,
                    "query": query,
                    "collected_at": datetime.utcnow().isoformat(timespec="seconds"),
                }

                results.append(entry)

                print(
                    f"[maps] {len(results):>3}/{max_results:>3} | "
                    f"{name[:48]:<48} | {phone or '-':<18} | {email or '-'}"
                )

                _human_delay(min_delay_s, max_delay_s)

            except Exception as e:
                print(f"[maps] Error processing {url} → {e.__class__.__name__}: {e}")
                _debug_screenshot(page, f"error_{idx}")

        context.close()

    print(f"[maps] Finished — collected {len(results)} valid places.")
    return results