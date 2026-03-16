"""
Website scraper — upgraded with:
- reachability check (DNS + HTTP status before scraping)
- better email extraction (mailto links, meta tags, contact page)
- social profile links
- basic tech/platform hints (Shopify, WordPress, Wix, etc.)
- structured return with site_status field
"""

import re
import socket
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"\+?[\d][\d\s().\-]{6,}\d")

SOCIAL_PATTERNS = {
    "facebook":  re.compile(r"facebook\.com/[\w.]+"),
    "instagram": re.compile(r"instagram\.com/[\w.]+"),
    "linkedin":  re.compile(r"linkedin\.com/(?:company|in)/[\w\-]+"),
    "twitter":   re.compile(r"(?:twitter|x)\.com/[\w]+"),
}

TECH_SIGNALS = {
    "wordpress":  ["wp-content", "wp-includes", "wordpress"],
    "shopify":    ["cdn.shopify.com", "myshopify.com"],
    "wix":        ["wix.com", "wixsite.com", "_wix_"],
    "squarespace":["squarespace.com", "sqsp.net"],
    "webflow":    ["webflow.io", "webflow.com"],
    "joomla":     ["/components/com_", "joomla"],
    "drupal":     ["drupal.js", "drupal.org"],
}

JUNK_EMAIL_DOMAINS = {
    "sentry.io", "example.com", "test.com", "wixpress.com",
    "squarespace.com", "shopify.com", "wordpress.com",
    "schema.org", "w3.org", "googleapis.com",
}


# ---------------------------------------------------------------------------
# Reachability
# ---------------------------------------------------------------------------

def check_reachable(url: str, timeout: int = 8) -> Dict[str, object]:
    """
    Returns {"reachable": bool, "status_code": int|None, "redirect_url": str|None, "error": str|None}
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or ""

    # DNS check
    try:
        socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        return {"reachable": False, "status_code": None, "redirect_url": None, "error": f"dns_fail: {e}"}

    # HTTP check
    try:
        resp = requests.head(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            allow_redirects=True,
        )
        final_url = resp.url if resp.url != url else None
        if resp.status_code >= 400:
            return {"reachable": False, "status_code": resp.status_code, "redirect_url": final_url, "error": f"http_{resp.status_code}"}
        return {"reachable": True, "status_code": resp.status_code, "redirect_url": final_url, "error": None}
    except requests.exceptions.SSLError:
        # Try http fallback
        http_url = url.replace("https://", "http://", 1)
        try:
            resp = requests.head(http_url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
            return {"reachable": resp.status_code < 400, "status_code": resp.status_code, "redirect_url": None, "error": "ssl_fallback_http"}
        except Exception as e2:
            return {"reachable": False, "status_code": None, "redirect_url": None, "error": f"ssl_error: {e2}"}
    except requests.RequestException as e:
        return {"reachable": False, "status_code": None, "redirect_url": None, "error": str(e)[:120]}


# ---------------------------------------------------------------------------
# Fetch + parse helpers
# ---------------------------------------------------------------------------

def fetch(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
        if resp.status_code >= 400:
            return None
        return resp.text
    except requests.RequestException:
        return None


def extract_text(html: str, max_chars: int = 4000) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()
    text = " ".join(s.strip() for s in soup.stripped_strings)
    return text[:max_chars]


def extract_emails(html: str, base_url: str) -> List[str]:
    """Extract emails from text content AND mailto: links, filter junk domains."""
    soup = BeautifulSoup(html, "html.parser")
    found = set()

    # mailto links first (most reliable)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            if EMAIL_RE.match(email):
                found.add(email)

    # text scan
    text = " ".join(s.strip() for s in soup.stripped_strings)
    for m in EMAIL_RE.findall(text):
        found.add(m.lower())

    # filter junk
    clean = [
        e for e in found
        if not any(junk in e for junk in JUNK_EMAIL_DOMAINS)
        and not e.startswith("noreply")
        and not e.startswith("no-reply")
        and "example" not in e
    ]
    return sorted(clean)


def extract_phones(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = set()
    # tel: links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("tel:"):
            found.add(href[4:].strip())
    # text scan
    text = " ".join(s.strip() for s in soup.stripped_strings)
    for m in PHONE_RE.findall(text):
        digits = re.sub(r"\D", "", m)
        if 7 <= len(digits) <= 15:
            found.add(m.strip())
    return sorted(found)[:5]


def extract_socials(html: str) -> Dict[str, str]:
    socials = {}
    for platform, pattern in SOCIAL_PATTERNS.items():
        m = pattern.search(html)
        if m:
            socials[platform] = "https://" + m.group(0)
    return socials


def detect_tech(html: str) -> List[str]:
    html_lower = html.lower()
    detected = []
    for tech, signals in TECH_SIGNALS.items():
        if any(s in html_lower for s in signals):
            detected.append(tech)
    return detected


def find_candidate_links(html: str, base_url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Dict[str, str] = {}
    parsed_base = urlparse(base_url)
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").lower().strip()
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full = urljoin(base_url, href)
        # stay on same domain
        if urlparse(full).hostname != parsed_base.hostname:
            continue
        if "about" in text or "about" in href.lower():
            links.setdefault("about", full)
        if "service" in text or "service" in href.lower():
            links.setdefault("services", full)
        if "contact" in text or "contact" in href.lower():
            links.setdefault("contact", full)
    return links


# ---------------------------------------------------------------------------
# Main scrape entry point
# ---------------------------------------------------------------------------

def scrape_site(url: str, sleep_s: float = 0.8) -> Dict:
    """
    Full site scrape. Returns dict with:
      site_url, site_status, site_error, about_text, services_text,
      emails, phones, socials, tech_stack
    """
    result = {
        "site_url": url,
        "site_status": "unreachable",
        "site_error": None,
        "about_text": "",
        "services_text": "",
        "emails": "",
        "phones": "",
        "socials": "",
        "tech_stack": "",
    }

    # 1. Reachability
    reach = check_reachable(url)
    if not reach["reachable"]:
        result["site_error"] = reach["error"]
        return result

    result["site_status"] = "ok"
    effective_url = reach.get("redirect_url") or url

    # 2. Fetch homepage
    html = fetch(effective_url)
    if not html:
        result["site_status"] = "fetch_failed"
        return result

    # 3. Emails, phones, socials, tech from homepage
    emails = extract_emails(html, effective_url)
    phones = extract_phones(html)
    socials = extract_socials(html)
    tech = detect_tech(html)

    # 4. Sub-pages
    links = find_candidate_links(html, effective_url)
    about_text = ""
    services_text = ""

    if "about" in links:
        time.sleep(sleep_s)
        about_html = fetch(links["about"])
        if about_html:
            about_text = extract_text(about_html)
            emails += extract_emails(about_html, links["about"])

    if "contact" in links:
        time.sleep(sleep_s)
        contact_html = fetch(links["contact"])
        if contact_html:
            emails += extract_emails(contact_html, links["contact"])
            phones += extract_phones(contact_html)

    if "services" in links:
        time.sleep(sleep_s)
        services_html = fetch(links["services"])
        if services_html:
            services_text = extract_text(services_html)

    # Fallback to homepage text
    if not about_text:
        about_text = extract_text(html)
    if not services_text:
        services_text = extract_text(html, max_chars=2000)

    # Deduplicate emails
    seen = set()
    clean_emails = []
    for e in emails:
        if e not in seen:
            seen.add(e)
            clean_emails.append(e)

    result.update({
        "about_text":    about_text[:3000],
        "services_text": services_text[:2000],
        "emails":        ",".join(clean_emails[:8]),
        "phones":        ",".join(dict.fromkeys(phones))[:500],
        "socials":       str(socials) if socials else "",
        "tech_stack":    ",".join(tech),
    })
    return result
