import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")


def fetch(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        if resp.status_code >= 400:
            return None
        return resp.text
    except requests.RequestException:
        return None


def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(s.strip() for s in soup.stripped_strings)
    return text


def find_candidate_links(html: str, base_url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    links = {}
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").lower().strip()
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(base_url, href)
        if "about" in text or "about" in href:
            links.setdefault("about", full)
        if "service" in text or "services" in href:
            links.setdefault("services", full)
        if "contact" in text or "contact" in href:
            links.setdefault("contact", full)
    return links


def scrape_site(url: str, sleep_s: float = 1.0) -> Dict[str, str]:
    html = fetch(url)
    if not html:
        return {
            "site_url": url,
            "about_text": "",
            "services_text": "",
            "emails": "",
            "phones": "",
        }

    text = extract_text(html)
    emails = sorted(set(EMAIL_RE.findall(text)))
    phones = sorted(set(PHONE_RE.findall(text)))

    links = find_candidate_links(html, url)

    about_text = ""
    services_text = ""

    if "about" in links:
        time.sleep(sleep_s)
        about_html = fetch(links["about"])
        if about_html:
            about_text = extract_text(about_html)

    if "services" in links:
        time.sleep(sleep_s)
        services_html = fetch(links["services"])
        if services_html:
            services_text = extract_text(services_html)

    # If no services/about, fallback to main page snippets
    if not about_text:
        about_text = text[:2000]
    if not services_text:
        services_text = text[:2000]

    return {
        "site_url": url,
        "about_text": about_text,
        "services_text": services_text,
        "emails": ",".join(emails)[:2000],
        "phones": ",".join(phones)[:2000],
    }
