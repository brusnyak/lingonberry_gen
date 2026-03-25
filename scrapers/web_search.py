"""
leadgen/scrapers/web_search.py
DuckDuckGo web search lead source. pip install ddgs
"""
from __future__ import annotations
import re, time, random
from datetime import datetime
from urllib.parse import urlparse

_SKIP_DOMAINS = {
    "google.com","google.at","google.sk","google.cz","google.de",
    "facebook.com","instagram.com","linkedin.com","twitter.com","x.com",
    "tiktok.com","yelp.com","tripadvisor.com","yellowpages.com","whitepages.com",
    "wikipedia.org","youtube.com",
    "hipages.com.au","yellowpages.com.au","truelocal.com.au",
    "oneflare.com.au","serviceseeking.com.au","airtasker.com",
    "zlatestranky.sk","firmy.cz","herold.at","wlw.de",
}

def _is_business_domain(url):
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
        return not any(domain == s or domain.endswith("."+s) for s in _SKIP_DOMAINS)
    except Exception:
        return False

def _extract_business_name(title, url):
    name = re.sub(r"\s*[-|]\s*.{0,60}$", "", title).strip()
    name = re.sub(r"\s*(Home|Welcome|Official Site|Website)\s*$", "", name, flags=re.I).strip()
    if len(name) < 3:
        domain = urlparse(url).netloc.lstrip("www.").split(".")[0]
        name = domain.replace("-"," ").replace("_"," ").title()
    return name[:120]

def _infer_address(query):
    cities = {"sydney","melbourne","brisbane","perth","adelaide","canberra","vienna","bratislava","prague","wien"}
    for part in reversed(query.lower().split()):
        if part in cities:
            return part.title()
    return ""

def _ddg_search(query, max_results=15):
    try:
        from ddgs import DDGS
    except ImportError:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            print("[web_search] Run: pip install ddgs")
            return []
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results*2, region="wt-wt"):
                url = r.get("href","")
                if not url or not url.startswith("http"):
                    continue
                if not _is_business_domain(url):
                    continue
                results.append({"title":r.get("title",""),"url":url,"snippet":r.get("body","")})
                if len(results) >= max_results:
                    break
    except Exception as e:
        print(f"[web_search] DDG error: {e}")
    return results

def search_leads(query, max_results=15, category=""):
    print(f"[web_search] Searching: {query}")
    results = _ddg_search(query, max_results=max_results)
    leads = []
    seen = set()
    for r in results:
        domain = urlparse(r["url"]).netloc.lstrip("www.")
        if domain in seen:
            continue
        seen.add(domain)
        em = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", r["snippet"])
        ph = re.search(r"(\+?[\d\s\-().]{7,18})", r["snippet"])
        leads.append({
            "place_id": f"ddg_{re.sub(r'[^a-z0-9]','_',domain[:40])}",
            "name": _extract_business_name(r["title"], r["url"]),
            "category": category or "business",
            "rating": 0.0, "reviews_count": 0,
            "address": _infer_address(query),
            "phone": ph.group(1).strip() if ph else "",
            "website": r["url"],
            "email_maps": em.group(0) if em else "",
            "hours": "", "maps_url": "",
            "query": f"ddg:{query}",
            "collected_at": datetime.utcnow().isoformat(),
            "_source": "web_search",
            "_snippet": r["snippet"][:300],
        })
        if len(leads) >= max_results:
            break
    print(f"[web_search] Got {len(leads)} leads from '{query}'")
    return leads

def batch_search(queries, max_per_query=10, delay_between=3.0):
    all_leads, seen = [], set()
    for q in queries:
        for lead in search_leads(q, max_results=max_per_query):
            if lead["place_id"] not in seen:
                seen.add(lead["place_id"])
                all_leads.append(lead)
        time.sleep(delay_between + random.uniform(0, 2.0))
    return all_leads
