"""
leadgen/enrichment/social_discovery.py

Finds LinkedIn company profiles and Facebook pages for existing leads using web search.
"""
from __future__ import annotations
import re
import time
import random
import sqlite3
from typing import Dict, Optional
from scrapers.web_search import _ddg_search

def find_social_profiles(business_name: str, city: str = "") -> Dict[str, str]:
    """
    Search for LinkedIn and Facebook profiles for a business.
    Returns {"linkedin": "url", "facebook": "url"}
    """
    results = {}
    query_city = f" {city}" if city else ""
    
    # 1. LinkedIn Search
    li_query = f'"{business_name}"{query_city} site:linkedin.com/company'
    li_results = _ddg_search(li_query, max_results=3)
    for r in li_results:
        url = r.get("url", "")
        if "linkedin.com/company/" in url:
            results["linkedin"] = url
            break
            
    # 2. Facebook Search (if not found in li_results)
    fb_query = f'"{business_name}"{query_city} site:facebook.com'
    fb_results = _ddg_search(fb_query, max_results=3)
    for r in fb_results:
        url = r.get("url", "")
        if "facebook.com/" in url and not any(x in url for x in ["/groups/", "/events/", "/posts/"]):
            results["facebook"] = url
            break
            
    return results

def run_social_discovery(conn: sqlite3.Connection, limit: int = 50) -> Dict[str, int]:
    """
    Find social profiles for qualified leads missing LinkedIn/Facebook URLs.
    """
    query = """
        SELECT b.id, b.name, b.address
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE b.validation_status = 'qualified'
          AND (w.linkedin_url IS NULL OR w.linkedin_url = '')
        LIMIT ?
    """
    rows = conn.execute(query, (limit,)).fetchall()
    counts = {"found": 0, "total": len(rows)}
    
    for row in rows:
        lead_id = row["id"]
        name = row["name"]
        # Extract city from address
        city = ""
        if row["address"]:
            # Simple heuristic: last part of address
            parts = [p.strip() for p in row["address"].split(",")]
            if parts:
                city = parts[-1]
                
        profiles = find_social_profiles(name, city)
        if profiles:
            updates = []
            params = []
            if "linkedin" in profiles:
                updates.append("linkedin_url = ?")
                params.append(profiles["linkedin"])
            if "facebook" in profiles:
                updates.append("facebook_url = ?")
                params.append(profiles["facebook"])
                
            if updates:
                set_sql = ", ".join(updates)
                conn.execute(
                    f"UPDATE website_data SET {set_sql} WHERE business_id = ?",
                    [*params, lead_id]
                )
                counts["found"] += 1
        
        # Human delay to avoid rate limiting
        time.sleep(random.uniform(1.0, 3.0))
        
    conn.commit()
    return counts
