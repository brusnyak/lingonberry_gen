"""
Validation layer for leadgen.

Steps per lead:
1. Dedup — fuzzy name match against existing qualified leads
2. Chain/franchise filter — multiple listings sharing same phone or website
3. Contact reachability — email format, phone format (E.164-ish)
4. AI classification — qualified / skip / needs_review with reason
"""

import re
import os
import json
import sqlite3
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# 1. Dedup helpers
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    """Lowercase, strip punctuation/whitespace for fuzzy compare."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def is_duplicate(conn: sqlite3.Connection, name: str, business_id: int, threshold: int = 85) -> bool:
    """
    Returns True if a lead with a very similar name already has validation_status='qualified'.
    Uses simple character-overlap ratio (no external deps).
    """
    rows = conn.execute(
        "SELECT id, name FROM businesses WHERE validation_status='qualified' AND id != ?",
        (business_id,),
    ).fetchall()
    norm_new = _normalize(name)
    if not norm_new:
        return False
    for row in rows:
        norm_existing = _normalize(row["name"])
        if not norm_existing:
            continue
        # Jaccard on bigrams
        def bigrams(s):
            return set(s[i:i+2] for i in range(len(s) - 1))
        bg_new = bigrams(norm_new)
        bg_ex = bigrams(norm_existing)
        if not bg_new or not bg_ex:
            continue
        sim = len(bg_new & bg_ex) / len(bg_new | bg_ex)
        if sim >= threshold / 100:
            return True
    return False


# ---------------------------------------------------------------------------
# 2. Chain / franchise filter
# ---------------------------------------------------------------------------

_CHAIN_KEYWORDS = [
    "mcdonald", "kfc", "subway", "starbucks", "ikea", "lidl", "tesco", "billa",
    "kaufland", "dm drogerie", "penny", "albert", "interspar", "spar",
    "h&m", "zara", "primark", "deichmann", "sportisimo",
]

def is_chain(name: str, category: str) -> bool:
    """Heuristic: known chain brand names."""
    combined = _normalize((name or "") + " " + (category or ""))
    return any(kw in combined for kw in _CHAIN_KEYWORDS)


def has_duplicate_contact(conn: sqlite3.Connection, phone: str, website: str, business_id: int) -> bool:
    """
    Returns True if another lead already shares the same phone or website root domain,
    suggesting a franchise / multi-location chain.
    """
    def root_domain(url: str) -> str:
        url = re.sub(r"https?://", "", (url or "").lower()).strip("/")
        return url.split("/")[0].lstrip("www.")

    results = []
    if phone and len(phone) > 5:
        rows = conn.execute(
            "SELECT id FROM businesses WHERE phone=? AND id != ? AND validation_status='qualified'",
            (phone.strip(), business_id),
        ).fetchall()
        results.extend(rows)

    if website:
        domain = root_domain(website)
        if domain:
            rows = conn.execute(
                "SELECT id, website FROM businesses WHERE id != ? AND validation_status='qualified'",
                (business_id,),
            ).fetchall()
            for row in rows:
                if root_domain(row["website"]) == domain:
                    results.append(row)

    return len(results) > 0


# ---------------------------------------------------------------------------
# 3. Contact reachability
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,20}$")


def validate_email(email: str) -> bool:
    return bool(email and _EMAIL_RE.match(email.strip()))


def validate_phone(phone: str) -> bool:
    if not phone:
        return False
    digits = re.sub(r"\D", "", phone)
    # Accept anything with 7-15 digits — format varies heavily by country
    return 7 <= len(digits) <= 15


def best_email(email_maps: str, website_emails: str) -> Optional[str]:
    """Return first valid email from maps or website scrape."""
    for source in [email_maps, website_emails]:
        if not source:
            continue
        for candidate in re.split(r"[,;\s]+", source):
            if validate_email(candidate):
                return candidate.strip()
    return None


# ---------------------------------------------------------------------------
# 4. AI classification
# ---------------------------------------------------------------------------

def _ai_classify(lead: dict) -> dict:
    """
    Call LLM to classify lead as qualified / skip / needs_review.
    Returns {"status": ..., "reason": ...}
    Falls back to rule-based if LLM unavailable.
    """
    import requests

    prompt = f"""You are a B2B lead qualification assistant.

Given this business lead, classify it as one of:
- qualified: good ICP fit, reachable, worth outreach
- skip: chain/franchise, no contact info, irrelevant category, or clearly not a fit
- needs_review: uncertain, missing info, or borderline

Respond ONLY with valid JSON: {{"status": "qualified|skip|needs_review", "reason": "one sentence"}}

Lead:
Name: {lead.get('name', '')}
Category: {lead.get('category', '')}
Address: {lead.get('address', '')}
Phone: {lead.get('phone', '')}
Website: {lead.get('website', '')}
Email: {lead.get('email', '')}
Rating: {lead.get('rating', '')} ({lead.get('reviews_count', 0)} reviews)
Score: {lead.get('score', 0)}
Industry: {lead.get('industry', '')}
ICP fit: {lead.get('icp_fit', '')}
Pain points: {lead.get('pain_points', '')}
"""

    # Try Ollama cloud first
    ollama_url = os.environ.get("OLLAMA_BASE_URL", "").rstrip("/")
    ollama_key = os.environ.get("OLLAMA_API_KEY", "")
    if ollama_url and ollama_key:
        try:
            resp = requests.post(
                f"{ollama_url}/api/chat",
                headers={"Authorization": f"Bearer {ollama_key}", "Content-Type": "application/json"},
                json={"model": "gemma3:4b", "messages": [{"role": "user", "content": prompt}], "stream": False},
                timeout=15,
            )
            if resp.ok:
                text = resp.json()["message"]["content"].strip()
                return _parse_ai_response(text)
        except Exception:
            pass

    # Fallback: OpenRouter
    or_key = os.environ.get("OPENROUTER_API_KEY", "")
    if or_key:
        for model in ["meta-llama/llama-3.3-70b-instruct:free", "google/gemma-3-27b-it:free"]:
            try:
                resp = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {or_key}", "Content-Type": "application/json"},
                    json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                    timeout=20,
                )
                if resp.ok:
                    text = resp.json()["choices"][0]["message"]["content"].strip()
                    return _parse_ai_response(text)
            except Exception:
                continue

    # Rule-based fallback
    return _rule_based_classify(lead)


def _parse_ai_response(text: str) -> dict:
    """Extract JSON from LLM response, tolerating markdown fences."""
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        data = json.loads(text)
        status = data.get("status", "needs_review")
        if status not in ("qualified", "skip", "needs_review"):
            status = "needs_review"
        return {"status": status, "reason": data.get("reason", "")}
    except Exception:
        # Try to extract status keyword from free text
        for s in ("qualified", "skip", "needs_review"):
            if s in text.lower():
                return {"status": s, "reason": text[:200]}
        return {"status": "needs_review", "reason": text[:200]}


def _rule_based_classify(lead: dict) -> dict:
    """Simple rule-based fallback when LLM is unavailable."""
    score = lead.get("score", 0) or 0
    has_contact = bool(lead.get("email") or lead.get("phone"))
    has_website = bool(lead.get("website"))

    if score >= 35 and has_contact and has_website:
        return {"status": "qualified", "reason": "rule: high score + contact + website"}
    if score < 15 or (not has_contact and not has_website):
        return {"status": "skip", "reason": "rule: low score or no contact info"}
    return {"status": "needs_review", "reason": "rule: borderline — manual check needed"}


# ---------------------------------------------------------------------------
# 5. Main entry point
# ---------------------------------------------------------------------------

def validate_lead(conn: sqlite3.Connection, lead: dict, use_ai: bool = True) -> dict:
    """
    Run all validation checks on a single lead dict.
    Returns {"status": "qualified|skip|needs_review", "notes": [...]}
    """
    notes = []
    bid = lead.get("id", 0)
    name = lead.get("name", "")
    phone = lead.get("phone", "")
    website = lead.get("website", "")
    category = lead.get("category", "")

    # Resolve best email
    email = best_email(lead.get("email_maps", ""), lead.get("emails", ""))
    lead["email"] = email  # inject for AI prompt

    # --- Dedup ---
    if is_duplicate(conn, name, bid):
        notes.append("duplicate: similar name already qualified")
        return {"status": "skip", "notes": notes}

    # --- Chain filter ---
    if is_chain(name, category):
        notes.append("chain/franchise: known brand keyword")
        return {"status": "skip", "notes": notes}

    if has_duplicate_contact(conn, phone, website, bid):
        notes.append("chain/franchise: shared phone or domain with another qualified lead")
        return {"status": "skip", "notes": notes}

    # --- Contact reachability ---
    if email:
        notes.append(f"email_valid: {email}")
    else:
        notes.append("no_valid_email")

    if phone and validate_phone(phone):
        notes.append("phone_valid")
    elif phone:
        notes.append("phone_ok")  # has phone, format may vary
    else:
        notes.append("no_phone")

    # --- AI classification ---
    if use_ai:
        ai = _ai_classify(lead)
    else:
        ai = _rule_based_classify(lead)

    notes.append(f"ai_reason: {ai['reason']}")
    return {"status": ai["status"], "notes": notes}


def run_validation(conn: sqlite3.Connection, use_ai: bool = True, only_pending: bool = True) -> dict:
    """
    Validate all leads (or only pending ones).
    Returns summary counts.
    """
    query = """
        SELECT b.id, b.name, b.category, b.rating, b.reviews_count,
               b.address, b.phone, b.website, b.email_maps,
               b.score, b.score_reason, b.validation_status,
               w.emails, w.phones,
               e.industry, e.icp_fit, e.pain_points
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN enrichment e ON e.business_id = b.id
    """
    if only_pending:
        query += " WHERE b.validation_status = 'pending' OR b.validation_status IS NULL"

    rows = conn.execute(query).fetchall()
    counts = {"qualified": 0, "skip": 0, "needs_review": 0, "total": len(rows)}

    for row in rows:
        lead = dict(row)
        result = validate_lead(conn, lead, use_ai=use_ai)
        status = result["status"]
        notes_str = " | ".join(result["notes"])
        conn.execute(
            "UPDATE businesses SET validation_status=?, validation_notes=? WHERE id=?",
            (status, notes_str, lead["id"]),
        )
        counts[status] = counts.get(status, 0) + 1

    conn.commit()
    return counts
