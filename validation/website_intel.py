"""
Per-lead website intelligence layer.

For each lead with a website, produces a structured brand profile:
- site_reachable, has_email, has_phone, has_socials
- brand_summary: what the business actually does (from site content)
- tech_stack: platform hints
- llm_eval: AI assessment of the lead quality, pain points, and a casual outreach angle

This runs AFTER website scraping, BEFORE final validation status is set.
"""

import json
import os
import re
import sqlite3
from typing import Optional

import requests


# ---------------------------------------------------------------------------
# LLM brand evaluation
# ---------------------------------------------------------------------------

_EVAL_SYSTEM = (
    "You are a sharp B2B sales researcher. "
    "Given a business's website content, write a concise brand profile and evaluate them as a potential client. "
    "Return ONLY valid JSON."
)

_EVAL_SCHEMA = """{
  "brand_summary": "1-2 sentences: what this business actually does and who they serve",
  "apparent_size": "solo | small (2-10) | medium (10-50) | large (50+) | unknown",
  "digital_maturity": "low | medium | high",
  "pain_point_guess": "most likely pain point or gap you can spot from their site",
  "outreach_angle": "one specific, non-generic hook for a first message — reference something real from their site",
  "qualification": "strong | moderate | weak | skip",
  "qualification_reason": "one sentence why"
}"""


def _llm_eval(lead: dict) -> Optional[dict]:
    """Call LLM with site content. Returns parsed dict or None on failure."""
    name = lead.get("name", "")
    category = lead.get("category", "")
    about = (lead.get("about_text") or "")[:2000]
    services = (lead.get("services_text") or "")[:1000]
    tech = lead.get("tech_stack", "")
    socials = lead.get("socials", "")

    prompt = f"""Business: {name}
Category: {category}
Tech stack: {tech}
Social profiles found: {socials}

--- About page ---
{about}

--- Services page ---
{services}

Respond with this exact JSON schema:
{_EVAL_SCHEMA}"""

    # Try Ollama cloud
    ollama_url = os.environ.get("OLLAMA_BASE_URL", "").rstrip("/")
    ollama_key = os.environ.get("OLLAMA_API_KEY", "")
    if ollama_url and ollama_key:
        try:
            resp = requests.post(
                f"{ollama_url}/api/chat",
                headers={"Authorization": f"Bearer {ollama_key}", "Content-Type": "application/json"},
                json={
                    "model": "gemma3:4b",
                    "messages": [
                        {"role": "system", "content": _EVAL_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    "stream": False,
                    "format": "json",
                    "options": {"temperature": 0.3},
                },
                timeout=20,
            )
            if resp.ok:
                content = resp.json().get("message", {}).get("content", "")
                return _parse_eval(content)
        except Exception:
            pass

    # Fallback: OpenRouter
    or_key = os.environ.get("OPENROUTER_API_KEY", "")
    if or_key:
        for model in [
            "meta-llama/llama-3.3-70b-instruct:free",
            "google/gemma-3-27b-it:free",
            "mistralai/mistral-small-3.1-24b-instruct:free",
        ]:
            try:
                resp = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {or_key}",
                        "HTTP-Referer": "https://github.com/biz-leadgen",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": _EVAL_SYSTEM},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.3,
                    },
                    timeout=25,
                )
                if resp.ok:
                    content = resp.json()["choices"][0]["message"]["content"]
                    result = _parse_eval(content)
                    if result:
                        return result
            except Exception:
                continue

    return None


def _parse_eval(text: str) -> Optional[dict]:
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        data = json.loads(text)
        # Normalise qualification value
        qual = str(data.get("qualification", "")).lower()
        if qual not in ("strong", "moderate", "weak", "skip"):
            qual = "moderate"
        data["qualification"] = qual
        return data
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main entry: enrich a single lead with website intel
# ---------------------------------------------------------------------------

def build_website_intel(lead: dict, use_ai: bool = True) -> dict:
    """
    Takes a lead dict (already has website scrape data merged in).
    Returns an intel dict with structured fields.
    """
    intel = {
        "site_reachable": False,
        "has_email": False,
        "has_phone": False,
        "has_socials": False,
        "tech_stack": lead.get("tech_stack", ""),
        "brand_summary": "",
        "apparent_size": "unknown",
        "digital_maturity": "unknown",
        "pain_point_guess": "",
        "outreach_angle": "",
        "qualification": "unknown",
        "qualification_reason": "",
    }

    # Site reachability (from scrape result)
    site_status = lead.get("site_status", "")
    intel["site_reachable"] = site_status == "ok"

    if not intel["site_reachable"]:
        intel["qualification"] = "skip"
        intel["qualification_reason"] = f"site unreachable: {lead.get('site_error', 'unknown')}"
        return intel

    # Contact signals
    emails = lead.get("emails", "") or ""
    phones = lead.get("phones", "") or lead.get("phone", "") or ""
    socials = lead.get("socials", "") or ""

    intel["has_email"] = bool(emails.strip())
    intel["has_phone"] = bool(phones.strip())
    intel["has_socials"] = bool(socials.strip() and socials != "{}")

    # No content at all — weak signal
    about = (lead.get("about_text") or "").strip()
    services = (lead.get("services_text") or "").strip()
    if not about and not services:
        intel["qualification"] = "weak"
        intel["qualification_reason"] = "site reachable but no readable content"
        return intel

    # AI evaluation
    if use_ai:
        eval_result = _llm_eval(lead)
        if eval_result:
            intel.update({
                "brand_summary":        eval_result.get("brand_summary", ""),
                "apparent_size":        eval_result.get("apparent_size", "unknown"),
                "digital_maturity":     eval_result.get("digital_maturity", "unknown"),
                "pain_point_guess":     eval_result.get("pain_point_guess", ""),
                "outreach_angle":       eval_result.get("outreach_angle", ""),
                "qualification":        eval_result.get("qualification", "moderate"),
                "qualification_reason": eval_result.get("qualification_reason", ""),
            })
        else:
            # Rule-based fallback
            score = 0
            if intel["has_email"]:
                score += 2
            if intel["has_phone"]:
                score += 1
            if intel["has_socials"]:
                score += 1
            if len(about) > 300:
                score += 1
            intel["qualification"] = "strong" if score >= 4 else "moderate" if score >= 2 else "weak"
            intel["qualification_reason"] = "rule-based: ai unavailable"
    else:
        # Rule-based only
        score = 0
        if intel["has_email"]:
            score += 2
        if intel["has_phone"]:
            score += 1
        if intel["has_socials"]:
            score += 1
        if len(about) > 300:
            score += 1
        intel["qualification"] = "strong" if score >= 4 else "moderate" if score >= 2 else "weak"
        intel["qualification_reason"] = "rule-based"

    return intel


# ---------------------------------------------------------------------------
# Batch runner against DB
# ---------------------------------------------------------------------------

def run_website_intel(conn: sqlite3.Connection, use_ai: bool = True, only_missing: bool = True) -> dict:
    """
    Run website intel for all leads that have a website.
    Stores results back into businesses table (new columns).
    Returns summary counts.
    """
    _ensure_columns(conn)

    query = """
        SELECT b.id, b.name, b.category, b.phone, b.website, b.email_maps,
               b.validation_status,
               w.about_text, w.services_text, w.emails, w.phones,
               w.site_url, w.socials, w.tech_stack, w.site_status, w.site_error
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        WHERE b.website IS NOT NULL AND b.website != ''
    """
    if only_missing:
        query += " AND (b.site_intel_done IS NULL OR b.site_intel_done = 0)"

    rows = conn.execute(query).fetchall()
    counts = {"total": len(rows), "reachable": 0, "unreachable": 0, "strong": 0, "moderate": 0, "weak": 0, "skip": 0}

    for row in rows:
        lead = dict(row)
        intel = build_website_intel(lead, use_ai=use_ai)

        qual = intel["qualification"]
        counts[qual] = counts.get(qual, 0) + 1
        if intel["site_reachable"]:
            counts["reachable"] += 1
        else:
            counts["unreachable"] += 1

        conn.execute(
            """UPDATE businesses SET
                site_reachable=?, has_email=?, has_phone=?, has_socials=?,
                brand_summary=?, apparent_size=?, digital_maturity=?,
                pain_point_guess=?, outreach_angle=?,
                site_qualification=?, site_qual_reason=?,
                site_intel_done=1
               WHERE id=?""",
            (
                int(intel["site_reachable"]),
                int(intel["has_email"]),
                int(intel["has_phone"]),
                int(intel["has_socials"]),
                intel["brand_summary"],
                intel["apparent_size"],
                intel["digital_maturity"],
                intel["pain_point_guess"],
                intel["outreach_angle"],
                intel["qualification"],
                intel["qualification_reason"],
                lead["id"],
            ),
        )

    conn.commit()
    return counts


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """Add new intel columns to businesses table if not present."""
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(businesses)").fetchall()}
    new_cols = [
        ("site_reachable",    "INTEGER DEFAULT 0"),
        ("has_email",         "INTEGER DEFAULT 0"),
        ("has_phone",         "INTEGER DEFAULT 0"),
        ("has_socials",       "INTEGER DEFAULT 0"),
        ("brand_summary",     "TEXT"),
        ("apparent_size",     "TEXT"),
        ("digital_maturity",  "TEXT"),
        ("pain_point_guess",  "TEXT"),
        ("outreach_angle",    "TEXT"),
        ("site_qualification","TEXT"),
        ("site_qual_reason",  "TEXT"),
        ("site_intel_done",   "INTEGER DEFAULT 0"),
    ]
    for col, typedef in new_cols:
        if col not in existing:
            conn.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typedef}")
    conn.commit()
