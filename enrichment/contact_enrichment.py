"""
leadgen/enrichment/contact_enrichment.py

Extracts owner first name and detects pain signals from available lead data.
Populates: contact_name, pain_point_guess, outreach_angle, apparent_size, digital_maturity

No external API calls — pure heuristics on data already in the DB.
LLM-based enrichment (outreach_angle) is optional and uses OpenRouter.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional


# ── Name extraction ───────────────────────────────────────────────────────────

_GENERIC_PREFIXES = {
    "info", "contact", "hello", "admin", "support", "office", "mail", "team",
    "sales", "enquiries", "enquiry", "noreply", "reception", "booking", "bookings",
    "accounts", "service", "services", "help", "general", "jobs", "work", "workorders",
    "clinic", "dental", "praxis", "ordination", "klinika", "centrum",
    "center", "studio", "shop", "store", "group", "management",
}

_NAME_PATTERNS = [
    # "Hi, I'm James" / "I'm Sarah" / "My name is Tom"
    re.compile(r"(?:hi,?\s+)?i'?m\s+([A-Z][a-z]{2,15})\b", re.I),
    re.compile(r"my name is\s+([A-Z][a-z]{2,15})\b", re.I),
    # "Ask for James" / "speak to Sarah"
    re.compile(r"(?:ask for|speak to|contact)\s+([A-Z][a-z]{2,15})\b", re.I),
    # "Owner: James Smith" / "Proprietor: Tom"
    re.compile(r"(?:owner|proprietor|founder|director|manager)[:\s]+([A-Z][a-z]{2,15})\b", re.I),
    # "- James, Plumber" (hipages style)
    re.compile(r"^[-–]\s*([A-Z][a-z]{2,15}),\s+\w", re.M),
]

_NOT_NAMES = {
    "the", "and", "for", "our", "your", "this", "that", "with", "from",
    "have", "been", "will", "more", "also", "about", "over", "just",
    "all", "any", "can", "get", "has", "its", "new", "not", "one",
    "out", "see", "use", "was", "way", "who", "you", "are", "but",
    "call", "email", "phone", "free", "best", "top", "pro", "ltd",
    "pty", "inc", "llc", "plumbing", "electrical", "hvac", "services",
}


def extract_contact_name(
    about_text: str = "",
    email: str = "",
    maps_description: str = "",
) -> str:
    """Extract owner first name from available text sources."""
    # 1. Try pattern matching on about text + maps description
    for source in [about_text, maps_description]:
        if not source:
            continue
        for pattern in _NAME_PATTERNS:
            m = pattern.search(source)
            if m:
                candidate = m.group(1).strip().capitalize()
                if candidate.lower() not in _NOT_NAMES and len(candidate) >= 3:
                    return candidate

    # 2. Email prefix: james.smith@ or j.smith@ -> "James"
    if email:
        prefix = email.split("@")[0].lower()
        if prefix not in _GENERIC_PREFIXES:
            # firstname.lastname pattern
            if re.match(r"^[a-z]{2,12}\.[a-z]{2,15}$", prefix):
                first = prefix.split(".")[0]
                if first not in _NOT_NAMES and first not in _GENERIC_PREFIXES:
                    return first.capitalize()
            # plain first name: james@, sarah@
            if re.match(r"^[a-z]{3,12}$", prefix) and prefix not in _NOT_NAMES:
                return prefix.capitalize()

    return ""


# ── Pain signal detection ─────────────────────────────────────────────────────

_PAIN_SIGNALS = {
    "slow_followup": {
        "patterns": [
            r"slow.{0,20}(reply|response|respond|get back)",
            r"(didn'?t|did not|never).{0,20}(call|reply|respond|get back|follow)",
            r"hard to (reach|contact|get hold)",
            r"(took|takes).{0,20}(days?|week|long).{0,20}(reply|respond|get back)",
            r"no.{0,15}(response|reply|callback)",
            r"(missed|unanswered).{0,15}(call|message)",
        ],
        "label": "slow_followup",
        "outreach_hint": "auto-reply so every missed call gets a text back within 60 seconds",
    },
    "no_booking": {
        "patterns": [
            r"no.{0,20}(online.{0,10}book|booking.{0,10}system|book.{0,10}online)",
            r"can'?t.{0,20}book.{0,20}online",
            r"(call|phone|ring).{0,20}to.{0,20}book",
        ],
        "label": "no_booking",
        "outreach_hint": "add an online booking link so customers can book without calling",
    },
    "overwhelmed": {
        "patterns": [
            r"(very|extremely|always).{0,15}busy",
            r"(hard|difficult).{0,20}(get.{0,10}appointment|schedule|availability)",
            r"(wait|waiting).{0,20}(list|time|weeks?|months?)",
            r"(understaffed|short.{0,10}staff|can'?t keep up)",
        ],
        "label": "overwhelmed",
        "outreach_hint": "automate intake and triage so you handle more jobs without more admin",
    },
    "no_social": {
        "patterns": [
            r"no.{0,20}(facebook|instagram|social)",
            r"(inactive|dead|old).{0,20}(facebook|instagram|social|page|profile)",
        ],
        "label": "no_social",
        "outreach_hint": "set up a simple social presence so customers can find and trust you",
    },
    "no_website": {
        "patterns": [
            r"no.{0,15}website",
            r"(doesn'?t|does not).{0,15}have.{0,15}website",
        ],
        "label": "no_website",
        "outreach_hint": "build a simple one-page site so customers can find your contact details",
    },
}


def detect_pain_signals(
    review_text: str = "",
    about_text: str = "",
    website: str = "",
    socials: str = "",
    has_booking: int = 0,
    has_lead_capture: int = 0,
) -> tuple[str, str]:
    """
    Returns (pain_label, outreach_hint) for the strongest detected signal.
    Falls back to structural signals (no booking, no website) if no text signals found.
    """
    combined = " ".join([review_text or "", about_text or ""]).lower()

    for signal_key, signal in _PAIN_SIGNALS.items():
        for pattern in signal["patterns"]:
            if re.search(pattern, combined, re.I):
                return signal["label"], signal["outreach_hint"]

    # Structural signals from gap detection
    if not has_booking:
        return "no_booking", _PAIN_SIGNALS["no_booking"]["outreach_hint"]
    if not has_lead_capture:
        return "no_lead_capture", "add a contact form so you capture leads even when you're on a job"
    if not website:
        return "no_website", _PAIN_SIGNALS["no_website"]["outreach_hint"]

    return "", ""


# ── Outreach angle builder ────────────────────────────────────────────────────

def build_outreach_angle(
    business_name: str,
    trade: str,
    pain_label: str,
    pain_hint: str,
    city: str = "",
) -> str:
    """
    Build a one-sentence specific offer line for this lead.
    Used as outreach_angle in the generator.
    """
    if not pain_hint:
        return ""

    # Sanitize trade — reject numeric/rating strings, use fallback
    trade_clean = (trade or "").strip()
    if not trade_clean or re.match(r"^[\d.\s]+$", trade_clean) or len(trade_clean) > 60:
        trade_clean = "trades business"

    trade_label = trade_clean.lower()
    city_suffix = f" in {city}" if city else ""

    hint = pain_hint
    if "{trade}" in hint:
        hint = hint.replace("{trade}", trade_label)

    return f"For a {trade_label}{city_suffix} I could {hint}."


# ── City extraction ───────────────────────────────────────────────────────────

_AU_CITIES = {
    "sydney", "melbourne", "brisbane", "perth", "adelaide",
    "canberra", "darwin", "hobart", "gold coast", "newcastle",
    "wollongong", "geelong", "townsville", "cairns", "toowoomba",
}

_AU_STATES = {"nsw", "vic", "qld", "wa", "sa", "nt", "act", "tas"}


def extract_city(address: str) -> str:
    """Best-effort city extraction from address string."""
    if not address:
        return ""
    addr_lower = address.lower()
    for city in _AU_CITIES:
        if city in addr_lower:
            return city.title()
    # Try to parse "Suburb, STATE POSTCODE" pattern
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        candidate = parts[-2].strip()
        # Remove postcode if present
        candidate = re.sub(r"\b\d{4}\b", "", candidate).strip()
        if candidate and len(candidate) > 2:
            return candidate
    return ""


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_contact_and_pain(lead: dict, website_row: dict | None = None) -> dict:
    """
    Given a lead dict and optional website_data row, return enrichment fields:
    {contact_name, pain_point_guess, outreach_angle, apparent_size, digital_maturity}
    """
    about_text = (website_row or {}).get("about_text", "") or ""
    socials_raw = (website_row or {}).get("socials", "") or ""
    website = lead.get("website", "") or ""
    email = (lead.get("email_maps", "") or lead.get("site_emails", "") or "").split(",")[0].strip()
    address = lead.get("address", "") or ""
    category = lead.get("category", "") or ""
    name = lead.get("name", "") or ""

    # Name
    contact_name = extract_contact_name(
        about_text=about_text,
        email=email,
        maps_description=lead.get("brand_summary", "") or "",
    )

    # Pain
    pain_label, pain_hint = detect_pain_signals(
        review_text="",  # reviews not yet scraped — extend later
        about_text=about_text,
        website=website,
        socials=socials_raw,
        has_booking=lead.get("has_booking", 0) or 0,
        has_lead_capture=lead.get("has_lead_capture", 0) or 0,
    )

    # City
    city = extract_city(address)

    # Outreach angle
    outreach_angle = build_outreach_angle(
        business_name=name,
        trade=category,
        pain_label=pain_label,
        pain_hint=pain_hint,
        city=city,
    )

    # Apparent size (heuristic from reviews count)
    reviews = lead.get("reviews_count") or 0
    if reviews == 0:
        apparent_size = "unknown"
    elif reviews < 20:
        apparent_size = "micro"
    elif reviews < 100:
        apparent_size = "small"
    else:
        apparent_size = "established"

    # Digital maturity
    has_website = bool(website)
    has_socials = bool(socials_raw and socials_raw != "{}")
    has_booking = bool(lead.get("has_booking"))
    if has_booking:
        digital_maturity = "moderate"
    elif has_website and has_socials:
        digital_maturity = "basic"
    elif has_website or has_socials:
        digital_maturity = "minimal"
    else:
        digital_maturity = "none"

    return {
        "contact_name": contact_name,
        "pain_point_guess": pain_label,
        "outreach_angle": outreach_angle,
        "apparent_size": apparent_size,
        "digital_maturity": digital_maturity,
    }


def run_contact_enrichment(conn: sqlite3.Connection, limit: int = 100, only_missing: bool = True) -> dict:
    """
    Run contact+pain enrichment for leads that are qualified but missing outreach_angle.
    Updates businesses table in place.
    """
    query = """
        SELECT b.id, b.name, b.category, b.address, b.website, b.email_maps,
               b.has_booking, b.has_lead_capture, b.brand_summary,
               b.contact_name, b.outreach_angle,
               w.about_text, w.socials, w.emails AS site_emails
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE b.validation_status IN ('qualified', 'needs_review')
    """
    if only_missing:
        query += " AND (b.outreach_angle IS NULL OR b.outreach_angle = '')"
    query += f" LIMIT {int(limit)}"

    rows = conn.execute(query).fetchall()
    counts = {"enriched": 0, "skipped": 0, "total": len(rows)}

    for row in rows:
        lead = dict(row)
        website_row = {"about_text": lead.pop("about_text", ""), "socials": lead.pop("socials", "")}
        result = enrich_contact_and_pain(lead, website_row)

        # Only update non-empty fields — don't overwrite existing data
        updates = {k: v for k, v in result.items() if v}
        if not updates:
            counts["skipped"] += 1
            continue

        set_sql = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(
            f"UPDATE businesses SET {set_sql} WHERE id = ?",
            [*updates.values(), lead["id"]],
        )
        counts["enriched"] += 1

    conn.commit()
    return counts
