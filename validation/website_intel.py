"""
Per-lead website intelligence layer.

For each lead with a website, produces a structured brand profile + gap profile:
- site_reachable, has_email, has_phone, has_socials
- brand_summary: what the business actually does (from site content)
- tech_stack: platform hints
- gap_profile: structured detectable weaknesses (booking, portal, ecommerce, tracking, lead capture)
- llm_eval: AI assessment of the lead quality, pain points, and a casual outreach angle

This runs AFTER website scraping, BEFORE final validation status is set.
"""

import json
import os
import re
import sqlite3
from typing import Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent.remote_models import complete_text

try:
    from leadgen.niches import infer_niche
except ImportError:
    from niches import infer_niche  # type: ignore


# ---------------------------------------------------------------------------
# Gap detection — rule-based, no LLM needed
# ---------------------------------------------------------------------------

# Booking widget signals
_BOOKING_SIGNALS = [
    "calendly.com", "doctolib", "simplybook", "fresha.com", "booksy",
    "zocdoc", "practo", "setmore", "acuityscheduling", "square appointments",
    "/booking", "/rezervacia", "/rezervovat", "/termin", "/appointment",
    "book-online", "book an appointment", "online booking", "buchen sie",
    "jetzt buchen", "termin buchen", "objednat se", "rezervace online",
    "objednať", "objednat", "objednať sa", "objednat se", "objednajte sa",
    "rezervovať", "rezervovat", "rezervácia", "rezervace", "book now",
]

# Client portal signals (accounting / professional services)
_PORTAL_SIGNALS = [
    "/login", "/portal", "/client-area", "/client-portal", "/secure",
    "/moje", "/klient", "/kundenportal",
    "taxdome", "karbon", "canopy", "onvio", "suralink",
    "sharepoint", "google drive", "dropbox business",
]

# E-commerce signals
_ECOMMERCE_SIGNALS = [
    "/cart", "/shop", "/store", "/eshop", "/obchod",
    "add to cart", "add to basket", "buy now", "checkout",
    "woocommerce", "shopify", "prestashop", "magento", "opencart",
]

# Analytics / tracking signals
_TRACKING_SIGNALS = [
    "google-analytics", "googletagmanager", "gtag(", "ga(", "_gaq",
    "fbq(", "facebook pixel", "hotjar", "clarity.ms",
    "segment.com", "mixpanel",
]

# Lead capture signals
_LEAD_CAPTURE_SIGNALS = [
    "<form", "contact-form", "contactform", "wpcf7", "gravityforms",
    "hubspot-form", "typeform", "jotform",
    "get in touch", "send us a message", "request a quote",
    "free consultation", "book a call", "schedule a call",
    "kontakt", "kontaktujte", "napíšte nám", "napiste nam",
    "objednať sa", "objednat se", "rezervácia", "rezervace",
]

_DENTAL_STRONG_RE = re.compile(
    r"\b(dental|dentist|zahnarzt|stomatolog|stomatol[oó]g|orthodont|implantolog|implant)\b",
    re.IGNORECASE,
)
_DENTAL_LOCAL_RE = re.compile(
    r"\b(zubn[aá]|zubn[eé]|zuby|zubar|zuba[rř]|dent[aá]lna hygiena|dent[aá]lne centrum|stroj[čc]ek)\b",
    re.IGNORECASE,
)
_NON_DENTAL_RE = re.compile(
    r"\b(account|accounting|účt|uct|tax|audit|finance|financ|marketing|agent[uú]ra|agency|consult|consulting|advisory|media)\b",
    re.IGNORECASE,
)

_DENTAL_OPPORTUNITY_RULES = [
    (
        "high_value_case_followup",
        [
            "implant", "implanto", "all on 4", "all-on-4", "all on 6",
            "veneer", "fazet", "korunk", "crown", "protet", "invisalign",
            "aligner", "strojček", "orthodont", "esthetic", "estetick",
        ],
        "High-value treatments are visible on the site, which usually means enquiry follow-up speed and nurture matter.",
        "I noticed you handle higher-value treatments like implants/esthetic work, so I was curious how follow-up is handled after an enquiry comes in.",
    ),
    (
        "hygiene_recall",
        [
            "dentálna hygiena", "dental hygiene", "preventívna prehliadka",
            "preventive", "recall", "profylax", "hygiena",
        ],
        "Recall-driven services are visible, which creates a strong case for reminder and reactivation flows.",
        "I noticed dentálna hygiena / preventive care is a visible part of the clinic, so I was curious how recalls and repeat reminders are handled now.",
    ),
    (
        "emergency_intake",
        [
            "pohotovosť", "emergency", "urgent", "same day", "sobota na objednávku",
        ],
        "Emergency or urgent care is mentioned, which usually creates phone-heavy intake and missed-call risk.",
        "I noticed emergency / urgent care is part of the offer, so I was curious how after-hours or missed-call intake is handled right now.",
    ),
    (
        "multilingual_patient_flow",
        [
            " sk en ", " sk de ", " english", " deutsch", "eng de", "en de",
        ],
        "Multiple language cues appear on the site, which often means more intake friction across channels and forms.",
        "I noticed the site appears to serve patients in multiple languages, so I was curious how enquiries are handled when they come in across different channels.",
    ),
    (
        "review_capture",
        [
            "recenzie", "reviews", "testimonial", "príbehy", "before and after", "pred a po",
        ],
        "The clinic already leans on proof and testimonials, which creates an opening around review capture and follow-up.",
        "I noticed reviews / patient results are an important part of the site, so I was curious how you currently ask happy patients for new reviews.",
    ),
]


def detect_gaps(html: str, links: dict, allow_negative_inference: bool = False) -> dict:
    """
    Detect presence/absence of key digital capabilities from raw HTML.
    Returns a gap_profile dict.
    """
    html_lower = html.lower()

    has_booking      = any(s in html_lower for s in _BOOKING_SIGNALS)
    has_portal       = any(s in html_lower for s in _PORTAL_SIGNALS)
    has_ecommerce    = any(s in html_lower for s in _ECOMMERCE_SIGNALS)
    has_tracking     = any(s in html_lower for s in _TRACKING_SIGNALS)
    has_lead_capture = any(s in html_lower for s in _LEAD_CAPTURE_SIGNALS)

    # Also check sub-page links for booking/portal hints
    all_links = " ".join(links.values()).lower()
    if not has_booking:
        has_booking = any(s in all_links for s in ["/booking", "/termin", "/appointment", "/rezervacia"])
    if not has_portal:
        has_portal = any(s in all_links for s in ["/login", "/portal", "/client"])

    detected_gaps = []
    if allow_negative_inference:
        if not has_booking:
            detected_gaps.append("no_booking")
        if not has_portal:
            detected_gaps.append("no_client_portal")
        if not has_ecommerce:
            detected_gaps.append("no_ecommerce")
        if not has_tracking:
            detected_gaps.append("no_tracking")
        if not has_lead_capture:
            detected_gaps.append("no_lead_capture")

    return {
        "has_booking":       has_booking,
        "has_client_portal": has_portal,
        "has_ecommerce":     has_ecommerce,
        "has_tracking":      has_tracking,
        "has_lead_capture":  has_lead_capture,
        "detected_gaps":     detected_gaps,
        "negative_inference_used": allow_negative_inference,
    }


def detect_language(text: str) -> str:
    """Detect language of website content. Returns ISO 639-1 code or 'unknown'."""
    if not text or len(text.strip()) < 50:
        return "unknown"
    try:
        from langdetect import detect
        return detect(text[:2000])
    except Exception:
        pass
    # Lightweight fallback: count common stopwords
    sample = text.lower()
    scores = {
        "de": sum(sample.count(w) for w in [" die ", " der ", " und ", " ist ", " mit ", " für "]),
        "sk": sum(sample.count(w) for w in [" pre ", " nie ", " ako ", " ale ", " pri ", " som "]),
        "cs": sum(sample.count(w) for w in [" pro ", " ale ", " jak ", " nebo ", " jsou ", " jsme "]),
        "en": sum(sample.count(w) for w in [" the ", " and ", " for ", " with ", " our ", " you "]),
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 2 else "unknown"


def _is_dental_lead(lead: dict) -> bool:
    identity_text = " ".join(
        str(lead.get(key, "") or "")
        for key in ("name", "website", "category")
    )
    content_text = " ".join(
        str(lead.get(key, "") or "")
        for key in ("about_text", "services_text")
    )
    if _DENTAL_STRONG_RE.search(identity_text) or _DENTAL_LOCAL_RE.search(identity_text):
        return True
    if _NON_DENTAL_RE.search(identity_text):
        return False
    if _DENTAL_STRONG_RE.search(content_text) and (
        "clinic" in identity_text.lower() or "smile" in identity_text.lower() or "dent" in identity_text.lower()
    ):
        return True
    if _DENTAL_LOCAL_RE.search(content_text) and "zub" in content_text.lower():
        return True
    return False


def build_dental_opportunity_profile(lead: dict, intel: dict) -> dict:
    text = " ".join([
        str(lead.get("name", "") or ""),
        str(lead.get("about_text", "") or ""),
        str(lead.get("services_text", "") or ""),
        str(lead.get("website", "") or ""),
    ]).lower()

    opportunities = []
    for code, signals, pain_point, outreach_angle in _DENTAL_OPPORTUNITY_RULES:
        if any(signal in text for signal in signals):
            opportunities.append({
                "type": code,
                "pain_point": pain_point,
                "outreach_angle": outreach_angle,
            })

    # Strong positive evidence from the site itself.
    multi_location_markers = sum(text.count(city) for city in ["bratislava", "košice", "kosice", "žilina", "zilina", "piešťany", "piestany", "vienna", "praha", "prague"])
    if multi_location_markers >= 2:
        opportunities.append({
            "type": "multi_location_coordination",
            "pain_point": "Multiple locations appear on the site, which often means fragmented call handling and follow-up.",
            "outreach_angle": "I noticed the clinic appears to operate across multiple locations, so I was curious how enquiries and follow-up are coordinated between them.",
        })

    unique = []
    seen = set()
    for item in opportunities:
        if item["type"] in seen:
            continue
        seen.add(item["type"])
        unique.append(item)

    top = unique[0]["type"] if unique else ""
    top_angle = unique[0]["outreach_angle"] if unique else ""
    return {
        "is_dental": True,
        "opportunities": unique,
        "top_opportunity": top,
        "outreach_angle": top_angle,
    }


# ---------------------------------------------------------------------------
# LLM brand evaluation
# ---------------------------------------------------------------------------

_EVAL_SYSTEM = (
    "You are a sharp B2B sales researcher. "
    "Given a business's website content and detected gaps, write a concise brand profile and evaluate them as a potential client. "
    "Return ONLY valid JSON."
)

_EVAL_SCHEMA = """{
  "brand_summary": "1-2 sentences: what this business actually does and who they serve",
  "apparent_size": "solo | small (2-10) | medium (10-50) | large (50+) | unknown",
  "digital_maturity": "low | medium | high",
  "pain_point_guess": "most likely pain point or gap you can spot from their site",
  "outreach_angle": "one specific, non-generic opening line referencing a real gap or detail from their site — not a generic pitch",
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
    gap_profile = lead.get("_gap_profile", {})
    detected_gaps = gap_profile.get("detected_gaps", [])
    language = lead.get("_language", "unknown")

    prompt = f"""Business: {name}
Category: {category}
Tech stack: {tech}
Social profiles found: {socials}
Website language: {language}
Detected gaps (missing capabilities): {', '.join(detected_gaps) if detected_gaps else 'none detected'}

--- About page ---
{about}

--- Services page ---
{services}

Respond with this exact JSON schema:
{_EVAL_SCHEMA}"""

    if any(
        os.environ.get(name)
        for name in ("OPENROUTER_API_KEY", "GROQ_API_KEY", "GOOGLE_AI_STUDIO_API_KEY", "GOOGLE_AI_VICTOR_API_KEY")
    ):
        try:
            content = complete_text(
                system_prompt=_EVAL_SYSTEM,
                user_prompt=prompt,
                temperature=0.3,
                max_tokens=500,
            )
            result = _parse_eval(content)
            if result:
                return result
        except Exception:
            pass

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
    Returns an intel dict with structured fields including gap_profile.
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
        # Gap profile
        "has_booking": False,
        "has_client_portal": False,
        "has_ecommerce": False,
        "has_tracking": False,
        "has_lead_capture": False,
        "gap_profile": "{}",
        "top_gap": "",
        "language": "unknown",
        "opportunity_profile": "{}",
        "top_opportunity": "",
        "resolved_niche": "",
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
    intel["has_socials"] = bool(socials.strip() and socials not in ("{}", ""))

    # No content at all — weak signal
    about = (lead.get("about_text") or "").strip()
    services = (lead.get("services_text") or "").strip()
    if not about and not services:
        intel["qualification"] = "weak"
        intel["qualification_reason"] = "site reachable but no readable content"
        return intel

    # --- Language detection ---
    intel["language"] = detect_language(about or services)

    # --- Gap detection (rule-based, fast) ---
    # We need the raw HTML — use about+services text as proxy if raw HTML not available
    raw_html = lead.get("_raw_html")
    candidate_links = lead.get("_candidate_links", {})
    text_proxy = about + " " + services
    # Allow negative inference (detecting missing features) when we have sufficient content
    # Raw HTML always allows it; otherwise need at least 50 chars of text
    allow_negative = bool(raw_html) or len(text_proxy) >= 50
    gap_profile = detect_gaps(raw_html or text_proxy, candidate_links, allow_negative_inference=allow_negative)

    intel["has_booking"]       = gap_profile["has_booking"]
    intel["has_client_portal"] = gap_profile["has_client_portal"]
    intel["has_ecommerce"]     = gap_profile["has_ecommerce"]
    intel["has_tracking"]      = gap_profile["has_tracking"]
    intel["has_lead_capture"]  = gap_profile["has_lead_capture"]

    resolved_niche = (lead.get("target_niche") or "").strip()
    if not resolved_niche or resolved_niche == "unknown":
        resolved_niche, _confidence = infer_niche(
            str(lead.get("name", "") or ""),
            str(lead.get("category", "") or ""),
            str(lead.get("query", "") or ""),
            str(lead.get("website", "") or ""),
        )
    intel["resolved_niche"] = resolved_niche

    # Determine top_gap using canonical niche priority, not noisy category labels.
    gap_priority = {
        "dental_medical": ["no_booking", "no_lead_capture", "no_tracking"],
        "beauty_salon": ["no_booking", "no_lead_capture", "no_tracking"],
        "physiotherapy_wellness": ["no_booking", "no_lead_capture", "no_tracking"],
        "real_estate": ["no_lead_capture", "no_tracking"],
        "accounting_tax": ["no_client_portal", "no_lead_capture", "no_tracking"],
        "home_services": ["no_lead_capture", "no_tracking"],
        "local_retail_ecommerce": ["no_ecommerce", "no_tracking", "no_lead_capture"],
        "hospitality_restaurants": ["no_lead_capture", "no_tracking", "no_booking"],
    }.get(resolved_niche, ["no_lead_capture", "no_tracking", "no_client_portal", "no_ecommerce"])

    detected = gap_profile["detected_gaps"]
    top_gap = next((g for g in gap_priority if g in detected), detected[0] if detected else "")
    intel["top_gap"] = top_gap
    gap_profile["top_gap"] = top_gap
    intel["gap_profile"] = json.dumps(gap_profile)

    if _is_dental_lead(lead):
        opp = build_dental_opportunity_profile(lead, intel)
        intel["opportunity_profile"] = json.dumps(opp)
        intel["top_opportunity"] = opp.get("top_opportunity", "")
        # For dental, prefer specific opportunity-driven outreach over generic gap outreach.
        if opp.get("outreach_angle"):
            intel["outreach_angle"] = opp["outreach_angle"]

    # Inject gap info for LLM
    lead["_gap_profile"] = gap_profile
    lead["_language"] = intel["language"]

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
            intel.update(_rule_based_qual(intel))
    else:
        intel.update(_rule_based_qual(intel))

    return intel


def _rule_based_qual(intel: dict) -> dict:
    score = 0
    if intel["has_email"]:      score += 2
    if intel["has_phone"]:      score += 1
    if intel["has_socials"]:    score += 1
    if intel["has_lead_capture"]: score += 1
    qual = "strong" if score >= 4 else "moderate" if score >= 2 else "weak"
    return {"qualification": qual, "qualification_reason": "rule-based: ai unavailable"}


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
        SELECT b.id, b.name, b.category, b.phone, b.website, b.email_maps, b.query, b.target_niche,
               b.validation_status,
               w.about_text, w.services_text, w.emails, w.phones,
               w.site_url, w.socials, w.tech_stack, w.site_status, w.site_error,
               w.raw_html
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
                has_booking=?, has_client_portal=?, has_ecommerce=?,
                has_tracking=?, has_lead_capture=?,
                gap_profile=?, top_gap=?, opportunity_profile=?, top_opportunity=?, target_niche=?,
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
                int(intel["has_booking"]),
                int(intel["has_client_portal"]),
                int(intel["has_ecommerce"]),
                int(intel["has_tracking"]),
                int(intel["has_lead_capture"]),
                intel["gap_profile"],
                intel["top_gap"],
                intel.get("opportunity_profile", "{}"),
                intel.get("top_opportunity", ""),
                intel.get("resolved_niche") or lead.get("target_niche") or None,
                lead["id"],
            ),
        )

        # Also update language on website_data row
        if intel.get("language") and intel["language"] != "unknown":
            conn.execute(
                "UPDATE website_data SET language=? WHERE business_id=?",
                (intel["language"], lead["id"]),
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
        # Gap profile
        ("has_booking",       "INTEGER DEFAULT 0"),
        ("has_client_portal", "INTEGER DEFAULT 0"),
        ("has_ecommerce",     "INTEGER DEFAULT 0"),
        ("has_tracking",      "INTEGER DEFAULT 0"),
        ("has_lead_capture",  "INTEGER DEFAULT 0"),
        ("gap_profile",       "TEXT"),
        ("top_gap",           "TEXT"),
        ("opportunity_profile","TEXT"),
        ("top_opportunity",   "TEXT"),
    ]
    for col, typedef in new_cols:
        if col not in existing:
            conn.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typedef}")
    conn.commit()
