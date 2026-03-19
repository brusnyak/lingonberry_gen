"""
leadgen/niches.py
Canonical niche assignment and research scoring helpers.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone


NICHES: dict[str, dict] = {
    "dental_medical": {
        "label": "Dental / Medical",
        "sample_market": "Broader EU local clinics and health practices",
        "common_pains": [
            "missed calls and slow intake follow-up",
            "manual appointment confirmations and recalls",
            "high-value treatment enquiry leakage",
            "review capture and patient reactivation",
        ],
        "pain_detectability": 4.5,
        "contactability": 4.0,
        "ability_to_deliver": 4.5,
        "price_tolerance": 4.2,
        "content_leverage": 3.6,
        "outreach_channel_fit": ["email", "instagram", "facebook", "form"],
        "external_evidence": "Health providers often depend on fast follow-up, reminders, and retention workflows.",
        "patterns": [
            r"\bdent",
            r"\bdental\b",
            r"\bclinic\b",
            r"\bzahn",
            r"\bmedical\b",
            r"\bphysio",
            r"\bimplant",
            r"\borthodont",
            r"\bdoctor\b",
            r"\bmedic",
        ],
    },
    "real_estate": {
        "label": "Real Estate",
        "sample_market": "Broader EU agencies, realtors, brokers, property firms",
        "common_pains": [
            "slow response to listing enquiries",
            "manual lead qualification and follow-up",
            "weak personal-brand and social proof systems",
            "poor pipeline visibility across channels",
        ],
        "pain_detectability": 4.2,
        "contactability": 4.1,
        "ability_to_deliver": 4.3,
        "price_tolerance": 4.4,
        "content_leverage": 4.4,
        "outreach_channel_fit": ["email", "instagram", "facebook", "linkedin"],
        "external_evidence": "High-speed follow-up and personal-brand credibility are recurring leverage points for agents.",
        "patterns": [
            r"\breal estate\b",
            r"\brealtor\b",
            r"\bproperty\b",
            r"\bimmobil",
            r"\bbroker\b",
            r"\brealty\b",
        ],
    },
    "accounting_tax": {
        "label": "Accounting / Tax",
        "sample_market": "Broader EU accounting, tax, bookkeeping firms",
        "common_pains": [
            "manual client intake and document collection",
            "back-and-forth over email for onboarding",
            "poor content visibility for trust and SEO",
            "slow qualification of inbound leads",
        ],
        "pain_detectability": 4.1,
        "contactability": 4.3,
        "ability_to_deliver": 4.4,
        "price_tolerance": 4.0,
        "content_leverage": 3.3,
        "outreach_channel_fit": ["email", "linkedin", "form"],
        "external_evidence": "Service delivery often involves repetitive admin and secure-intake friction.",
        "patterns": [
            r"\baccount",
            r"\btax\b",
            r"\bbookkeep",
            r"\bsteuer",
            r"\búčt",
            r"\bucet",
            r"\bda[nň]",
            r"\baudit\b",
        ],
    },
    "beauty_salon": {
        "label": "Beauty / Barber / Salon",
        "sample_market": "Broader EU salons, barbershops, beauty studios",
        "common_pains": [
            "missed booking opportunities",
            "manual reminder and repeat-visit follow-up",
            "inconsistent content and social activity",
            "weak review and referral loops",
        ],
        "pain_detectability": 4.3,
        "contactability": 4.2,
        "ability_to_deliver": 4.1,
        "price_tolerance": 3.2,
        "content_leverage": 4.6,
        "outreach_channel_fit": ["instagram", "facebook", "email"],
        "external_evidence": "These businesses live on repeat booking and visible social proof.",
        "patterns": [
            r"\bbarber",
            r"\bsalon\b",
            r"\bbeauty\b",
            r"\bnail\b",
            r"\bhair\b",
            r"\bspa\b",
            r"\bbrow\b",
        ],
    },
    "physiotherapy_wellness": {
        "label": "Physiotherapy / Wellness",
        "sample_market": "Broader EU physio, rehab, wellness providers",
        "common_pains": [
            "manual booking and follow-up",
            "reactivation of past clients",
            "package or session retention tracking",
            "limited content trust signals",
        ],
        "pain_detectability": 4.0,
        "contactability": 4.0,
        "ability_to_deliver": 4.3,
        "price_tolerance": 3.6,
        "content_leverage": 3.8,
        "outreach_channel_fit": ["email", "instagram", "facebook", "form"],
        "external_evidence": "Repeat attendance and reactivation are valuable in therapy and wellness businesses.",
        "patterns": [
            r"\bphysio",
            r"\btherapy\b",
            r"\brehab\b",
            r"\bwellness\b",
            r"\bmassage\b",
            r"\bchiro",
        ],
    },
    "home_services": {
        "label": "Home Services",
        "sample_market": "Broader EU trades and local home-service businesses",
        "common_pains": [
            "missed calls and untracked quote requests",
            "manual follow-up on estimates",
            "poor job pipeline visibility",
            "thin web presence and weak trust signals",
        ],
        "pain_detectability": 3.8,
        "contactability": 3.9,
        "ability_to_deliver": 4.5,
        "price_tolerance": 4.0,
        "content_leverage": 3.0,
        "outreach_channel_fit": ["email", "facebook", "form"],
        "external_evidence": "Lead response and quote follow-up are common operational choke points.",
        "patterns": [
            r"\bplumb",
            r"\belectric",
            r"\broof",
            r"\bclean",
            r"\bhvac\b",
            r"\bconstruction\b",
            r"\brenov",
            r"\binstall",
        ],
    },
    "local_retail_ecommerce": {
        "label": "Local Retail / E-commerce",
        "sample_market": "Broader EU local shops and small retail brands",
        "common_pains": [
            "weak repeat-purchase follow-up",
            "manual catalog or product-content upkeep",
            "poor tracking and reporting",
            "limited owned-channel capture",
        ],
        "pain_detectability": 3.9,
        "contactability": 3.7,
        "ability_to_deliver": 4.1,
        "price_tolerance": 3.5,
        "content_leverage": 4.0,
        "outreach_channel_fit": ["email", "instagram", "facebook"],
        "external_evidence": "Retail operators benefit from tracking, reactivation, and content systems.",
        "patterns": [
            r"\bboutique\b",
            r"\bshop\b",
            r"\bstore\b",
            r"\be-?commerce\b",
            r"\bjewel",
            r"\bflorist\b",
            r"\bgift\b",
        ],
    },
    "hospitality_restaurants": {
        "label": "Hospitality / Restaurants",
        "sample_market": "Broader EU restaurants, cafes, hospitality venues",
        "common_pains": [
            "manual reservation and event follow-up",
            "weak repeat-visit or list-building systems",
            "reputation management inconsistency",
            "thin campaign reporting",
        ],
        "pain_detectability": 3.5,
        "contactability": 3.6,
        "ability_to_deliver": 3.8,
        "price_tolerance": 3.1,
        "content_leverage": 4.3,
        "outreach_channel_fit": ["instagram", "facebook", "email"],
        "external_evidence": "Reputation, repeat visits, and reservations create automation opportunities.",
        "patterns": [
            r"\brestaurant\b",
            r"\bcafe\b",
            r"\bcoffee\b",
            r"\bbistro\b",
            r"\bbar\b",
            r"\bhotel\b",
        ],
    },
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_signal_score(total: int, qualified: int, contactable: int) -> float:
    raw = (min(total, 25) / 25.0) * 2.5 + (min(qualified, 15) / 15.0) * 4.0 + (min(contactable, 15) / 15.0) * 3.5
    return round(raw, 2)


def infer_niche(name: str = "", category: str = "", query: str = "", website: str = "") -> tuple[str, float]:
    haystack = " ".join(part for part in [name, category, query, website] if part).lower()
    if not haystack.strip():
        return "unknown", 0.0

    scores: dict[str, int] = {}
    for slug, meta in NICHES.items():
        score = 0
        for pattern in meta["patterns"]:
            if re.search(pattern, haystack):
                score += 1
        if score:
            scores[slug] = score

    if not scores:
        return "unknown", 0.0

    winner, winner_score = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0]
    confidence = min(1.0, 0.45 + (winner_score * 0.15))
    return winner, round(confidence, 2)


def ensure_niche_research_seed(conn: sqlite3.Connection) -> None:
    now = _now()
    for slug, meta in NICHES.items():
        conn.execute(
            """
            INSERT INTO niche_research (
                niche, sample_market, common_pains, pain_detectability, contactability,
                ability_to_deliver, price_tolerance, content_leverage, outreach_channel_fit,
                repo_evidence, external_evidence, notes, score, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?)
            ON CONFLICT(niche) DO NOTHING
            """,
            (
                slug,
                meta["sample_market"],
                json.dumps(meta["common_pains"]),
                meta["pain_detectability"],
                meta["contactability"],
                meta["ability_to_deliver"],
                meta["price_tolerance"],
                meta["content_leverage"],
                json.dumps(meta["outreach_channel_fit"]),
                json.dumps({}),
                meta["external_evidence"],
                "",
                0.0,
                now,
                now,
            ),
        )


def refresh_business_niches(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT id, name, category, query, website, target_niche, niche_confidence
        FROM businesses
        """
    ).fetchall()
    changed = 0
    for row in rows:
        niche, confidence = infer_niche(
            row["name"] or "",
            row["category"] or "",
            row["query"] or "",
            row["website"] or "",
        )
        if row["target_niche"] != niche or (row["niche_confidence"] or 0) != confidence:
            conn.execute(
                "UPDATE businesses SET target_niche=?, niche_confidence=? WHERE id=?",
                (niche, confidence, row["id"]),
            )
            changed += 1
    return changed


def refresh_niche_scores(conn: sqlite3.Connection) -> None:
    now = _now()
    for slug, meta in NICHES.items():
        lead_counts = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE validation_status='qualified') AS qualified,
                COUNT(*) FILTER (
                    WHERE validation_status='qualified'
                      AND (
                        COALESCE(email_maps, '') <> ''
                        OR EXISTS (
                            SELECT 1 FROM website_data w
                            WHERE w.business_id = businesses.id
                              AND (
                                COALESCE(w.emails, '') <> ''
                                OR COALESCE(w.instagram_url, '') <> ''
                                OR COALESCE(w.facebook_url, '') <> ''
                              )
                        )
                      )
                ) AS contactable
            FROM businesses
            WHERE target_niche=?
            """,
            (slug,),
        ).fetchone()

        heuristics = [
            meta["pain_detectability"],
            meta["contactability"],
            meta["ability_to_deliver"],
            meta["price_tolerance"],
            meta["content_leverage"],
        ]
        heuristic_score = (sum(heuristics) / len(heuristics)) * 2.0
        repo_signal = _repo_signal_score(
            int(lead_counts["total"] or 0),
            int(lead_counts["qualified"] or 0),
            int(lead_counts["contactable"] or 0),
        )
        final_score = round((heuristic_score * 0.65) + (repo_signal * 0.35), 2)

        repo_evidence = {
            "total_leads": int(lead_counts["total"] or 0),
            "qualified_leads": int(lead_counts["qualified"] or 0),
            "contactable_qualified_leads": int(lead_counts["contactable"] or 0),
        }

        conn.execute(
            """
            UPDATE niche_research
            SET repo_evidence=?,
                score=?,
                updated_at=?
            WHERE niche=?
            """,
            (json.dumps(repo_evidence), final_score, now, slug),
        )


def refresh_niche_validation(conn: sqlite3.Connection) -> None:
    now = _now()
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    has_outreach = "outreach_log" in tables and "replies" in tables and "reply_classification" in tables
    for slug in NICHES:
        if has_outreach:
            row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT b.id) AS leads_count,
                    COUNT(DISTINCT CASE WHEN b.validation_status='qualified' THEN b.id END) AS qualified_count,
                    COUNT(DISTINCT CASE WHEN o.status='sent' THEN o.id END) AS contacted_count,
                    COUNT(DISTINCT r.id) AS replies_count,
                    COUNT(DISTINCT CASE WHEN rc.label='interested' THEN r.id END) AS interested_count
                FROM businesses b
                LEFT JOIN outreach_log o ON o.lead_id = b.id
                LEFT JOIN replies r ON r.lead_id = b.id
                LEFT JOIN reply_classification rc ON rc.reply_id = r.id
                WHERE b.target_niche=?
                """,
                (slug,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT b.id) AS leads_count,
                    COUNT(DISTINCT CASE WHEN b.validation_status='qualified' THEN b.id END) AS qualified_count,
                    0 AS contacted_count,
                    0 AS replies_count,
                    0 AS interested_count
                FROM businesses b
                WHERE b.target_niche=?
                """,
                (slug,),
            ).fetchone()
        conn.execute(
            """
            INSERT INTO niche_validation (
                niche, leads_count, qualified_count, contacted_count, replies_count,
                interested_count, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(niche) DO UPDATE SET
                leads_count=excluded.leads_count,
                qualified_count=excluded.qualified_count,
                contacted_count=excluded.contacted_count,
                replies_count=excluded.replies_count,
                interested_count=excluded.interested_count,
                updated_at=excluded.updated_at
            """,
            (
                slug,
                int(row["leads_count"] or 0),
                int(row["qualified_count"] or 0),
                int(row["contacted_count"] or 0),
                int(row["replies_count"] or 0),
                int(row["interested_count"] or 0),
                now,
            ),
        )


def shortlist_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM niche_research WHERE status='shortlisted'"
    ).fetchone()
    return bool(row["n"])
