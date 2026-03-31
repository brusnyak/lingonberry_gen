import json
import os
import sys
from typing import Any, Dict, Iterable, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from agent.remote_models import complete_text

SYSTEM_PROMPT = (
    "You are a B2B sales researcher helping a small agency find clients. "
    "Return ONLY a valid JSON object with these exact keys: "
    '"industry", "role", "icp_fit", "pain_points", "outreach_message".\n\n'
    "Rules:\n"
    "- industry: short label (max 4 words)\n"
    "- role: most likely decision-maker title (e.g. 'Marketing Director', 'Owner')\n"
    "- icp_fit: integer 0–100 how well this business matches our ideal customer profile\n"
    "- pain_points: 2–3 short bullet points as a single string (use • or -)\n"
    "- outreach_message: short, casual, human-sounding first-touch message (2–3 sentences max, within 50-60 words). "
    "Reference something specific from their business. "
    "No buzzwords, no 'hope this finds you well', no generic compliments, no hype.\n\n"
    "Output valid JSON only — no explanation, no markdown, no code fences."
)


def _build_user_payload(business: Dict[str, Any]) -> str:
    name = business.get("name", "").strip()
    category = business.get("category", "").strip()
    address = business.get("address", "").strip()
    website = business.get("website", "").strip()
    services_text = (business.get("services_text") or business.get("description") or "").strip()[:1400]

    # Try to give the model useful context without overwhelming it
    parts = []
    if name:
        parts.append(f"Business name: {name}")
    if category:
        parts.append(f"Category: {category}")
    if address:
        parts.append(f"Location: {address}")
    if website:
        parts.append(f"Website: {website}")
    if services_text:
        parts.append(f"About / services:\n{services_text}")

    return "\n\n".join(parts) or "[No meaningful business data available]"


def _parse_response(content: str) -> Dict[str, str]:
    content = content.strip()

    # Strip common markdown fences people/models like to add
    if content.startswith("```json"):
        content = content[7:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    content = content.strip()

    default = {
        "industry": "",
        "role": "",
        "icp_fit": "0",
        "pain_points": "",
        "outreach_message": "",
    }

    try:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            return default

        return {
            "industry": str(parsed.get("industry", "")).strip()[:180],
            "role": str(parsed.get("role", "")).strip()[:120],
            "icp_fit": str(parsed.get("icp_fit", "0")).strip()[:4],
            "pain_points": str(parsed.get("pain_points", "")).strip()[:600],
            "outreach_message": str(parsed.get("outreach_message", "")).strip()[:900],
        }
    except json.JSONDecodeError:
        # Last resort — try to salvage message if it's plain text
        if len(content) > 40 and "{" not in content[:10]:
            return {**default, "outreach_message": content[:850]}
        return default


def _enrich_remote(
    business: Dict[str, Any],
    timeout: int = 50,
) -> Dict[str, str]:
    del timeout  # request timeout is managed inside the shared provider client
    content = complete_text(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=_build_user_payload(business),
        temperature=0.15,
        max_tokens=400,
    )
    return _parse_response(content)


def enrich_business(
    model: str,
    business: Dict[str, Any],
    timeout: int = 70,
    prefer_ollama: bool = False,  # Changed default to False - use OpenRouter first
    fallback_to_openrouter: bool = True,
) -> Dict[str, str]:
    """
    Enrich one business record with remote providers only.
    Legacy flags are kept for CLI compatibility.
    """
    if fallback_to_openrouter:
        try:
            return _enrich_remote(business, timeout=timeout)
        except Exception as e:
            if prefer_ollama:
                print("[enrich] prefer_ollama was requested but local inference is now disabled; using remote providers only.")
            print(f"[enrich] Remote enrichment failed: {e}")
            raise

    raise RuntimeError("No enrichment backend succeeded")


def enrich_batch(
    model: str = "llama3.2:3b",
    leads: Iterable[Dict[str, Any]] = (),
    timeout: int = 70,
    prefer_ollama: bool = True,
    fallback_to_openrouter: bool = True,
    skip_failed: bool = False,
) -> List[Dict[str, str]]:
    """
    Enrich multiple leads. Returns list of enrichment dicts (same length as input).
    """
    results = []
    empty = {
        "industry": "",
        "role": "",
        "icp_fit": "0",
        "pain_points": "",
        "outreach_message": "",
    }

    for lead in leads:
        name = lead.get("name", "unknown")
        try:
            enriched = enrich_business(
                model=model,
                business=lead,
                timeout=timeout,
                prefer_ollama=prefer_ollama,
                fallback_to_openrouter=fallback_to_openrouter,
            )
            results.append(enriched)
            print(f"[enrich] OK  → {name}")
        except Exception as e:
            print(f"[enrich] FAILED → {name}: {e}")
            if skip_failed:
                results.append(empty.copy())
            else:
                raise

    return results
