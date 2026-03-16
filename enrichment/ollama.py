import json
import os
import time
from typing import Any, Dict, Iterable, List

import requests

OPENROUTER_FREE_MODELS = [
    "mistralai/mistral-7b-instruct:free",
    "google/gemma-2-9b-it:free",           # updated — gemma-3 doesn't exist yet
    "meta-llama/llama-3.2-3b-instruct:free",
    "qwen/qwen2.5-7b-instruct:free",       # usually very good & free
]

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


def _enrich_ollama(
    model: str,
    business: Dict[str, Any],
    timeout: int = 70,
) -> Dict[str, str]:
    base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_payload(business)},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.15,
            "top_p": 0.9,
        },
    }

    try:
        resp = requests.post(
            f"{base_url}/api/chat",
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data.get("message", {}).get("content", "{}")
        return _parse_response(content)
    except requests.RequestException as e:
        raise RuntimeError(f"Ollama request failed: {e}")


def _enrich_openrouter(
    business: Dict[str, Any],
    timeout: int = 50,
) -> Dict[str, str]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Environment variable OPENROUTER_API_KEY is not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://github.com/yourusername/leadgen",  # change to your repo if public
        "X-Title": "Lead Enrichment Script",
        "Content-Type": "application/json",
    }

    user_content = _build_user_payload(business)

    for model in OPENROUTER_FREE_MODELS:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.15,
            "max_tokens": 400,
        }

        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return _parse_response(content)

        except Exception as e:
            print(f"[openrouter] Model {model} failed: {e.__class__.__name__}")
            time.sleep(1.5)

    raise RuntimeError("All OpenRouter free models failed")


def enrich_business(
    model: str,
    business: Dict[str, Any],
    timeout: int = 70,
    prefer_ollama: bool = True,
    fallback_to_openrouter: bool = True,
) -> Dict[str, str]:
    """
    Enrich one business record with LLM classification & outreach message.
    Tries Ollama first (if prefer_ollama=True), then OpenRouter.
    """
    if prefer_ollama:
        try:
            return _enrich_ollama(model, business, timeout=timeout)
        except Exception as e:
            print(f"[enrich] Ollama failed for '{business.get('name', '?')}': {e}")
            if not fallback_to_openrouter:
                raise

    if fallback_to_openrouter:
        try:
            return _enrich_openrouter(business, timeout=timeout)
        except Exception as e:
            print(f"[enrich] OpenRouter also failed: {e}")
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