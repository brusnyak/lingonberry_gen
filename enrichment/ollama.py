import json
import os
import time
from typing import Any, Dict, Iterable, List

import requests

# Best free models on OpenRouter (ordered by quality/capacity)
OPENROUTER_FREE_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "qwen/qwen3-next-80b-a3b-instruct:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemma-3-27b-it:free",
    "meta-llama/llama-3.2-3b-instruct:free",
]

SYSTEM_PROMPT = (
    "You are a business research assistant. "
    "Return ONLY a valid JSON object with these keys: "
    "industry (short label), role (likely decision-maker), "
    "icp_fit (integer 0-100), pain_points (short bullet string), "
    "outreach_message (2-3 sentences, friendly, no hype)."
)


def _build_user_payload(business: Dict[str, Any]) -> str:
    return json.dumps({
        "name":          business.get("name"),
        "category":      business.get("category"),
        "address":       business.get("address"),
        "rating":        business.get("rating"),
        "website":       business.get("website"),
        "email":         business.get("email_maps") or business.get("emails", ""),
        "about_text":    (business.get("about_text") or "")[:1500],
        "services_text": (business.get("services_text") or "")[:1500],
    }, ensure_ascii=True)


def _parse_response(content: str) -> Dict[str, str]:
    try:
        clean = content.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
    except json.JSONDecodeError:
        parsed = {"outreach_message": content[:800]}
    return {
        "industry":         str(parsed.get("industry", ""))[:200],
        "role":             str(parsed.get("role", ""))[:200],
        "icp_fit":          str(parsed.get("icp_fit", ""))[:4],
        "pain_points":      str(parsed.get("pain_points", ""))[:500],
        "outreach_message": str(parsed.get("outreach_message", ""))[:800],
    }


def _enrich_ollama(model: str, business: Dict[str, Any], timeout: int = 60) -> Dict[str, str]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": _build_user_payload(business)},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.2},
    }
    base = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    resp = requests.post(f"{base}/api/chat", json=payload, timeout=timeout)
    resp.raise_for_status()
    content = resp.json().get("message", {}).get("content", "{}")
    return _parse_response(content)


def _enrich_openrouter(business: Dict[str, Any], timeout: int = 60) -> Dict[str, str]:
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer":  "https://github.com/biz-leadgen",
        "X-Title":       "leadgen-enrichment",
        "Content-Type":  "application/json",
    }
    last_err: Exception = RuntimeError("no models tried")
    for model in OPENROUTER_FREE_MODELS:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": _build_user_payload(business)},
            ],
            "temperature": 0.2,
        }
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers, json=payload, timeout=timeout,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return _parse_response(content)
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise RuntimeError(f"OpenRouter enrichment failed: {last_err}")


def enrich_business(
    model: str,
    business: Dict[str, Any],
    timeout: int = 60,
    fallback_to_openrouter: bool = True,
) -> Dict[str, str]:
    """Enrich a single business. Tries Ollama first, falls back to OpenRouter."""
    try:
        return _enrich_ollama(model, business, timeout=timeout)
    except Exception as ollama_err:
        if not fallback_to_openrouter:
            raise
        print(f"[enrich] Ollama failed ({ollama_err}), trying OpenRouter...")
        return _enrich_openrouter(business, timeout=timeout)


def enrich_batch(
    model: str,
    leads: Iterable[Dict[str, Any]],
    timeout: int = 60,
) -> List[Dict[str, str]]:
    empty: Dict[str, str] = {
        "industry": "", "role": "", "icp_fit": "", "pain_points": "", "outreach_message": ""
    }
    results: List[Dict[str, str]] = []
    for lead in leads:
        try:
            results.append(enrich_business(model, lead, timeout=timeout))
        except Exception as e:
            print(f"[enrich] Failed for {lead.get('name')}: {e}")
            results.append(empty.copy())
    return results
