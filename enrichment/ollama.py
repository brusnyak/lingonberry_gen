import json
import time
from typing import Dict, Any

import requests


def enrich_business(model: str, business: Dict[str, Any], timeout: int = 60) -> Dict[str, str]:
    system = (
        "You are a business research assistant. Return a JSON object with keys: "
        "classification, pain_points, outreach_message. "
        "classification: short category label. "
        "pain_points: short bullet-like string. "
        "outreach_message: 2-3 sentences, friendly, no hype."
    )

    user = {
        "name": business.get("name"),
        "category": business.get("category"),
        "address": business.get("address"),
        "rating": business.get("rating"),
        "website": business.get("website"),
        "about_text": business.get("about_text", "")[:1500],
        "services_text": business.get("services_text", "")[:1500],
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=True)},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.2},
    }

    resp = requests.post("http://127.0.0.1:11434/api/chat", json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    content = data.get("message", {}).get("content", "{}").strip()

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = {
            "classification": "",
            "pain_points": "",
            "outreach_message": content[:800],
        }

    return {
        "classification": str(parsed.get("classification", ""))[:200],
        "pain_points": str(parsed.get("pain_points", ""))[:500],
        "outreach_message": str(parsed.get("outreach_message", ""))[:800],
    }
