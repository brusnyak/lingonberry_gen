# leadgen — Project Status

## What this is

Part of a larger personal automation agency system (see `../SPEC.md`).
Goal: collect leads from public sources, enrich them with LLM, validate quality,
and produce a clean qualified list ready for automated outreach.

## Current State: ~75% complete

### Done

- Google Maps scraper (Playwright)
  - Two-phase approach: scroll feed to collect all card URLs, then visit each directly
  - Extracts: name, category, rating, reviews, address, phone, website, email (from About tab + panel scan), hours
  - Automation banner suppressed (`ignore_default_args`, `navigator.webdriver` masked)
  - Junk/sponsored card filtering
  - Debug screenshots on failure saved to `data/debug/`

- Website scraper (requests + BeautifulSoup)
  - Visits business website, finds about/services/contact pages
  - Extracts: about text, services text, emails, phones

- LLM enrichment (`enrichment/ollama.py`)
  - Tries local Ollama first, auto-falls back to OpenRouter free models
  - OpenRouter model priority: llama-3.3-70b → nemotron-30b → qwen3-80b → mistral-small → gemma-3-27b → llama-3.2-3b
  - Returns: industry, role, icp_fit (0-100), pain_points, outreach_message

- SQLite storage (`storage/db.py`)
  - Tables: businesses, website_data, enrichment, query_runs
  - Columns include: email_maps, hours, validation_status, validation_notes
  - Auto-migration on init

- Streamlit review UI (`ui.py`)
  - Approve/reject leads before enrichment
  - View outreach drafts

- Scripts + Makefile — all paths relative, no hardcoded absolute paths

### Not yet built (next steps)

1. Validation layer — AI-driven dedup, chain/franchise filter, contact reachability check,
   auto-assigns status: `qualified` / `skip` / `needs_review`
2. DuckDuckGo / OpenSerp web search source — find niche directories and local biz sites
   beyond Google Maps
3. Telegram bot integration — remote trigger scrape runs, get daily reports
4. OpenRouter enrichment needs `OPENROUTER_API_KEY` in `../.env` (already set)

## Stack

- Python 3.11
- Playwright (Chromium) — browser automation
- requests + BeautifulSoup — website scraping
- SQLite — storage
- Streamlit — review UI
- Ollama (local) + OpenRouter (cloud fallback) — LLM enrichment

## API Keys (stored in `../.env`)

- `OLLAMA_BASE_URL` + `OLLAMA_API_KEY` — local Ollama instance
- `OPENROUTER_API_KEY` — primary cloud LLM (free models)
- `GOOGLE_AI_STUDIO_API_KEY` — Gemini (available for future use)
- `HF_API_KEY` — HuggingFace (available for embeddings/classification)
- `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` — bot wired, not yet integrated

## How to run

```bash
cd leadgen
source .venv/bin/activate

# single query
python main.py --query "dentists in Bratislava" --max 50 --headless --profile data/playwright-profile

# skip LLM enrichment (scrape only)
python main.py --query "accountants in Vienna" --max 50 --headless --skip-llm --profile data/playwright-profile

# enrich already-approved leads
python main.py --enrich-approved --model llama3.1

# review UI
streamlit run ui.py
```

## Bigger picture (from `../SPEC.md`)

```
leadgen  →  outreach  →  planning
```

- `outreach/` — send personalized emails/LinkedIn DMs to qualified leads,
  listen for replies, extract pain points, daily Telegram report
- `planning/` — universal biz thinking tool: niche validation, offer building,
  agent task planner, ops Q&A

The end goal is a Telegram-controlled agent that runs the full cycle autonomously:
scrape → enrich → validate → outreach → parse replies → report insights.
You only step in to review qualified leads and make final decisions.

## Agentic layer (future)

A central agent controller (`../agent/`) will:
- Accept commands via Telegram bot
- Route tasks to the right tool (leadgen, outreach, planning)
- Use OpenRouter for reasoning/planning
- Use Gemini for web research
- Maintain state in SQLite
- Send daily summaries back to Telegram

This is the "OpenClaw-lite" described in `../notes/`.
