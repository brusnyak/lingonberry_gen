# leadgen — Project Status

Part of the biz automation system. See `../SPEC.md` for full picture.
Own GitHub repo (separate from other mini-projects).

## Status: ~80% complete

## What works

- Google Maps scraper (Playwright, two-phase)
  - Phase 1: scroll feed, collect all card URLs as strings
  - Phase 2: visit each URL directly — no stale element refs
  - Extracts: name, category, rating, reviews, address, phone, website, email (About tab), hours
  - Private-use unicode icons stripped from all fields (phone, category were dirty)
  - Category junk filter (rejects Google review disclaimer text)
  - Automation banner suppressed (ignore_default_args + navigator.webdriver masked)
  - Debug screenshots saved to data/debug/ on failure

- Website scraper (requests + BeautifulSoup)
  - Visits business website, finds about/services/contact subpages
  - Extracts: about text, services text, emails, phones

- LLM enrichment (enrichment/ollama.py)
  - Tries Ollama cloud first (api.ollama.com, ~1.4s/lead, Bearer auth)
  - Auto-falls back to OpenRouter free models if Ollama fails
  - OpenRouter priority: llama-3.3-70b → nemotron-30b → qwen3-80b → mistral-small → gemma-3-27b → llama-3.2-3b
  - Returns: industry, role, icp_fit (0-100), pain_points, outreach_message

- .env loading: main.py loads ../env at startup — OLLAMA_BASE_URL, keys all available

- SQLite storage (storage/db.py)
  - Tables: businesses, website_data, enrichment, query_runs
  - Fields: email_maps, hours, validation_status, validation_notes
  - Auto-migration on init

- Streamlit review UI (ui.py) — approve leads before enrichment, view outreach drafts

- Scripts + Makefile — all relative paths, no hardcoded absolutes

- .gitignore — excludes .venv, data/*.db, data/*.csv, playwright-profile, .env

## Stress test results (this session)

- 60 leads collected across 3 queries (dentists, accountants, web agencies — Bratislava)
- 21 enriched with Ollama cloud gemma3:4b
- Pipeline timing: scrape ~2-3min/20 leads, enrichment ~1.4s/lead via Ollama cloud
- Phone numbers clean, categories clean, outreach drafts coherent
- 1 email captured directly from Maps panel (recepcia@smileclinic.sk)
- Most emails come from website scraper, not Maps panel

## Known issues / next fixes

- category selector still occasionally returns rating number instead of category text
  (Maps DOM varies — needs more selector fallbacks or post-clean digit-only filter)
- email capture from Maps About tab is rare — most businesses don't list it there
- validation_status column exists in DB but validation layer not built yet

## What's next (next chat)

1. Validation layer:
   - dedup by name similarity (not just place_id)
   - filter chains/franchises (multiple listings same phone/website)
   - contact reachability check (email format valid, phone format valid)
   - AI classification: qualified / skip / needs_review
   - extend Streamlit UI to show validation status

2. DuckDuckGo / OpenSerp as second lead source (niche directories, not just Maps)

3. Telegram bot integration:
   - /scrape [query] [max] — trigger a run
   - /status — pipeline state
   - /report — daily summary
   - /export — send CSV

4. Push to GitHub (repo is clean and ready)

## How to run

```bash
cd leadgen
source .venv/bin/activate

# scrape + enrich (Ollama cloud auto-used via ../.env)
python main.py --query "dentists in Bratislava" --max 50 --headless \
  --profile data/playwright-profile --model gemma3:4b

# scrape only, skip LLM
python main.py --query "accountants in Vienna" --max 50 --headless --skip-llm \
  --profile data/playwright-profile

# enrich already-approved leads
python main.py --enrich-approved --model gemma3:4b

# review UI
streamlit run ui.py
```

## Stack

- Python 3.11 (pyenv)
- Playwright 1.50 (Chromium)
- requests + BeautifulSoup4
- SQLite3
- Streamlit
- Ollama cloud (api.ollama.com) + OpenRouter fallback

## Repo structure

```
leadgen/
  main.py              # CLI entrypoint
  ui.py                # Streamlit review UI
  scrapers/
    google_maps.py     # Maps scraper (two-phase)
    website.py         # Website scraper
  enrichment/
    ollama.py          # LLM enrichment (Ollama cloud + OpenRouter fallback)
  storage/
    db.py              # SQLite schema, queries, CSV export
  scripts/             # bash wrappers (run.sh, batch, ui, enrich)
  data/                # leads.db, leads.csv, debug/ — gitignored
  Makefile
  requirements.txt
  .gitignore
  PROJECT.md           # this file
```
