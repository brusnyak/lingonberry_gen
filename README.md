# Hybrid Lead Research Tool

Lightweight, local-first research assistant for collecting public business info from Google Maps and company websites, then enriching with a local LLM via Ollama.

## What It Does
- Searches Google Maps for a query
- Collects business name, category, website, phone, address, rating
- Visits the business website (if present) to extract about/services/contact info
- Runs local LLM enrichment to classify, guess pain points, and draft outreach
- Exports to CSV for manual outreach

## Setup

```bash
make install
make browser
```

Make sure Ollama is running locally.

## Usage

Single query:
```bash
make run q="dentists in Bratislava"
```

Batch queries:
```bash
# one query per line in data/queries.txt
make batch qf=/Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/data/queries.txt
```

Review UI:
```bash
make ui
```

Enrich approved leads (after you approve in the UI):
```bash
python main.py --enrich-approved --model llama3.1
make enrich m=llama3.1
```

Outputs:
- SQLite: `data/leads.db`
- CSV: `data/leads.csv`

## Notes
- This tool only accesses public listings and public websites.
- Be mindful of Google Maps and website terms of service and rate limits.
- Use low volume and reasonable delays to avoid blocks.
- For session persistence (cookies), the scraper uses a Playwright profile dir.

## Example Flags

```bash
python main.py --query "accountants in Vienna" --max 100 --model llama3.1
python main.py --queries-file data/queries.txt --daily --max 50 --headless
```
