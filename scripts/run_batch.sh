#!/usr/bin/env bash
set -euo pipefail

QUERIES_FILE="${1:-/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/queries.txt}"

python /Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/main.py \
  --queries-file "$QUERIES_FILE" \
  --daily \
  --max 50 \
  --headless \
  --min-delay 1.0 \
  --max-delay 2.5 \
  --profile "/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/playwright-profile"
