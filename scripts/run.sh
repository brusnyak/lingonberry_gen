#!/usr/bin/env bash
set -euo pipefail

QUERY="${1:-}"
if [[ -z "$QUERY" ]]; then
  echo "Usage: ./scripts/run.sh \"dentists in Bratislava\""
  exit 1
fi

python /Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/main.py \
  --query "$QUERY" \
  --max 50 \
  --headless \
  --min-delay 1.0 \
  --max-delay 2.5 \
  --profile "/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/playwright-profile"
