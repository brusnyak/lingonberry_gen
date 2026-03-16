#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

QUERIES_FILE="${1:-$ROOT/data/queries.txt}"

python "$ROOT/main.py" \
  --queries-file "$QUERIES_FILE" \
  --daily \
  --max 50 \
  --headless \
  --workers 6 \
  --min-delay 1.5 \
  --max-delay 4.0 \
  --slow-mo 150 \
  --profile "$ROOT/data/playwright-profile"
