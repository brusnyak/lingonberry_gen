#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

QUERY="${1:-}"
if [[ -z "$QUERY" ]]; then
  echo "Usage: ./scripts/run.sh \"dentists in Bratislava\""
  exit 1
fi

python "$ROOT/main.py" \
  --query "$QUERY" \
  --max 50 \
  --headless \
  --workers 6 \
  --min-delay 1.5 \
  --max-delay 4.0 \
  --slow-mo 150 \
  --profile "$ROOT/data/playwright-profile"
