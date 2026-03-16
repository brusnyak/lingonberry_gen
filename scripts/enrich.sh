#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

MODEL="${1:-llama3.1}"

python "$ROOT/main.py" \
  --enrich-approved \
  --model "$MODEL"
