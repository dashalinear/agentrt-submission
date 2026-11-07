#!/usr/bin/env bash
# One command real-LLM run. Configure via .env (see .env.example) or env vars:
#   OPENAI_API_KEY, OPENAI_BASE_URL (default https://api.openai.com/v1), MODEL (default gpt-4o-mini)
# Ollama:  OPENAI_API_KEY=ollama OPENAI_BASE_URL=http://localhost:11434/v1 MODEL=llama3.1 bash scripts/run_real_llm.sh
# Output: results/real_llm/summary.json, calls.jsonl, recovery_runs_real.csv (never mixed with results/raw)
set -euo pipefail
source "$(dirname "$0")/_py.sh"
"$PY" -m experiments.real_llm_run "$@"
"$PY" -m experiments.make_report
