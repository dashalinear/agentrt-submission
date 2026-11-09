#!/usr/bin/env bash
# Part 1 recovery experiment inside a Linux container (real POSIX SIGKILL) -> results/raw_linux/
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -q -t agentrt:local . >/dev/null
mkdir -p results/raw_linux
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/w/src:/w \
  -v "$PWD:/w:ro" -v "$PWD/results/raw_linux:/out" --tmpfs /w/results/work -w /w \
  agentrt:local python -m experiments.exp_recovery --reps 5 --out /out
