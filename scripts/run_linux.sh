#!/usr/bin/env bash
# Tests + Part 1/2/3 experiments inside a Linux container (real POSIX SIGKILL) -> results/raw_linux/
# Usage: scripts/run_linux.sh [tests,recovery,events,load]
set -euo pipefail
cd "$(dirname "$0")/.."
PARTS="${1:-tests,recovery,events,load}"
docker build -q -t agentrt:local . >/dev/null
mkdir -p results/raw_linux
run() {
  docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/w/src:/w \
    -v "$PWD:/w:ro" -v "$PWD/results/raw_linux:/out" --tmpfs /w/results/work -w /w \
    agentrt:local sh -c "$1"
}
for p in ${PARTS//,/ }; do
  echo "== linux: $p"
  case "$p" in
    tests)    run "python -m pytest -v -p no:cacheprovider > /out/pytest_output.txt 2>&1; tail -1 /out/pytest_output.txt" ;;
    recovery) run "python -m experiments.exp_recovery --reps 5 --out /out" ;;
    events)   run "python -m experiments.exp_events --race-reps 300 --out /out" ;;
    load)     run "python -m experiments.exp_load --seeds 11,12,13 --policies fifo,drr,drr_iso --scale 0.1 --out /out" ;;
  esac
done
