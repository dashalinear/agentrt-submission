#!/usr/bin/env bash
# Clean state -> tests -> all experiments -> figures + REPORT.md/.pdf
set -euo pipefail
source "$(dirname "$0")/_py.sh"
rm -rf results/work results/raw results/figures
mkdir -p results/raw results/figures
"$PY" -m pytest -v | tee results/pytest_output.txt
"$PY" -m experiments.exp_recovery --reps 5
"$PY" -m experiments.exp_events --race-reps 300
"$PY" -m experiments.exp_load --seeds 11,12,13 --policies fifo,drr,drr_iso --scale 0.1
"$PY" -m experiments.sample_log
"$PY" -m experiments.make_report
