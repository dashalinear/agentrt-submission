#!/usr/bin/env bash
# Clean state -> tests -> all experiments -> figures + REPORT.md/.pdf
set -euo pipefail
source "$(dirname "$0")/_py.sh"
rm -rf results/work results/raw results/figures
mkdir -p results/raw results/figures
"$PY" -m pytest -v | tee results/pytest_output.txt
"$PY" -m experiments.exp_recovery --reps 10
"$PY" -m experiments.exp_events --race-reps 300
"$PY" -m experiments.exp_load --seeds 11,12,13,14,15 --policies fifo,drr,drr_iso --scale 0.1
"$PY" -m experiments.exp_load --seeds 11,12,13,14,15 --policies drr --approval-share 0.5 --approval-delay 20 --prefix loadappr_ --scale 0.1
"$PY" -m experiments.diag_e5_polling
"$PY" -m experiments.sample_log
"$PY" -m experiments.make_report
