#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_py.sh"
"$PY" -m experiments.exp_load --seeds 11,12,13,14,15 --policies fifo,drr,drr_iso --scale 0.1 "$@"
