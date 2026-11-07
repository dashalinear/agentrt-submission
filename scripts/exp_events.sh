#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_py.sh"
"$PY" -m experiments.exp_events --race-reps 300 "$@"
