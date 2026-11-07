#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_py.sh"
"$PY" -m pytest -v "$@"
