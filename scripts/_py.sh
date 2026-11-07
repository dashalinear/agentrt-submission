# sourced by scripts/*.sh: picks the venv interpreter and sets PYTHONPATH
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [ -x .venv/bin/python ]; then PY=.venv/bin/python; elif [ -x .venv/Scripts/python ]; then PY=.venv/Scripts/python; else PY=python; fi
export PYTHONPATH=src
