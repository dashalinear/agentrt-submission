# Usage: make setup | test | exp-recovery | exp-events | exp-load | report | all | real-llm | linux | zip
PY ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || ([ -x .venv/Scripts/python ] && echo .venv/Scripts/python || echo python))
export PYTHONPATH := src

.PHONY: setup test exp-recovery exp-events exp-load report all real-llm linux zip clean

setup:
	python -m venv .venv && $(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -v | tee results/pytest_output.txt

exp-recovery:
	$(PY) -m experiments.exp_recovery --reps 5

exp-events:
	$(PY) -m experiments.exp_events --race-reps 300

exp-load:
	$(PY) -m experiments.exp_load --seeds 11,12,13 --policies fifo,drr,drr_iso --scale 0.1

report:
	$(PY) -m experiments.sample_log
	$(PY) -m experiments.make_report

all: clean test exp-recovery exp-events exp-load report

real-llm:
	bash scripts/run_real_llm.sh

linux:  # tests + Part 1-3 + E5 diagnostic in a Linux container (Docker) -> results/raw_linux
	bash scripts/run_linux.sh tests,recovery,events,load,diag

zip:
	bash scripts/make_submission.sh

clean:
	rm -rf results/work results/raw results/figures && mkdir -p results/raw results/figures
