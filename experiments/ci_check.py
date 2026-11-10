"""Guarantee checks over experiment outputs (used by CI on a separate Linux VM, also runnable locally).

  python -m experiments.ci_check <results_dir>

Fails (exit 1) if any guarantee is violated; prints a Markdown summary (appended to $GITHUB_STEP_SUMMARY in CI).
"""
from __future__ import annotations

import os
import sys

from experiments.report_data import events, recovery

EXPECTED_FINAL = {"s4_transient_repeated": "FAILED"}  # every other scenario must end SUCCEEDED


def check(base: str) -> tuple[list[str], list[str]]:
    lines, errors = [], []
    rec = recovery(base)
    if rec["cases"]:
        lines += ["### Part 1 — recovery (real SIGKILL)", "",
                  "| scenario | N | final | repeated LLM | checkpointed steps re-executed | publish phys / logical | logical duplicates |",
                  "|---|---|---|---|---|---|---|"]
        for c in rec["cases"]:
            want = EXPECTED_FINAL.get(c["case"], "SUCCEEDED")
            ok_final = c["final"] == {want: c["n"]}
            lines.append(f"| {c['case']} | {c['n']} | {c['final']} | {c['rep_llm']} | {c['reexec']} | "
                         f"{c['phys']} / {c['logical']} | {c['dups']} |")
            if not ok_final:
                errors.append(f"{c['case']}: expected all {want}, got {c['final']}")
            if c["reexec"]:
                errors.append(f"{c['case']}: {c['reexec']} checkpointed steps re-executed")
            if c["dups"]:
                errors.append(f"{c['case']}: {c['dups']} duplicate logical publications")
            if c["case"].startswith("s2") and c["rep_llm"] > c["n"]:
                errors.append(f"{c['case']}: interrupted LLM call repeated more than once per run")
    ev = events(base)
    s = ev["summary"]
    if s:
        from experiments.make_report import _e5_conformance

        inv_keys = ("invalid_transitions", "terminal_changes", "version_gaps", "multiple_terminal", "log_state_mismatch")
        viol = {k: sum(s[k]["invariants"][x] for x in inv_keys) for k in ("e1", "e2", "e3", "e4", "e5", "e7")}
        rule = sum(_e5_conformance(ev["e5"])[0].values()) if ev["e5"] else 0
        lines += ["", "### Part 2 — event lifecycle", "",
                  f"- E1 final: {s['e1']['final']}",
                  f"- E2 final: {s['e2']['final']}, steps during wait: {s['e2']['step_runs_during_wait_total']}",
                  f"- E3: applied {s['e3']['applied']}, duplicates {s['e3']['duplicates_detected']}, "
                  f"jobs with >1 transition {s['e3']['jobs_with_more_than_one_transition']}",
                  f"- E4: late events {s['e4']['late_events']}, state changed {s['e4']['state_changed']}",
                  f"- E5: final {s['e5']['final']}, orphan publications {s['e5']['orphan_publications']}, "
                  f"race-rule violations {rule}",
                  f"- invariant violations per experiment: {viol}"]
        if any(viol.values()):
            errors.append(f"invariant violations: {viol}")
        if rule:
            errors.append(f"E5 race-rule violations: {rule}")
        if s["e3"]["jobs_with_more_than_one_transition"] or s["e4"]["state_changed"] or s["e5"]["orphan_publications"]:
            errors.append("duplicate/late event or orphan publication check failed")
        if s["e2"]["step_runs_during_wait_total"]:
            errors.append("steps executed while waiting for approval")
    return lines, errors


def main(argv=None) -> int:
    base = (argv or sys.argv[1:] or ["results/raw"])[0]
    lines, errors = check(base)
    out = "\n".join(["## agentrt guarantee checks", "", *lines, "",
                     ("**FAILED:** " + "; ".join(errors)) if errors else "**All guarantee checks passed.**", ""])
    print(out)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(out)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
