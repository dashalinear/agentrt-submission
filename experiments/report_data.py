"""Load raw results and compute every number used in REPORT.md (no hand-typed numbers)."""
from __future__ import annotations

import csv
import json
import math
import os
from collections import Counter, defaultdict

from experiments.common import RAW, ROOT, pct

REAL = os.path.join(ROOT, "results", "real_llm")


def _conv(v: str):
    if v == "" or v is None:
        return None
    if v in ("True", "False"):
        return v == "True"
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in v and "e" not in v.lower() else f
    except ValueError:
        return v


def load_csv(name: str, base: str = RAW) -> list[dict]:
    p = os.path.join(base, name)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as fh:
        return [{k: _conv(v) for k, v in r.items()} for r in csv.DictReader(fh)]


def load_json(name: str, base: str = RAW):
    p = os.path.join(base, name)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def q(xs, p):
    v = pct([x for x in xs if x is not None], p)
    return v


def fmt(x, nd=2):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "–"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


# ----------------------------------------------------------------------------- Part 1
def recovery():
    rows = load_csv("recovery_runs.csv")
    by = defaultdict(list)
    for r in rows:
        by[r["case"]].append(r)
    base_p50 = q([r["total_time_s"] for r in by.get("baseline", [])], 50)
    out = []
    for case, rs in by.items():
        out.append({
            "case": case, "n": len(rs), "final": dict(Counter(r["final_state"] for r in rs)),
            "total_p50": q([r["total_time_s"] for r in rs], 50),
            "total_min": min(r["total_time_s"] for r in rs), "total_max": max(r["total_time_s"] for r in rs),
            "overhead_p50": q([r["total_time_s"] for r in rs], 50) - base_p50 if base_p50 == base_p50 else None,
            "resume_spawn_p50": q([r["resume_after_spawn_s"] for r in rs], 50),
            "resume_ready_p50": q([r["resume_after_ready_s"] for r in rs], 50),
            "resume_ready_max": max([r["resume_after_ready_s"] for r in rs if r["resume_after_ready_s"] is not None], default=None),
            "ready_after_spawn_p50": q([r["worker_ready_after_spawn_s"] for r in rs], 50),
            "llm_calls": sum(r["llm_calls"] for r in rs), "tool_calls": sum(r["tool_calls"] for r in rs),
            "rep_llm": sum(r["repeated_llm_calls"] for r in rs), "rep_tool": sum(r["repeated_tool_calls"] for r in rs),
            "unknown": sum(r["unknown_outcome_calls"] for r in rs),
            "reexec": sum(r["checkpointed_step_reexecuted"] for r in rs),
            "phys": sum(r["publish_physical"] for r in rs), "logical": sum(r["publish_logical"] for r in rs),
            "dups": sum(r["logical_duplicates"] for r in rs),
            "tokens_ok": sum(r["tokens_total_ok"] for r in rs),
            "extra_ok": sum(r["extra_tokens_ok"] for r in rs), "extra_upper": sum(r["extra_tokens_upper_bound"] for r in rs),
            "cost_total": sum(r["cost_total_usd"] for r in rs), "cost_extra": sum(r["cost_extra_usd"] for r in rs),
            "cost_extra_upper": sum(r.get("cost_extra_upper_usd") or 0 for r in rs),
            "transient_attempts": sum(r["transient_attempts"] for r in rs),
            "pauses": [json.loads(r["retry_pauses_s"]) for r in rs], "errors": [r["error"] for r in rs],
        })
    return {"rows": rows, "cases": out, "baseline_p50": base_p50, "meta": load_json("recovery_meta.json")}


# ----------------------------------------------------------------------------- Part 2
def events():
    s = load_json("events_summary.json") or {}
    return {"summary": s, "e1": load_csv("events_e1_approval.csv"), "e2": load_csv("events_e2_timeout.csv"),
            "e3": load_csv("events_e3_duplicates.csv"), "e4": load_csv("events_e4_late.csv"),
            "e5": load_csv("events_e5_races.csv"), "e6": load_csv("events_e6_restart_wait.csv"),
            "e7": load_csv("events_e7_cancel_stop.csv")}


# ----------------------------------------------------------------------------- Part 3
TENANTS = ("heavy", "light1", "light2")


def load():
    meta = load_json("load_meta.json") or {}
    jobs = load_csv("load_jobs.csv")
    runs = load_csv("load_step_runs.csv")
    ts = load_csv("load_timeseries.csv")
    rej = load_csv("load_rejections.csv")
    policies = list(dict.fromkeys(r["policy"] for r in jobs))
    seeds = sorted({r["seed"] for r in jobs})
    groups = []
    for p in policies:
        for t in TENANTS:
            for c in ("short", "long"):
                xs = [r["wait_total_v"] for r in jobs if r["policy"] == p and r["tenant"] == t and r["job_class"] == c]
                per_seed = [q([r["wait_total_v"] for r in jobs if r["policy"] == p and r["tenant"] == t and
                               r["job_class"] == c and r["seed"] == s], 95) for s in seeds]
                e2e = [r["e2e_v"] for r in jobs if r["policy"] == p and r["tenant"] == t and r["job_class"] == c
                       and r["state"] == "SUCCEEDED"]
                groups.append({"policy": p, "tenant": t, "job_class": c, "n": len(xs), "p50": q(xs, 50),
                               "p95": q(xs, 95), "max": max(xs) if xs else None,
                               "p95_seed_min": min(per_seed), "p95_seed_max": max(per_seed),
                               "n_succeeded": len(e2e), "e2e_p50": q(e2e, 50),
                               "expired": sum(1 for r in jobs if r["policy"] == p and r["tenant"] == t and
                                              r["job_class"] == c and r["state"] == "TIMED_OUT")})
    per_policy = []
    burst = next((ph for ph in meta.get("phases", []) if ph[0].startswith("P2")), ["P2", 150, 240])
    b0, b1 = burst[1], burst[2]
    for p in policies:
        pj = [r for r in jobs if r["policy"] == p]
        pr = [r for r in runs if r["policy"] == p and r["end_v"] is not None]
        mk = [m for m in meta.get("runs", []) if m["policy"] == p]
        makespan = sum(m["makespan_v"] for m in mk) / max(1, len(mk))
        succ = sum(1 for r in pj if r["state"] == "SUCCEEDED")
        # work actually executed per tenant inside the burst window (clipped step durations)
        work = Counter()
        busy = 0.0
        for r in pr:
            ov = max(0.0, min(r["end_v"], b1) - max(r["start_v"], b0))
            busy += ov
            if ov > 0 and r["outcome"] == "ok":
                work[r["tenant"]] += ov
        tot = sum(work.values()) or 1.0
        fin_burst = sum(1 for r in pj if r["state"] == "SUCCEEDED" and b0 <= r["created_v"] + r["e2e_v"] < b1)
        pts = [x for x in ts if x["policy"] == p]
        per_policy.append({
            "policy": p, "runs": len(mk), "arrivals": sum(m["arrivals"] for m in mk),
            "admitted": len(pj), "rejected": sum(m["rejected"] for m in mk),
            "rejected_by_tenant": dict(Counter(r["tenant"] for r in rej if r["policy"] == p)),
            "succeeded": succ, "timed_out": sum(1 for r in pj if r["state"] == "TIMED_OUT"),
            "timed_out_by_tenant": dict(Counter(r["tenant"] for r in pj if r["state"] == "TIMED_OUT")),
            "expired_in_queue": sum(1 for r in pj if r.get("expired_in") == "QUEUED"),
            "throughput": succ / len(mk) / makespan if makespan else None,
            "burst_throughput": fin_burst / len(mk) / (b1 - b0),
            "max_wait": max(r["wait_total_v"] for r in pj),
            "max_oldest_age": max(x["oldest_queued_age_v"] for x in pts) if pts else None,
            "max_queue": max(x["queued_total"] for x in pts) if pts else None,
            "work_share_burst": {t: work[t] / tot for t in TENANTS},
            "slot_utilization_burst": busy / (len(mk) * (b1 - b0) * (mk[0]["global_limit"] if mk else 1)),
            "max_global_obs": max(m["max_global_concurrency_observed"] for m in mk),
            "global_limit": mk[0]["global_limit"] if mk else None,
            "max_tenant_obs": max(max(m["max_per_tenant_observed"].values()) for m in mk),
            "tenant_limit": mk[0]["per_tenant_limit"] if mk else None,
            "steps_ok": sum(1 for r in pr if r["outcome"] == "ok"),
        })
    # demand share in the burst window (nominal work of arrivals)
    nominal = meta.get("nominal_work_v", {"short": 1.0, "long": 9.5})
    demand = Counter()
    for s in seeds:
        for a in load_csv(f"load_arrivals_seed{s}.csv"):
            if b0 <= a["t_v"] < b1:
                demand[a["tenant"]] += nominal[a["job_class"]]
    dtot = sum(demand.values()) or 1.0
    # step-duration overhead (measured vs nominal) -> scheduling/DB overhead at this time scale
    from agentrt.config import DEFAULT_SIM

    over = []
    for r in runs:
        if r["outcome"] == "ok" and r["end_v"] is not None and r["step"] in DEFAULT_SIM[r["job_class"]]:
            over.append(r["end_v"] - r["start_v"] - DEFAULT_SIM[r["job_class"]][r["step"]])
    return {"meta": meta, "jobs": jobs, "runs": runs, "ts": ts, "rej": rej, "policies": policies, "seeds": seeds,
            "groups": groups, "per_policy": per_policy, "burst": (b0, b1),
            "demand_share": {t: demand[t] / dtot for t in TENANTS},
            "step_overhead_p50": q(over, 50), "step_overhead_p95": q(over, 95)}


def real_llm(path=REAL):
    s = load_json("summary.json", path)
    rec = load_csv("recovery_runs_real.csv", path)
    return {"summary": s, "recovery": rec}


def pytest_summary():
    p = os.path.join(ROOT, "results", "pytest_output.txt")
    if not os.path.exists(p):
        return None
    lines = open(p, encoding="utf-8", errors="replace").read().splitlines()
    tests = [ln for ln in lines if "::" in ln and (" PASSED" in ln or " FAILED" in ln or " ERROR" in ln)]
    last = next((ln for ln in reversed(lines) if " passed" in ln or " failed" in ln), "")
    return {"tests": tests, "last": last.strip("= ").strip()}
