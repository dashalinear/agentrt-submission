"""Part 1 experiments: recovery & idempotency with REAL worker process kills.

Worker = separate OS process (``python -m agentrt.cli worker``), killed with
Popen.kill() (SIGKILL on POSIX, TerminateProcess on Windows).
FakePublisherService = separate HTTP process with its own SQLite file.

Usage: python -m experiments.exp_recovery [--reps 5] [--llm sim|real] [--out results/raw]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time

from experiments.common import (RAW, WORK, environment, fresh_dir, hard_kill, ro, spawn_worker, start_publisher, wait_for,
                                write_csv, write_jsonl)

from agentrt.config import Config
from agentrt.store import Store

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")

BASE = dict(time_scale=1.0, lease_timeout_s=2.0, heartbeat_interval_s=0.25, reaper_interval_s=0.2,
            poll_interval_s=0.01, backoff_base_s=0.5, backoff_cap_s=4.0, global_concurrency=2,
            per_tenant_concurrency=2,
            sim={"short": {"fetch_documents": 0.3, "extract": 0.2, "draft": 1.0, "draft_chunks": 2, "decide": 0.05,
                           "save_report": 0.05, "publish": 0.1, "docs": 3}})

CASES = {
    "baseline": {"desc": "no failure", "faults": {}},
    "s1_kill_between_steps": {"desc": "kill after extract#0 committed, before draft starts; restart same worker name",
                              "faults": {"pause_after_step": {"extract": 1.0}}, "restart_name": "w1"},
    "s2a_kill_during_llm_lease": {"desc": "kill during LLM call (draft#0.0); recovery by another worker after lease "
                                          "expiry", "faults": {}, "restart_name": "w2"},
    "s2b_kill_during_llm_restart": {"desc": "kill during LLM call; same worker name restarts (immediate reclaim)",
                                    "faults": {}, "restart_name": "w1"},
    "s3a_crash_after_publish": {"desc": "worker crashes right after publisher committed (ack never checkpointed)",
                                "faults": {"crash_after_publish_response": True}, "restart_name": "w1"},
    "s3b_ack_lost_network": {"desc": "publisher commits but answers 503 (ack lost); retry with same key",
                             "faults": {}, "publisher_lose_ack": True},
    "s4_transient_repeated": {"desc": "LLM always returns transient 503 on draft; max_attempts=4",
                              "faults": {"llm_fail": {"draft": -1}}},
}


def job_terminal(db, job_id):
    with ro(db) as c:
        r = c.execute("SELECT state FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return r and r[0] in TERMINAL and r[0]


def run_case(case: str, rep: int, pub_url: str, pub_db: str, llm: str, seed: int) -> tuple[dict, list, list]:
    spec = CASES[case]
    d = fresh_dir("recovery", f"{case}_{rep}")
    db, ledger, cfg_path = os.path.join(d, "state.db"), os.path.join(d, "ledger.db"), os.path.join(d, "cfg.json")
    faults = json.loads(json.dumps(spec["faults"]))
    if faults.get("crash_after_publish_response"):
        faults["crash_marker"] = os.path.join(d, "crashed.marker")
    cfg = Config(**BASE, faults=faults, seed=seed)
    cfg.dump(cfg_path)
    store = Store(db, cfg)
    if spec.get("publisher_lose_ack"):
        import httpx

        lost = httpx.get(pub_url + "/v1/stats").json()["acks_lost"]
        httpx.post(pub_url + "/admin/faults", json={"lose_ack_first_n": lost + 1})
    procs = []
    w = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_url=pub_url, log_file=os.path.join(d, "worker.log"),
                     llm=llm)
    procs.append(w)
    wait_for(lambda: ro(db).execute("SELECT ready_at FROM workers WHERE ready_at IS NOT NULL").fetchone())
    job, _ = store.create_job("tenant-a", f"rec-{case}-{rep}", "short",
                              params={"require_approval": False, "collection_id": f"col-{rep}"},
                              limits={"max_attempts": 4, "deadline_s": 120})
    jid = job["job_id"]
    t_kill = t_spawn = None
    restart = spec.get("restart_name")
    if case == "s1_kill_between_steps":
        wait_for(lambda: ro(db).execute("SELECT 1 FROM step_results WHERE job_id=? AND step_key='extract#0'",
                                        (jid,)).fetchone())
        time.sleep(0.2)  # inside the injected 1.0 s pause between steps
        t_kill = hard_kill(w)
    elif case.startswith("s2"):
        wait_for(lambda: os.path.exists(ledger) and ro(ledger).execute(
            "SELECT 1 FROM calls WHERE provider='llm' AND ended_at IS NULL").fetchone())
        time.sleep(0.3)  # mid-call
        t_kill = hard_kill(w)
    elif case == "s3a_crash_after_publish":
        wait_for(lambda: w.poll() is not None, timeout=300)
        t_kill = time.time()
    if restart:
        t_spawn = time.time()
        w2 = spawn_worker(db, cfg_path, restart, ledger=ledger, publisher_url=pub_url,
                          log_file=os.path.join(d, "worker2.log"), llm=llm)
        procs.append(w2)
    final = wait_for(lambda: job_terminal(db, jid), timeout=180)
    time.sleep(0.3)  # let outbox relay deliver terminal notification
    for p in procs:
        if p.poll() is None:
            hard_kill(p)

    c = ro(db)
    j = dict(c.execute("SELECT * FROM jobs WHERE job_id=?", (jid,)).fetchone())
    runs = [dict(r) for r in c.execute("SELECT * FROM step_runs WHERE job_id=? ORDER BY id", (jid,))]
    results = {r["step_key"]: dict(r) for r in c.execute("SELECT * FROM step_results WHERE job_id=?", (jid,))}
    workers = [dict(r) for r in c.execute("SELECT * FROM workers ORDER BY started_at")]
    calls = [dict(r) for r in ro(ledger).execute("SELECT * FROM calls ORDER BY id")] if os.path.exists(ledger) else []
    pc = ro(pub_db)
    key = f"{jid}:publish"
    phys = pc.execute("SELECT COUNT(*) FROM requests WHERE kind='publish' AND idempotency_key=?", (key,)).fetchone()[0]
    logical = pc.execute("SELECT COUNT(*) FROM publications WHERE idempotency_key=?", (key,)).fetchone()[0]

    # repeated external calls and "re-execution of a checkpointed step" violations
    by_step: dict[str, list] = {}
    for cl in calls:
        by_step.setdefault(cl["step_key"], []).append(cl)
    repeated_llm = sum(len(v) - 1 for k, v in by_step.items() if v[0]["provider"] == "llm")
    repeated_tool = sum(len(v) - 1 for k, v in by_step.items() if v[0]["provider"] == "tool")
    violations = sum(1 for cl in calls if cl["step_key"] in results and cl["started_at"] > results[cl["step_key"]]["created_at"])
    ok_calls = [cl for cl in calls if cl["outcome"] == "ok" and cl["provider"] == "llm"]
    tokens_total = sum(cl["prompt_tokens"] + cl["completion_tokens"] for cl in ok_calls)
    # extra = every LLM call whose result did NOT become the checkpointed result
    used = {(k, results[k]["attempt"]) for k in results}
    extra_ok = [cl for cl in ok_calls if (cl["step_key"], cl["attempt"]) not in used]
    unknown = [cl for cl in calls if cl["ended_at"] is None]
    extra_tokens = sum(cl["prompt_tokens"] + cl["completion_tokens"] for cl in extra_ok)
    # an LLM call cut off by the crash may still have been processed and billed by the provider:
    # upper bound = its prompt tokens + max_output_tokens (200, agent default)
    extra_tokens_upper = extra_tokens + sum(cl["prompt_tokens"] + 200 for cl in unknown if cl["provider"] == "llm")
    price = lambda p, cpl: p / 1000 * cfg.price_per_1k_prompt + cpl / 1000 * cfg.price_per_1k_completion  # noqa: E731
    cost_total = sum(price(cl["prompt_tokens"], cl["completion_tokens"]) for cl in ok_calls)
    cost_extra = sum(price(cl["prompt_tokens"], cl["completion_tokens"]) for cl in extra_ok)
    cost_extra_upper = cost_extra + sum(price(cl["prompt_tokens"], 200) for cl in unknown if cl["provider"] == "llm")

    resume_ts = None
    w_new = None
    if t_kill:
        later = [r for r in runs if r["started_at"] > t_kill]
        resume_ts = later[0]["started_at"] if later else None
        w_new = next((w_ for w_ in workers if t_spawn and w_["started_at"] >= t_spawn), None)
    transient_runs = [r for r in runs if r["outcome"] == "transient_error"]
    pauses = [round(b["started_at"] - a["ended_at"], 3) for a, b in zip(runs, runs[1:])
              if a["outcome"] == "transient_error"]
    row = {
        "case": case, "rep": rep, "seed": seed, "llm": llm, "job_id": jid, "final_state": final,
        "error": j["error"], "total_time_s": round(j["finished_at"] - j["created_at"], 3),
        "kill_ts": t_kill, "restart_spawn_ts": t_spawn,
        "worker_ready_after_spawn_s": round(w_new["ready_at"] - t_spawn, 3) if w_new else None,
        "resume_after_spawn_s": round(resume_ts - t_spawn, 3) if resume_ts and t_spawn else None,
        "resume_after_ready_s": round(resume_ts - w_new["ready_at"], 3) if resume_ts and w_new else None,
        "resume_after_kill_s": round(resume_ts - t_kill, 3) if resume_ts and t_kill else None,
        "step_runs": len(runs), "llm_calls": sum(1 for cl in calls if cl["provider"] == "llm"),
        "tool_calls": sum(1 for cl in calls if cl["provider"] == "tool"),
        "repeated_llm_calls": repeated_llm, "repeated_tool_calls": repeated_tool,
        "unknown_outcome_calls": len(unknown), "checkpointed_step_reexecuted": violations,
        "tokens_total_ok": tokens_total, "extra_tokens_ok": extra_tokens, "extra_tokens_upper_bound": extra_tokens_upper,
        "cost_total_usd": round(cost_total, 8), "cost_extra_usd": round(cost_extra, 8), "cost_extra_upper_usd": round(cost_extra_upper, 8),
        "publish_physical": phys, "publish_logical": logical, "logical_duplicates": max(0, logical - 1),
        "transient_attempts": len(transient_runs), "retry_pauses_s": json.dumps(pauses),
    }
    for r in runs:
        r["case"], r["rep"] = case, rep
    for cl in calls:
        cl["case"], cl["rep"] = case, rep
    return row, runs, calls


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--llm", default="sim", choices=["sim", "real"])
    ap.add_argument("--out", default=RAW)
    ap.add_argument("--cases", default=",".join(CASES))
    a = ap.parse_args(argv)
    os.makedirs(WORK, exist_ok=True)
    pub_dir = fresh_dir("recovery_publisher")
    pub_db = os.path.join(pub_dir, "publisher.db")
    pub, url = start_publisher(pub_db)
    rows, all_runs, all_calls = [], [], []
    try:
        for case in a.cases.split(","):
            for rep in range(a.reps):
                seed = 1000 + rep
                row, runs, calls = run_case(case, rep, url, pub_db, a.llm, seed)
                print(json.dumps({k: row[k] for k in ("case", "rep", "final_state", "total_time_s",
                                                      "resume_after_spawn_s", "repeated_llm_calls",
                                                      "publish_physical", "publish_logical")}))
                rows.append(row)
                all_runs += runs
                all_calls += calls
    finally:
        hard_kill(pub)
    suffix = "" if a.llm == "sim" else "_real"
    write_csv(os.path.join(a.out, f"recovery_runs{suffix}.csv"), rows)
    write_jsonl(os.path.join(a.out, f"recovery_step_runs{suffix}.jsonl"), all_runs)
    write_jsonl(os.path.join(a.out, f"recovery_calls{suffix}.jsonl"), all_calls)
    with open(os.path.join(a.out, f"recovery_meta{suffix}.json"), "w") as fh:
        json.dump({"env": environment(), "config": BASE, "cases": CASES, "reps": a.reps,
                   "seeds": [1000 + r for r in range(a.reps)], "time_scale": 1.0}, fh, indent=2)


if __name__ == "__main__":
    main()
