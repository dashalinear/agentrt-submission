"""Part 3: backpressure and fair scheduling under load.

Same arrival trace (per seed) for every policy, same resources (global_concurrency).
Time is SCALED: 1 virtual second = TIME_SCALE real seconds (default 0.1 -> 100 ms).
All reported times are virtual seconds.

Usage: python -m experiments.exp_load [--seeds 11,12,13] [--policies fifo,drr,drr_cap] [--scale 0.1]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import shutil
import time

from experiments.common import RAW, WORK, environment, fresh_dir, high_res_timer, write_csv

from agentrt.config import DEFAULT_SIM, Config
from agentrt.runtime import build_worker
from agentrt.store import Rejected, Store

TENANTS = ("heavy", "light1", "light2")
# phases: (name, start_v, end_v, rates per tenant in jobs / virtual second)
PHASES = [("P1_below", 0, 150, {"heavy": 0.5, "light1": 0.2, "light2": 0.2}),
          ("P2_burst", 150, 240, {"heavy": 3.0, "light1": 0.2, "light2": 0.2}),
          ("P3_recovery", 240, 480, {"heavy": 0.3, "light1": 0.2, "light2": 0.2})]
LONG_SHARE = 0.3
PRIO_SHARE = 0.1
DEADLINE_V = {"short": 60.0, "long": 200.0}
GLOBAL_CONC = 6
MAX_QUEUE = 150

POLICIES = {
    "fifo": dict(policy="fifo", per_tenant_concurrency=GLOBAL_CONC),
    "drr": dict(policy="drr", per_tenant_concurrency=GLOBAL_CONC),
    # DRR + hard isolation: per-tenant concurrency cap and per-tenant admission quota
    "drr_iso": dict(policy="drr", per_tenant_concurrency=3, max_queue_per_tenant=100),
}


def nominal_work(cls: str) -> float:
    s = DEFAULT_SIM[cls]
    return s["fetch_documents"] + s["extract"] + s["draft"] * s["draft_chunks"] + s["decide"] + s["save_report"] + s["publish"]


def make_trace(seed: int) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for ph, t0, t1, rates in PHASES:
        for tenant in TENANTS:
            t = t0
            while True:
                t += rng.expovariate(rates[tenant])
                if t >= t1:
                    break
                cls = "long" if rng.random() < LONG_SHARE else "short"
                out.append({"t_v": round(t, 4), "tenant": tenant, "job_class": cls, "phase": ph,
                            "priority": 1 if rng.random() < PRIO_SHARE else 0})
    out.sort(key=lambda a: a["t_v"])
    for i, a in enumerate(out):
        a["arrival_id"] = i
    return out


def make_cfg(policy: str, seed: int, scale: float) -> Config:
    return Config(time_scale=scale, global_concurrency=GLOBAL_CONC, max_queue_size=MAX_QUEUE,
                  lease_timeout_s=10.0, heartbeat_interval_s=1.0, poll_interval_s=0.002, reaper_interval_s=5.0,
                  aging_interval_s=10.0, drr_quantum_s=1.0, sim={"llm_jitter": 0.1}, seed=seed,
                  **POLICIES[policy])


async def run_once(policy: str, seed: int, trace: list[dict], scale: float, sample_v: float = 0.5) -> dict:
    d = fresh_dir("load", f"{policy}_{seed}")
    db = os.path.join(d, "s.db")
    cfg = make_cfg(policy, seed, scale)
    store = Store(db, cfg)
    worker = build_worker(db, cfg, "w1", publisher_db=os.path.join(d, "pub.db"))
    wtask = asyncio.create_task(worker.run())
    while worker.ready_at is None:
        await asyncio.sleep(0.001)
    t0 = time.time()
    samples, created, rejected = [], {}, []
    done = asyncio.Event()

    async def arrivals():
        for a in trace:
            delay = t0 + a["t_v"] * scale - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                job, _ = store.create_job(a["tenant"], f"arr-{a['arrival_id']}", a["job_class"], a["priority"],
                                          params={"require_approval": False, "collection_id": f"c{a['arrival_id']}"},
                                          limits={"deadline_s": DEADLINE_V[a["job_class"]], "step_timeout_s": 60})
                created[job["job_id"]] = a
                worker.wake()
            except Rejected as e:
                rejected.append({**a, "reason": e.reason, "t_actual_v": (time.time() - t0) / scale})

    async def sampler():
        while not done.is_set():
            now = time.time()
            row = {"t_v": round((now - t0) / scale, 3)}
            q = dict(store.conn.execute("SELECT tenant_id, COUNT(*) FROM jobs WHERE state='QUEUED' AND run_after<=? "
                                        "GROUP BY tenant_id", (now,)).fetchall())
            r = dict(store.conn.execute("SELECT tenant_id, COUNT(*) FROM step_runs WHERE ended_at IS NULL "
                                        "GROUP BY tenant_id").fetchall())
            oldest = store.conn.execute("SELECT MIN(ready_at) FROM jobs WHERE state='QUEUED'").fetchone()[0]
            for t in TENANTS:
                row[f"queued_{t}"] = q.get(t, 0)
                row[f"active_{t}"] = r.get(t, 0)
            row["queued_total"] = sum(q.values())
            row["active_total"] = sum(r.values())
            row["oldest_queued_age_v"] = round((now - oldest) / scale, 3) if oldest else 0.0
            samples.append(row)
            await asyncio.sleep(sample_v * scale)

    st = asyncio.create_task(sampler())
    await arrivals()
    while store.conn.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('QUEUED','RUNNING','WAITING_EVENT')").fetchone()[0]:
        await asyncio.sleep(0.05)
    t_end = time.time()
    done.set()
    await st
    worker.stop()
    await wtask

    jobs = []
    last_src = dict(store.conn.execute("SELECT job_id, src FROM transitions WHERE dst='TIMED_OUT'").fetchall())
    for r in store.conn.execute("SELECT * FROM jobs"):
        a = created[r["job_id"]]
        # a job that expired while queued: its last queued interval is never closed by a claim
        tail = (r["finished_at"] - max(r["ready_at"], r["run_after"])) if last_src.get(r["job_id"]) == "QUEUED" else 0.0
        jobs.append({"policy": policy, "seed": seed, "job_id": r["job_id"], "tenant": r["tenant_id"],
                     "job_class": r["job_class"], "priority": r["priority"], "phase": a["phase"],
                     "arrival_v": a["t_v"], "created_v": round((r["created_at"] - t0) / scale, 4), "state": r["state"],
                     "wait_total_v": round((r["queue_wait_total"] + max(0.0, tail)) / scale, 4),
                     "expired_in": last_src.get(r["job_id"]),
                     "wait_first_v": round((r["first_started_at"] - r["created_at"]) / scale, 4)
                     if r["first_started_at"] else None,
                     "e2e_v": round((r["finished_at"] - r["created_at"]) / scale, 4),
                     "steps": r["step_seq"], "error": r["error"]})
    runs = []
    for r in store.conn.execute("SELECT job_id, tenant_id, job_class, step_key, started_at, ended_at, outcome FROM step_runs"):
        runs.append({"policy": policy, "seed": seed, "job_id": r[0], "tenant": r[1], "job_class": r[2],
                     "step": r[3].split("#")[0], "start_v": round((r[4] - t0) / scale, 4),
                     "end_v": round((r[5] - t0) / scale, 4) if r[5] else None, "outcome": r[6]})
    for s in samples:
        s.update(policy=policy, seed=seed)
    for rj in rejected:
        rj.update(policy=policy, seed=seed)
    return {"jobs": jobs, "runs": runs, "samples": samples, "rejected": rejected,
            "makespan_v": round((t_end - t0) / scale, 3), "counters": store.counts()["counters"]}


def max_overlap(intervals):
    ev = []
    for s, e in intervals:
        ev.append((s, 1))
        ev.append((e if e is not None else float("inf"), -1))
    ev.sort(key=lambda x: (x[0], x[1]))
    cur = best = 0
    for _, d in ev:
        cur += d
        best = max(best, cur)
    return best


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default="11,12,13")
    ap.add_argument("--policies", default=",".join(POLICIES))
    ap.add_argument("--scale", type=float, default=0.1)
    ap.add_argument("--out", default=RAW)
    a = ap.parse_args(argv)
    high_res_timer()
    from agentrt.logs import setup_logging

    os.makedirs(WORK, exist_ok=True)
    os.environ.setdefault("AGENTRT_LOG_LEVEL", "WARNING")
    setup_logging(level="WARNING", path=os.path.join(WORK, "load_experiment.log"))
    seeds = [int(s) for s in a.seeds.split(",")]
    all_jobs, all_runs, all_samples, all_rej, meta_runs = [], [], [], [], []
    for seed in seeds:
        trace = make_trace(seed)
        write_csv(os.path.join(a.out, f"load_arrivals_seed{seed}.csv"), trace)
        for policy in a.policies.split(","):
            t_real = time.time()
            res = asyncio.run(run_once(policy, seed, trace, a.scale))
            all_jobs += res["jobs"]
            all_runs += res["runs"]
            all_samples += res["samples"]
            all_rej += res["rejected"]
            cfg = make_cfg(policy, seed, a.scale)
            iv_all = [(r["start_v"], r["end_v"]) for r in res["runs"]]
            per_t = {t: max_overlap([(r["start_v"], r["end_v"]) for r in res["runs"] if r["tenant"] == t]) for t in TENANTS}
            m = {"policy": policy, "seed": seed, "arrivals": len(trace), "admitted": len(res["jobs"]),
                 "rejected": len(res["rejected"]), "makespan_v": res["makespan_v"],
                 "real_seconds": round(time.time() - t_real, 2),
                 "max_global_concurrency_observed": max_overlap(iv_all), "global_limit": cfg.global_concurrency,
                 "max_per_tenant_observed": per_t, "per_tenant_limit": cfg.per_tenant_concurrency,
                 "counters": res["counters"]}
            meta_runs.append(m)
            print(json.dumps(m), flush=True)
    write_csv(os.path.join(a.out, "load_jobs.csv"), all_jobs)
    write_csv(os.path.join(a.out, "load_step_runs.csv"), all_runs)
    write_csv(os.path.join(a.out, "load_timeseries.csv"), all_samples)
    write_csv(os.path.join(a.out, "load_rejections.csv"), all_rej)
    with open(os.path.join(a.out, "load_meta.json"), "w") as fh:
        json.dump({"env": environment(), "time_scale": a.scale,
                   "time_scale_note": f"1 virtual second = {a.scale} real seconds", "seeds": seeds,
                   "phases": PHASES, "long_share": LONG_SHARE, "priority_share": PRIO_SHARE,
                   "deadlines_v": DEADLINE_V, "policies": POLICIES, "global_concurrency": GLOBAL_CONC,
                   "nominal_work_v": {c: nominal_work(c) for c in ("short", "long")},
                   "config_example": make_cfg("drr", seeds[0], a.scale).to_dict(), "runs": meta_runs}, fh, indent=2)


if __name__ == "__main__":
    main()
