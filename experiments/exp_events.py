"""Part 2 experiments: event-driven lifecycle (approval, deadlines, duplicates, late
events, races, restart while waiting, cancel stop latency).  time_scale = 1.0 (real s).

Usage: python -m experiments.exp_events [--race-reps 300] [--seed 7]
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import threading
import time
import uuid
from collections import Counter

from experiments.common import (RAW, WORK, environment, fresh_dir, hard_kill, pct, ro, spawn_worker, wait_for, write_csv,
                                write_jsonl)

from agentrt.config import Config
from agentrt.harness import ThreadWorker, check_invariants
from agentrt.publisher import FakePublisherService
from agentrt.store import Store

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
FAST_SIM = {"short": {"fetch_documents": 0.02, "extract": 0.01, "draft": 0.03, "draft_chunks": 1, "decide": 0.01,
                      "save_report": 0.01, "publish": 0.02, "docs": 3}}


def cfg_fast(**kw) -> Config:
    base = dict(time_scale=1.0, poll_interval_s=0.003, heartbeat_interval_s=0.05, lease_timeout_s=2.0,
                reaper_interval_s=0.2, global_concurrency=64, per_tenant_concurrency=64, sim=FAST_SIM)
    base.update(kw)
    return Config(**base)


def fresh(name: str) -> str:
    return fresh_dir("events", name)


def state(store, jid):
    return store.get_job(jid)["state"]


def wait_state(store, jid, states, timeout=30):
    return wait_for(lambda: state(store, jid) in states and state(store, jid), timeout=timeout, interval=0.001)


def approve(store, jid, event_id=None, decision="approve"):
    j = store.get_job(jid)
    t_send = time.time()
    r = store.submit_event(jid, event_id or "evt-" + uuid.uuid4().hex[:12], "approval",
                           {"approval_id": j["approval_id"], "decision": decision})
    r["t_send"] = t_send
    r["t_return"] = time.time()
    return r


def transition_ts(store, jid, src, dst):
    r = store.conn.execute("SELECT ts FROM transitions WHERE job_id=? AND src=? AND dst=? ORDER BY id LIMIT 1",
                           (jid, src, dst)).fetchone()
    return r[0] if r else None


# ------------------------------------------------------------------------- E1
def e1_approval_on_time(n, rng):
    d = fresh("e1")
    db = os.path.join(d, "s.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    rows = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        jobs = [store.create_job("tenant-a", f"e1-{i}")[0]["job_id"] for i in range(n)]
        for jid in jobs:
            wait_state(store, jid, ("WAITING_EVENT",))
            time.sleep(rng.uniform(0, 0.02))
            r = approve(store, jid)
            rows.append((jid, r))
        for jid in jobs:
            wait_state(store, jid, TERMINAL)
    out = []
    for jid, r in rows:
        t_q = transition_ts(store, jid, "WAITING_EVENT", "QUEUED")
        nxt = store.conn.execute("SELECT started_at FROM step_runs WHERE job_id=? AND step_key='save_report#0' "
                                 "ORDER BY id LIMIT 1", (jid,)).fetchone()
        out.append({"exp": "e1", "job_id": jid, "outcome": r["outcome"], "final": state(store, jid),
                    "event_to_transition_ms": (t_q - r["received_at"]) * 1000 if t_q else None,
                    "event_to_commit_ms": (r["committed_at"] - r["received_at"]) * 1000,
                    "client_roundtrip_ms": (r["t_return"] - r["t_send"]) * 1000,
                    "event_to_next_step_start_ms": (nxt[0] - r["received_at"]) * 1000 if nxt else None})
    return out, check_invariants(store.conn)


# ------------------------------------------------------------------------- E2
def e2_no_approval_timeout(n):
    d = fresh("e2")
    db = os.path.join(d, "s.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    out = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        jobs = [store.create_job("tenant-a", f"e2-{i}", limits={"deadline_s": 0.5})[0]["job_id"] for i in range(n)]
        for jid in jobs:
            wait_state(store, jid, TERMINAL)
    for jid in jobs:
        j = store.get_job(jid)
        t_to = transition_ts(store, jid, "WAITING_EVENT", "TIMED_OUT")
        open_runs_while_waiting = store.conn.execute(
            "SELECT COUNT(*) FROM step_runs WHERE job_id=? AND started_at >= ? ", (jid, transition_ts(
                store, jid, "RUNNING", "WAITING_EVENT") or 0)).fetchone()[0]
        out.append({"exp": "e2", "job_id": jid, "final": j["state"], "waited_in": "WAITING_EVENT" if t_to else "other",
                    "timeout_lateness_ms": (j["finished_at"] - j["deadline"]) * 1000,
                    "step_runs_during_wait": open_runs_while_waiting})
    return out, check_invariants(store.conn)


# ------------------------------------------------------------------------- E3
def e3_duplicates(n):
    d = fresh("e3")
    db = os.path.join(d, "s.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    out = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        jobs = [store.create_job("tenant-a", f"e3-{i}")[0]["job_id"] for i in range(n)]
        for jid in jobs:
            wait_state(store, jid, ("WAITING_EVENT",))
            j = store.get_job(jid)
            eid = "evt-dup-" + jid
            res = []
            barrier = threading.Barrier(3)

            def send():
                s2 = Store(db, cfg, init=False)
                barrier.wait()
                res.append(s2.submit_event(jid, eid, "approval", {"approval_id": j["approval_id"]})["outcome"])

            ths = [threading.Thread(target=send) for _ in range(3)]  # 3 concurrent copies
            [t.start() for t in ths]
            [t.join() for t in ths]
            for _ in range(2):  # 2 later redeliveries
                res.append(store.submit_event(jid, eid, "approval", {"approval_id": j["approval_id"]})["outcome"])
            out.append({"exp": "e3", "job_id": jid, "copies": 5, "applied": res.count("applied"),
                        "duplicates": res.count("duplicate"), "other": 5 - res.count("applied") - res.count("duplicate"),
                        "waiting_to_queued_transitions": store.conn.execute(
                            "SELECT COUNT(*) FROM transitions WHERE job_id=? AND src='WAITING_EVENT' AND dst='QUEUED'",
                            (jid,)).fetchone()[0]})
        for jid in jobs:
            wait_state(store, jid, TERMINAL)
    for r in out:
        r["final"] = state(store, r["job_id"])
    return out, check_invariants(store.conn)


# ------------------------------------------------------------------------- E4
def e4_late_events(n):
    d = fresh("e4")
    db = os.path.join(d, "s.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    out = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        for i in range(n):
            kind = "after_cancel" if i % 2 == 0 else "after_timeout"
            jid = store.create_job("tenant-a", f"e4-{i}", limits={"deadline_s": 0.3})[0]["job_id"]
            wait_state(store, jid, ("WAITING_EVENT",))
            apr = store.get_job(jid)["approval_id"]
            if kind == "after_cancel":
                store.cancel(jid)
            wait_state(store, jid, TERMINAL)
            before = store.get_job(jid)
            r = store.submit_event(jid, "evt-late-" + jid, "approval", {"approval_id": apr})
            c = store.cancel(jid)
            after = store.get_job(jid)
            out.append({"exp": "e4", "job_id": jid, "kind": kind, "state_before": before["state"],
                        "event_outcome": r["outcome"], "cancel_outcome": c["outcome"], "state_after": after["state"],
                        "version_changed": before["version"] != after["version"]})
    return out, check_invariants(store.conn)


# ------------------------------------------------------------------------- E5
def e5_races(reps, rng, batch=10):
    d = fresh("e5")
    db = os.path.join(d, "s.db")
    pub_db = os.path.join(d, "pub.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    out = []
    D = 0.6
    with ThreadWorker(db, cfg, publisher_db=pub_db) as tw:
        for b0 in range(0, reps, batch):
            plan = []
            for i in range(b0, min(reps, b0 + batch)):
                jid = store.create_job("tenant-a", f"e5-{i}", limits={"deadline_s": D})[0]["job_id"]
                plan.append({"i": i, "job_id": jid, "approve_off": rng.uniform(-0.15, 0.02),
                             "cancel_off": rng.uniform(-0.05, 0.05)})
            results = {}

            def actor(p, kind):
                s2 = Store(db, cfg, init=False)
                jid = p["job_id"]
                dl = s2.get_job(jid)["deadline"]
                try:
                    wait_for(lambda: s2.get_job(jid)["state"] in ("WAITING_EVENT",) + TERMINAL, timeout=10, interval=0.003)
                except TimeoutError:
                    pass
                t_target = dl + p[f"{kind}_off"]
                if t_target - time.time() > 0.003:
                    time.sleep(t_target - time.time() - 0.002)
                while time.time() < t_target:
                    pass
                t_send = time.time()
                if kind == "approve":
                    apr = s2.get_job(jid)["approval_id"]
                    r = s2.submit_event(jid, "evt-race-a-" + jid, "approval", {"approval_id": apr})
                else:
                    r = s2.cancel(jid)
                    r["committed_at"] = time.time()
                results[(jid, kind)] = (t_send, r)

            ths = [threading.Thread(target=actor, args=(p, k)) for p in plan for k in ("approve", "cancel")]
            [t.start() for t in ths]
            [t.join() for t in ths]
            for p in plan:
                wait_state(store, p["job_id"], TERMINAL)
            for p in plan:
                jid = p["job_id"]
                j = store.get_job(jid)
                ta, ra = results[(jid, "approve")]
                tc, rc = results[(jid, "cancel")]
                term = store.conn.execute(f"SELECT ts, src, reason FROM transitions WHERE job_id=? AND dst IN "
                                          f"{TERMINAL} ORDER BY id", (jid,)).fetchall()
                cancel_run = store.conn.execute("SELECT step_key, outcome, ended_at FROM step_runs WHERE job_id=? "
                                                "AND outcome LIKE 'aborted%' OR (job_id=? AND outcome='discarded_late_result')",
                                                (jid, jid)).fetchall()
                t_w = transition_ts(store, jid, "RUNNING", "WAITING_EVENT")
                out.append({"exp": "e5", "rep": p["i"], "job_id": jid, "final": j["state"],
                            "reached_waiting_before_deadline_ms": round((j["deadline"] - t_w) * 1000, 2) if t_w else None,
                            "terminal_from": term[0][1] if term else None, "n_terminal_transitions": len(term),
                            "approve_offset_ms": round(p["approve_off"] * 1000, 2),
                            "cancel_offset_ms": round(p["cancel_off"] * 1000, 2),
                            "approve_sent_ms": round((ta - j["deadline"]) * 1000, 2),
                            "cancel_sent_ms": round((tc - j["deadline"]) * 1000, 2),
                            "approval_outcome": ra["outcome"], "cancel_outcome": rc["outcome"],
                            "cancel_was": rc.get("was"),
                            "terminal_ts_minus_deadline_ms": round((term[0][0] - j["deadline"]) * 1000, 2) if term else None,
                            "aborted_or_discarded_steps": json.dumps([tuple(x[:2]) for x in cancel_run]),
                            "cancel_stop_ms": round((cancel_run[0][2] - j["finished_at"]) * 1000, 2)
                            if (cancel_run and j["state"] == "CANCELLED" and rc.get("was") == "RUNNING") else None})
        time.sleep(0.3)
        _ = tw
    pubs = FakePublisherService(pub_db).publications_by_key()
    for r in out:
        p = pubs.get(f"{r['job_id']}:publish")
        r["published"] = p is not None
        r["publication_retracted"] = bool(p and p["retracted_at"])
        r["orphan_publication"] = bool(p and not p["retracted_at"] and r["final"] != "SUCCEEDED")
        r["succeeded_without_publication"] = r["final"] == "SUCCEEDED" and (p is None or bool(p["retracted_at"]))
    inv = check_invariants(store.conn)
    inv["invalid_transition_attempts_counter"] = store.counts()["counters"].get("invalid_transition_attempts", 0)
    inv["counters"] = store.counts()["counters"]
    return out, inv


# ------------------------------------------------------------------------- E6
def e6_restart_while_waiting(reps):
    out = []
    for rep in range(reps):
        for variant in ("approval_while_down", "deadline_while_down", "approval_after_restart"):
            d = fresh(f"e6_{variant}_{rep}")
            db = os.path.join(d, "s.db")
            cfg = cfg_fast(lease_timeout_s=2.0)
            cfg_path = os.path.join(d, "cfg.json")
            cfg.dump(cfg_path)
            store = Store(db, cfg)
            pub_db = os.path.join(d, "pub.db")
            w = spawn_worker(db, cfg_path, "w1", log_file=os.path.join(d, "w1.log"))
            dl = 2.0 if variant == "deadline_while_down" else 60.0
            jid = store.create_job("tenant-a", f"e6-{rep}", limits={"deadline_s": dl})[0]["job_id"]
            wait_state(store, jid, ("WAITING_EVENT",))
            open_runs = store.conn.execute("SELECT COUNT(*) FROM step_runs WHERE ended_at IS NULL").fetchone()[0]
            t_kill = hard_kill(w)
            ev = None
            if variant == "approval_while_down":
                ev = approve(store, jid)
            elif variant == "deadline_while_down":
                time.sleep(max(0.0, store.get_job(jid)["deadline"] - time.time()) + 0.3)
            time.sleep(0.5)
            state_while_down = state(store, jid)
            t_spawn = time.time()
            w2 = spawn_worker(db, cfg_path, "w1", log_file=os.path.join(d, "w2.log"))
            ready = wait_for(lambda: ro(db).execute("SELECT ready_at FROM workers WHERE started_at>=? AND ready_at "
                                                    "IS NOT NULL", (t_spawn,)).fetchone())[0]
            if variant == "approval_after_restart":
                ev = approve(store, jid)
            final = wait_state(store, jid, TERMINAL)
            hard_kill(w2)
            j = store.get_job(jid)
            first_after = store.conn.execute("SELECT MIN(started_at) FROM step_runs WHERE job_id=? AND started_at>?",
                                             (jid, t_spawn)).fetchone()[0]
            out.append({"exp": "e6", "rep": rep, "variant": variant, "job_id": jid, "final": final,
                        "open_step_runs_while_waiting": open_runs, "state_while_worker_down": state_while_down,
                        "event_outcome": ev["outcome"] if ev else None,
                        "worker_ready_to_first_step_ms": round((first_after - ready) * 1000, 2) if first_after else None,
                        "worker_ready_to_terminal_ms": round((j["finished_at"] - ready) * 1000, 2),
                        "timeout_lateness_ms": round((j["finished_at"] - j["deadline"]) * 1000, 2)
                        if final == "TIMED_OUT" else None,
                        "downtime_s": round(t_spawn - t_kill, 3)})
    return out


# ------------------------------------------------------------------------- E7
def e7_cancel_stop(n, rng):
    d = fresh("e7")
    db = os.path.join(d, "s.db")
    sim = {"short": {**FAST_SIM["short"], "draft": 2.0}}
    cfg = cfg_fast(sim=sim, heartbeat_interval_s=0.1)
    store = Store(db, cfg)
    ledger = os.path.join(d, "ledger.db")
    out = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db"), ledger_path=ledger):
        for i in range(n):
            where = ["QUEUED", "RUNNING", "WAITING_EVENT"][i % 3]
            if where == "QUEUED":
                jid = store.create_job("tenant-b", f"e7-{i}", params={"collection_id": "x"},
                                       limits={"deadline_s": 60})[0]["job_id"]
                # cancel immediately (the job may not have been claimed yet)
            else:
                jid = store.create_job("tenant-b", f"e7-{i}", limits={"deadline_s": 60})[0]["job_id"]
                if where == "RUNNING":
                    wait_for(lambda: store.conn.execute("SELECT 1 FROM step_runs WHERE job_id=? AND step_key='draft#0.0' "
                                                        "AND ended_at IS NULL", (jid,)).fetchone(), interval=0.001)
                    time.sleep(rng.uniform(0.05, 1.5))
                else:
                    wait_state(store, jid, ("WAITING_EVENT",))
            r = store.cancel(jid)
            t_cancel = store.get_job(jid)["finished_at"]
            wait_for(lambda: store.conn.execute("SELECT COUNT(*) FROM step_runs WHERE job_id=? AND ended_at IS NULL",
                                                (jid,)).fetchone()[0] == 0, interval=0.001)
            last = store.conn.execute("SELECT step_key, outcome, ended_at FROM step_runs WHERE job_id=? ORDER BY id DESC "
                                      "LIMIT 1", (jid,)).fetchone()
            stop_ms = None
            if r.get("was") == "RUNNING" and last is not None:
                stop_ms = round((last[2] - t_cancel) * 1000, 2)
            elif r.get("was") in ("QUEUED", "WAITING_EVENT"):
                stop_ms = 0.0
            out.append({"exp": "e7", "job_id": jid, "cancel_in": r.get("was"), "outcome": r["outcome"],
                        "last_step": last[0] if last else None, "last_run_outcome": last[1] if last else None,
                        "stop_latency_ms": stop_ms})
        time.sleep(0.3)
    calls = [dict(c) for c in ro(ledger).execute("SELECT step_key, outcome FROM calls WHERE provider='llm'")]
    return out, check_invariants(store.conn), Counter(c["outcome"] for c in calls)


def summarize(name, vals):
    v = [x for x in vals if x is not None]
    return {"metric": name, "n": len(v), "p50": round(pct(v, 50), 3), "p95": round(pct(v, 95), 3),
            "max": round(max(v), 3) if v else None}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--race-reps", type=int, default=300)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--out", default=RAW)
    a = ap.parse_args(argv)
    rng = random.Random(a.seed)
    # worker and event senders are threads of ONE process here: shorten the GIL switch interval
    # (default 5 ms) so that measured latencies are not dominated by GIL hand-offs
    import sys

    sys.setswitchinterval(0.0005)
    from agentrt.logs import setup_logging
    os.makedirs(WORK, exist_ok=True)
    setup_logging(path=os.path.join(WORK, "events_experiment.log"))
    summary = {"seed": a.seed, "env": environment(), "time_scale": 1.0, "gil_switch_interval_s": 0.0005}

    e1, inv1 = e1_approval_on_time(a.n, rng)
    e2, inv2 = e2_no_approval_timeout(40)
    e3, inv3 = e3_duplicates(50)
    e4, inv4 = e4_late_events(40)
    print("e1-e4 done", flush=True)
    e5, inv5 = e5_races(a.race_reps, rng)
    print("e5 done", flush=True)
    e6 = e6_restart_while_waiting(5)
    e7, inv7, e7_calls = e7_cancel_stop(45, rng)
    print("e6-e7 done", flush=True)

    write_csv(os.path.join(a.out, "events_e1_approval.csv"), e1)
    write_csv(os.path.join(a.out, "events_e2_timeout.csv"), e2)
    write_csv(os.path.join(a.out, "events_e3_duplicates.csv"), e3)
    write_csv(os.path.join(a.out, "events_e4_late.csv"), e4)
    write_csv(os.path.join(a.out, "events_e5_races.csv"), e5)
    write_csv(os.path.join(a.out, "events_e6_restart_wait.csv"), e6)
    write_csv(os.path.join(a.out, "events_e7_cancel_stop.csv"), e7)

    summary["e1"] = {"final": Counter(r["final"] for r in e1), "outcomes": Counter(r["outcome"] for r in e1),
                     "latency": [summarize("event_to_transition_ms", [r["event_to_transition_ms"] for r in e1]),
                                 summarize("client_roundtrip_ms", [r["client_roundtrip_ms"] for r in e1]),
                                 summarize("event_to_next_step_start_ms", [r["event_to_next_step_start_ms"] for r in e1])],
                     "invariants": inv1}
    summary["e2"] = {"final": Counter(r["final"] for r in e2),
                     "lateness": summarize("timeout_lateness_ms", [r["timeout_lateness_ms"] for r in e2]),
                     "step_runs_during_wait_total": sum(r["step_runs_during_wait"] for r in e2), "invariants": inv2}
    summary["e3"] = {"events_sent": sum(r["copies"] for r in e3), "applied": sum(r["applied"] for r in e3),
                     "duplicates_detected": sum(r["duplicates"] for r in e3),
                     "jobs_with_more_than_one_transition": sum(1 for r in e3 if r["waiting_to_queued_transitions"] != 1),
                     "final": Counter(r["final"] for r in e3), "invariants": inv3}
    summary["e4"] = {"late_events": len(e4), "event_outcomes": Counter(r["event_outcome"] for r in e4),
                     "cancel_outcomes": Counter(r["cancel_outcome"] for r in e4),
                     "state_changed": sum(1 for r in e4 if r["state_before"] != r["state_after"]),
                     "version_changed": sum(1 for r in e4 if r["version_changed"]), "invariants": inv4}
    summary["e5"] = {"reps": len(e5), "final": Counter(r["final"] for r in e5),
                     "terminal_from": Counter(f"{r['terminal_from']}->{r['final']}" for r in e5),
                     "approval_outcomes": Counter(r["approval_outcome"] for r in e5),
                     "cancel_outcomes": Counter(r["cancel_outcome"] for r in e5),
                     "multi_terminal_jobs": sum(1 for r in e5 if r["n_terminal_transitions"] != 1),
                     "orphan_publications": sum(1 for r in e5 if r["orphan_publication"]),
                     "succeeded_without_publication": sum(1 for r in e5 if r["succeeded_without_publication"]),
                     "published_then_retracted": sum(1 for r in e5 if r["publication_retracted"]),
                     "cancel_stop": summarize("cancel_stop_ms", [r["cancel_stop_ms"] for r in e5]),
                     "invariants": inv5}
    summary["e6"] = {"rows": e6}
    summary["e7"] = {"by_state": {k: summarize(f"stop_latency_ms[{k}]", [r["stop_latency_ms"] for r in e7
                                                                         if r["cancel_in"] == k])
                                  for k in ("QUEUED", "RUNNING", "WAITING_EVENT")},
                     "cancel_in": Counter(r["cancel_in"] for r in e7),
                     "llm_call_outcomes": e7_calls, "invariants": inv7}
    with open(os.path.join(a.out, "events_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2, default=dict)
    print(json.dumps({k: v for k, v in summary.items() if k not in ("env", "e6")}, indent=1, default=dict)[:6000])


if __name__ == "__main__":
    main()
