"""Race resolution: priority rules (deterministic) + randomized approval/cancel/deadline/step races."""
import random
import threading
import time

from agentrt.config import Config
from agentrt.harness import ThreadWorker, check_invariants
from agentrt.publisher import FakePublisherService
from agentrt.scheduler import make_policy
from agentrt.store import StepCompletion, Store
from helpers import wait_for

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
FAST = {"short": {"fetch_documents": 0.02, "extract": 0.01, "draft": 0.03, "draft_chunks": 1, "decide": 0.01,
                  "save_report": 0.01, "publish": 0.02, "docs": 3}}


def cfg(**kw):
    base = dict(time_scale=1.0, poll_interval_s=0.003, heartbeat_interval_s=0.05, lease_timeout_s=2.0,
                global_concurrency=64, per_tenant_concurrency=64, sim=FAST)
    base.update(kw)
    return Config(**base)


def _waiting(tmp_path, deadline_s):
    db = str(tmp_path / "s.db")
    c = cfg()
    s = Store(db, c)
    j, _ = s.create_job("t", limits={"deadline_s": deadline_s})
    with ThreadWorker(db, c, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(j["job_id"])["state"] == "WAITING_EVENT")
    return s, s.get_job(j["job_id"])


def test_deadline_beats_approval_when_timer_not_yet_fired(tmp_path):
    s, j = _waiting(tmp_path, 0.4)
    time.sleep(max(0, j["deadline"] - time.time()) + 0.02)  # deadline passed, no worker -> timer not fired
    r = s.submit_event(j["job_id"], "a", "approval", {"approval_id": j["approval_id"]})
    assert r["outcome"] == "deadline_won" and s.get_job(j["job_id"])["state"] == "TIMED_OUT"


def test_cancel_beats_deadline_when_timer_not_yet_fired(tmp_path):
    s, j = _waiting(tmp_path, 0.4)
    time.sleep(max(0, j["deadline"] - time.time()) + 0.02)
    assert s.cancel(j["job_id"])["outcome"] == "cancelled"
    assert s.get_job(j["job_id"])["state"] == "CANCELLED"


def test_deadline_beats_step_completion(tmp_path):
    s = Store(str(tmp_path / "s.db"), cfg())
    j, _ = s.create_job("t", limits={"deadline_s": 0.1})
    claimed = s.claim_next("w:1", make_policy(s.cfg))
    time.sleep(0.15)
    assert s.complete_step(claimed, StepCompletion(result={"docs": []}, next_step="extract#0")) == "deadline"
    assert s.get_job(j["job_id"])["state"] == "TIMED_OUT"
    assert s.step_result(j["job_id"], "fetch_documents#0") is None


def test_randomized_races_preserve_invariants(tmp_path):
    db = str(tmp_path / "s.db")
    c = cfg()
    s = Store(db, c)
    rng = random.Random(123)
    reps, jobs = 60, []
    with ThreadWorker(db, c, publisher_db=db + ".pub"):
        for b0 in range(0, reps, 10):
            batch = [s.create_job("t", f"r{i}", limits={"deadline_s": 0.6})[0]["job_id"] for i in range(b0, b0 + 10)]
            jobs += batch

            def actor(jid, kind, off):
                s2 = Store(db, c, init=False)
                wait_for(lambda: s2.get_job(jid)["state"] in ("WAITING_EVENT",) + TERMINAL, interval=0.003)
                target = s2.get_job(jid)["deadline"] + off
                while time.time() < target:
                    time.sleep(0.0005)
                if kind == "a":
                    s2.submit_event(jid, "a-" + jid, "approval", {"approval_id": s2.get_job(jid)["approval_id"]})
                else:
                    s2.cancel(jid)

            ths = [threading.Thread(target=actor, args=(jid, k, rng.uniform(-0.15, 0.02) if k == "a" else
                                                        rng.uniform(-0.06, 0.06)))
                   for jid in batch for k in ("a", "c")]
            [t.start() for t in ths]
            [t.join() for t in ths]
            for jid in batch:
                wait_for(lambda: s.get_job(jid)["state"] in TERMINAL)
        time.sleep(0.2)
    inv = check_invariants(s.conn)
    assert inv["transitions_checked"] > 0
    assert {k: v for k, v in inv.items() if k != "transitions_checked"} == {
        "invalid_transitions": 0, "terminal_changes": 0, "version_gaps": 0, "multiple_terminal": 0,
        "log_state_mismatch": 0}
    pubs = FakePublisherService(db + ".pub").publications_by_key()
    for jid in jobs:
        st = s.get_job(jid)["state"]
        p = pubs.get(f"{jid}:publish")
        if st == "SUCCEEDED":
            assert p is not None and p["retracted_at"] is None
        else:  # no live publication may belong to a non-succeeded job
            assert p is None or p["retracted_at"] is not None
    assert s.conn.execute("SELECT COUNT(*) FROM step_runs WHERE ended_at IS NULL").fetchone()[0] == 0
