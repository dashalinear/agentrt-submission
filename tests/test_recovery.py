"""Recovery after a REAL kill of the worker process (Popen.kill = SIGKILL / TerminateProcess)."""
import os
import sqlite3
import time

from agentrt.config import Config
from agentrt.ledger import CallLedger
from agentrt.publisher import FakePublisherService
from agentrt.store import Store
from helpers import spawn_worker, wait_for

SIM = {"short": {"fetch_documents": 0.2, "extract": 0.1, "draft": 0.8, "draft_chunks": 2, "decide": 0.02,
                 "save_report": 0.02, "publish": 0.05, "docs": 3}}


def _setup(tmp_path, **faults):
    db, ledger, pub = (str(tmp_path / n) for n in ("s.db", "ledger.db", "pub.db"))
    cfg = Config(time_scale=1.0, lease_timeout_s=1.5, heartbeat_interval_s=0.2, reaper_interval_s=0.1,
                 poll_interval_s=0.01, sim=SIM, faults=faults)
    cfg_path = str(tmp_path / "cfg.json")
    cfg.dump(cfg_path)
    store = Store(db, cfg)
    return db, ledger, pub, cfg_path, store


def _calls(ledger):
    c = sqlite3.connect(ledger)
    c.row_factory = sqlite3.Row
    return [dict(r) for r in c.execute("SELECT * FROM calls")]


def _state(store, jid):
    return store.get_job(jid)["state"]


def test_kill_between_steps_does_not_repeat_checkpointed_steps(tmp_path):
    db, ledger, pub, cfg_path, store = _setup(tmp_path, pause_after_step={"extract": 1.0})
    job, _ = store.create_job("t", "k", params={"require_approval": False})
    w = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_db=pub)
    wait_for(lambda: store.step_result(job["job_id"], "extract#0"))
    time.sleep(0.2)
    w.kill()
    w.wait()
    w2 = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_db=pub)
    try:
        wait_for(lambda: _state(store, job["job_id"]) == "SUCCEEDED", timeout=30)
    finally:
        w2.kill()
    calls = _calls(ledger)
    assert sum(1 for c in calls if c["name"] == "fetch_documents") == 1  # not repeated
    keys = [c["step_key"] for c in calls]
    assert len(keys) == len(set(keys))  # no external call repeated at all
    res = {r[0]: r[1] for r in store.conn.execute("SELECT step_key, created_at FROM step_results WHERE job_id=?",
                                                  (job["job_id"],))}
    assert not [c for c in calls if c["step_key"] in res and c["started_at"] > res[c["step_key"]]]


def test_kill_during_llm_call_is_retried_after_lease_expiry(tmp_path):
    db, ledger, pub, cfg_path, store = _setup(tmp_path)
    job, _ = store.create_job("t", "k", params={"require_approval": False})
    w = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_db=pub)
    wait_for(lambda: os.path.exists(ledger) and any(c["provider"] == "llm" and c["ended_at"] is None
                                                    for c in _calls(ledger)))
    t_kill = time.time()
    w.kill()
    w.wait()
    w2 = spawn_worker(db, cfg_path, "w2", ledger=ledger, publisher_db=pub)  # different worker -> must wait lease
    try:
        wait_for(lambda: _state(store, job["job_id"]) == "SUCCEEDED", timeout=30)
    finally:
        w2.kill()
    draft0 = [c for c in _calls(ledger) if c["step_key"] == "draft#0.0"]
    assert len(draft0) == 2 and draft0[0]["ended_at"] is None and draft0[1]["outcome"] == "ok"
    runs = store.conn.execute("SELECT outcome, started_at FROM step_runs WHERE job_id=? AND step_key='draft#0.0' "
                              "ORDER BY id", (job["job_id"],)).fetchall()
    assert runs[0][0] == "lease_expired"
    assert runs[1][1] - t_kill > 0.5  # resumed only after the lease (1.5 s) ran out


def test_side_effect_without_ack_is_not_duplicated(tmp_path):
    marker = str(tmp_path / "crashed")
    db, ledger, pub, cfg_path, store = _setup(tmp_path, crash_after_publish_response=True, crash_marker=marker)
    job, _ = store.create_job("t", "k", params={"require_approval": False})
    w = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_db=pub)
    w.wait(30)  # worker hard-crashes (exit 137) right after the publisher committed
    assert os.path.exists(marker)
    assert _state(store, job["job_id"]) == "RUNNING"
    w2 = spawn_worker(db, cfg_path, "w1", ledger=ledger, publisher_db=pub)
    try:
        wait_for(lambda: _state(store, job["job_id"]) == "SUCCEEDED", timeout=30)
    finally:
        w2.kill()
    st = FakePublisherService(pub).stats()
    assert st["physical_publish_requests"] == 2
    assert st["logical_publications"] == 1 and st["replays"] == 1
    assert store.get_job(job["job_id"])["result"]["replayed"] is True
