"""Cancellation in QUEUED / RUNNING / WAITING_EVENT; late results are discarded."""
import time

from agentrt.harness import ThreadWorker
from agentrt.scheduler import make_policy
from agentrt.store import StepCompletion, Store
from helpers import fast_cfg, wait_for


def test_cancel_queued(tmp_path):
    s = Store(str(tmp_path / "s.db"), fast_cfg())
    j, _ = s.create_job("t")
    assert s.cancel(j["job_id"])["outcome"] == "cancelled"
    assert s.get_job(j["job_id"])["state"] == "CANCELLED"
    assert s.cancel(j["job_id"])["outcome"] == "already_terminal"


def test_cancel_waiting(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t")
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(j["job_id"])["state"] == "WAITING_EVENT")
        r = s.cancel(j["job_id"])
        assert r["outcome"] == "cancelled" and r["was"] == "WAITING_EVENT"
        time.sleep(0.1)
    assert s.get_job(j["job_id"])["state"] == "CANCELLED"


def test_cancel_running_stops_step_and_no_new_steps(tmp_path):
    db = str(tmp_path / "s.db")
    sim = {"short": {"draft": 50.0}}  # 50 virtual s * 0.02 = 1 s real LLM call
    cfg = fast_cfg(sim=sim, heartbeat_interval_s=2.5)  # heartbeat every 50 ms real
    s = Store(db, cfg)
    j, _ = s.create_job("t", params={"require_approval": False}, limits={"step_timeout_s": 500})
    jid = j["job_id"]
    with ThreadWorker(db, cfg, publisher_db=db + ".pub", ledger_path=db + ".ledger"):
        wait_for(lambda: s.conn.execute("SELECT 1 FROM step_runs WHERE job_id=? AND step_key='draft#0.0' AND "
                                        "ended_at IS NULL", (jid,)).fetchone())
        r = s.cancel(jid)
        assert r["was"] == "RUNNING"
        t_cancel = s.get_job(jid)["finished_at"]
        wait_for(lambda: s.conn.execute("SELECT COUNT(*) FROM step_runs WHERE ended_at IS NULL").fetchone()[0] == 0)
        time.sleep(0.2)
    run = s.conn.execute("SELECT outcome, ended_at FROM step_runs WHERE job_id=? ORDER BY id DESC LIMIT 1",
                         (jid,)).fetchone()
    assert run[0] == "aborted_lease_lost"
    assert run[1] - t_cancel < 0.5  # stopped long before the 1 s call would have finished
    started_after = s.conn.execute("SELECT COUNT(*) FROM step_runs WHERE job_id=? AND started_at > ?",
                                   (jid, t_cancel)).fetchone()[0]
    assert started_after == 0
    assert s.step_result(jid, "draft#0.0") is None


def test_late_step_result_after_cancel_is_discarded(tmp_path):
    s = Store(str(tmp_path / "s.db"), fast_cfg())
    j, _ = s.create_job("t")
    claimed = s.claim_next("w:1", make_policy(s.cfg))
    s.cancel(j["job_id"])
    res = s.complete_step(claimed, StepCompletion(result={"docs": []}, next_step="extract#0"))
    assert res == "fenced"
    assert s.step_result(j["job_id"], "fetch_documents#0") is None
    assert s.get_job(j["job_id"])["state"] == "CANCELLED"
    assert s.conn.execute("SELECT outcome FROM step_runs").fetchone()[0] == "discarded_late_result"
