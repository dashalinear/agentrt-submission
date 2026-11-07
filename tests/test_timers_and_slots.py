"""Durable deadlines (queue and waiting, across restarts) and 'waiting does not hold a slot'."""
import time

from agentrt.harness import ThreadWorker
from agentrt.store import Store
from helpers import fast_cfg, wait_for


def test_deadline_in_queue_fires_after_restart(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t", limits={"deadline_s": 5})  # 0.1 s real; no worker running
    time.sleep(0.2)
    assert s.get_job(j["job_id"])["state"] == "QUEUED"
    with ThreadWorker(db, cfg, publisher_db=db + ".pub") as tw:
        assert tw.worker.ready_at is not None
    assert s.get_job(j["job_id"])["state"] == "TIMED_OUT"
    assert s.transitions(j["job_id"])[-1]["src"] == "QUEUED"


def test_deadline_while_waiting_survives_worker_restart(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t", limits={"deadline_s": 25})  # 0.5 s real
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(j["job_id"])["state"] == "WAITING_EVENT")
    # worker gone; deadline passes while nobody runs
    time.sleep(max(0, s.get_job(j["job_id"])["deadline"] - time.time()) + 0.1)
    assert s.get_job(j["job_id"])["state"] == "WAITING_EVENT"
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        pass  # startup fires overdue timers
    jj = s.get_job(j["job_id"])
    assert jj["state"] == "TIMED_OUT" and s.transitions(j["job_id"])[-1]["src"] == "WAITING_EVENT"


def test_waiting_job_does_not_occupy_execution_slot(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(global_concurrency=1, per_tenant_concurrency=1)
    s = Store(db, cfg)
    a, _ = s.create_job("t", params={"require_approval": True})
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(a["job_id"])["state"] == "WAITING_EVENT")
        b, _ = s.create_job("t", params={"require_approval": False})
        wait_for(lambda: s.get_job(b["job_id"])["state"] == "SUCCEEDED")  # B runs although limit=1 and A waits
        assert s.get_job(a["job_id"])["state"] == "WAITING_EVENT"
    t_wait = next(t["ts"] for t in s.transitions(a["job_id"]) if t["dst"] == "WAITING_EVENT")
    a_runs_during_wait = s.conn.execute("SELECT COUNT(*) FROM step_runs WHERE job_id=? AND (ended_at IS NULL OR "
                                        "started_at > ?)", (a["job_id"], t_wait)).fetchone()[0]
    assert a_runs_during_wait == 0
    b_runs = s.conn.execute("SELECT COUNT(*) FROM step_runs WHERE job_id=?", (b["job_id"],)).fetchone()[0]
    assert b_runs >= 6
