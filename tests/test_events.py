"""Event handling: dedup by event_id, wrong-state / stale / late events ignored, approval & rejection."""
import time

from agentrt.harness import ThreadWorker
from agentrt.store import Store
from helpers import fast_cfg, wait_for

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")


def _waiting_job(tmp_path, cfg=None, **limits):
    db = str(tmp_path / "s.db")
    cfg = cfg or fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t", limits=limits or None)
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(j["job_id"])["state"] == "WAITING_EVENT")
    return db, cfg, s, s.get_job(j["job_id"])


def test_duplicate_event_applied_once(tmp_path):
    db, cfg, s, j = _waiting_job(tmp_path)
    p = {"approval_id": j["approval_id"], "decision": "approve"}
    assert s.submit_event(j["job_id"], "e-1", "approval", p)["outcome"] == "applied"
    assert s.submit_event(j["job_id"], "e-1", "approval", p)["outcome"] == "duplicate"
    assert s.submit_event(j["job_id"], "e-1", "approval", p)["outcome"] == "duplicate"
    n = s.conn.execute("SELECT COUNT(*) FROM transitions WHERE job_id=? AND src='WAITING_EVENT'",
                       (j["job_id"],)).fetchone()[0]
    assert n == 1
    assert s.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert s.conn.execute("SELECT COUNT(*) FROM event_receipts WHERE outcome='duplicate'").fetchone()[0] == 2


def test_event_in_wrong_state_is_ignored_and_logged(tmp_path):
    s = Store(str(tmp_path / "s.db"), fast_cfg())
    j, _ = s.create_job("t")  # QUEUED
    r = s.submit_event(j["job_id"], "e-1", "approval", {"approval_id": "whatever"})
    assert r["outcome"] == "ignored_state" and s.get_job(j["job_id"])["version"] == 1
    assert s.conn.execute("SELECT outcome FROM events WHERE event_id='e-1'").fetchone()[0] == "ignored_state"


def test_stale_approval_id_is_ignored(tmp_path):
    db, cfg, s, j = _waiting_job(tmp_path)
    r = s.submit_event(j["job_id"], "e-1", "approval", {"approval_id": "apr-old", "decision": "approve"})
    assert r["outcome"] == "ignored_stale_approval" and s.get_job(j["job_id"])["state"] == "WAITING_EVENT"


def test_approval_after_cancel_or_timeout_does_not_change_state(tmp_path):
    db, cfg, s, j = _waiting_job(tmp_path)
    s.cancel(j["job_id"])
    v = s.get_job(j["job_id"])["version"]
    r = s.submit_event(j["job_id"], "late", "approval", {"approval_id": j["approval_id"]})
    assert r["outcome"] == "ignored_terminal"
    assert s.get_job(j["job_id"])["state"] == "CANCELLED" and s.get_job(j["job_id"])["version"] == v


def test_rejection_revises_then_fails(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t", limits={"max_revisions": 1})
    jid = j["job_id"]
    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(jid)["state"] == "WAITING_EVENT")
        a1 = s.get_job(jid)["approval_id"]
        assert s.submit_event(jid, "r1", "approval", {"approval_id": a1, "decision": "reject",
                                                      "comment": "add numbers"})["outcome"] == "applied"
        wait_for(lambda: s.get_job(jid)["state"] == "WAITING_EVENT" and s.get_job(jid)["approval_id"] != a1)
        a2 = s.get_job(jid)["approval_id"]
        # the old approval id is now stale
        assert s.submit_event(jid, "old", "approval", {"approval_id": a1})["outcome"] == "ignored_stale_approval"
        assert s.submit_event(jid, "r2", "approval", {"approval_id": a2, "decision": "reject"})["outcome"] == "applied"
    j = s.get_job(jid)
    assert j["state"] == "FAILED" and j["error"] == "rejected_by_approver"
    assert "draft#1.0" in s.step_results(jid) and "feedback#1" in s.step_results(jid)


def test_outbox_notifies_approval_request_and_terminal_state(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    s = Store(db, cfg)
    j, _ = s.create_job("t")
    jid = j["job_id"]
    from agentrt.publisher import FakePublisherService

    with ThreadWorker(db, cfg, publisher_db=db + ".pub"):
        wait_for(lambda: s.get_job(jid)["state"] == "WAITING_EVENT")
        s.submit_event(jid, "a", "approval", {"approval_id": s.get_job(jid)["approval_id"]})
        wait_for(lambda: s.get_job(jid)["state"] in TERMINAL)
        wait_for(lambda: s.conn.execute("SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL").fetchone()[0] == 0)
    topics = {r[0] for r in s.conn.execute("SELECT topic FROM outbox")}
    assert topics == {"approval.requested", "job.succeeded"}
    assert FakePublisherService(db + ".pub").stats()["logical_notifications"] == 2
    time.sleep(0)
