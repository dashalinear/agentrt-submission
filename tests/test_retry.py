"""Bounded retries with backoff+jitter, error classification, limits."""
import asyncio
import random
import time

from agentrt.store import Store
from helpers import fast_cfg, make_worker, run_until


def _terminal(store, jid):
    return store.get_job(jid)["state"] in ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")


def test_repeated_transient_error_bounded_then_failed(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(faults={"llm_fail": {"draft": -1}}, backoff_base_s=2.0, backoff_cap_s=8.0)
    store = Store(db, cfg)
    job, _ = store.create_job("t", params={"require_approval": False}, limits={"max_attempts": 3})
    asyncio.run(run_until(make_worker(db, cfg), lambda: _terminal(store, job["job_id"])))
    j = store.get_job(job["job_id"])
    assert j["state"] == "FAILED"
    assert "failed after 3 attempts" in j["error"] and "TransientError" in j["error"]
    runs = store.conn.execute("SELECT started_at, ended_at, outcome FROM step_runs WHERE job_id=? AND "
                              "step_key='draft#0.0' ORDER BY id", (job["job_id"],)).fetchall()
    assert [r[2] for r in runs] == ["transient_error"] * 3
    # pauses respect equal-jitter lower bounds: attempt n waits >= d_n/2, d_n = base*2^(n-1)
    for n, (a, b) in enumerate(zip(runs, runs[1:]), start=1):
        assert b[0] - a[1] >= cfg.real(2.0 * 2 ** (n - 1) / 2) * 0.95


def test_transient_then_success(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(faults={"llm_fail": {"draft": 2}})
    store = Store(db, cfg)
    job, _ = store.create_job("t", params={"require_approval": False})
    asyncio.run(run_until(make_worker(db, cfg), lambda: _terminal(store, job["job_id"])))
    assert store.get_job(job["job_id"])["state"] == "SUCCEEDED"
    n = store.conn.execute("SELECT COUNT(*) FROM step_runs WHERE job_id=? AND step_key='draft#0.0'",
                           (job["job_id"],)).fetchone()[0]
    assert n == 3


def test_permanent_error_fails_immediately(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(faults={"llm_permanent": ["draft"]})
    store = Store(db, cfg)
    job, _ = store.create_job("t", params={"require_approval": False})
    asyncio.run(run_until(make_worker(db, cfg), lambda: _terminal(store, job["job_id"])))
    j = store.get_job(job["job_id"])
    assert j["state"] == "FAILED" and j["error"].startswith("permanent error at step draft#0.0")
    assert store.conn.execute("SELECT COUNT(*) FROM step_runs WHERE step_key='draft#0.0'").fetchone()[0] == 1


def test_token_budget_and_max_steps(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    store = Store(db, cfg)
    j1, _ = store.create_job("t", params={"require_approval": False}, limits={"max_tokens": 50})
    j2, _ = store.create_job("t", params={"require_approval": False}, limits={"max_steps": 2})
    asyncio.run(run_until(make_worker(db, cfg), lambda: _terminal(store, j1["job_id"]) and _terminal(store, j2["job_id"])))
    a, b = store.get_job(j1["job_id"]), store.get_job(j2["job_id"])
    assert a["state"] == "FAILED" and "BudgetExceeded" in a["error"]
    assert b["state"] == "FAILED" and "max_steps exceeded" in b["error"]


def test_backoff_equal_jitter_bounds():
    s = Store(":memory:", fast_cfg(backoff_base_s=0.5, backoff_cap_s=4.0))
    rnd = random.Random(1)
    for attempt in range(1, 8):
        d = min(4.0, 0.5 * 2 ** (attempt - 1))
        for _ in range(100):
            v = s.backoff(attempt, rnd)
            assert d / 2 <= v <= d


def test_lease_loss_counts_as_attempt_poison_pill(tmp_path):
    """A job that keeps killing its worker is failed after max_attempts lease losses."""
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(lease_timeout_s=1.0)
    store = Store(db, cfg)
    from agentrt.scheduler import make_policy

    job, _ = store.create_job("t", params={"require_approval": False}, limits={"max_attempts": 2})
    pol = make_policy(cfg)
    for _ in range(2):
        assert store.claim_next("dead:1", pol) is not None
        time.sleep(cfg.real(1.0) + 0.01)
        store.reap_expired()
    j = store.get_job(job["job_id"])
    assert j["state"] == "FAILED" and "lease_expired" in j["error"]
