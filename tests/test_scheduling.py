"""Concurrency limits (checked on the execution trace), admission control, DRR fairness, aging."""
import asyncio
import random

import pytest

from agentrt.scheduler import make_policy
from agentrt.store import Rejected, Store
from helpers import fast_cfg, make_worker, run_until


def max_overlap(intervals):
    ev = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals], key=lambda x: (x[0], x[1]))
    cur = best = 0
    for _, d in ev:
        cur += d
        best = max(best, cur)
    return best


@pytest.mark.parametrize("policy", ["fifo", "drr"])
def test_global_and_per_tenant_limits_never_exceeded(tmp_path, policy):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(global_concurrency=3, per_tenant_concurrency=2, policy=policy)
    s = Store(db, cfg)
    rng = random.Random(5)
    ids = [s.create_job(rng.choice(["a", "b", "c", "d"]), job_class=rng.choice(["short", "short", "long"]),
                        params={"require_approval": False})[0]["job_id"] for _ in range(40)]
    done = lambda: all(s.get_job(i)["state"] == "SUCCEEDED" for i in ids)  # noqa: E731
    asyncio.run(run_until(make_worker(db, cfg), done, timeout=60))
    runs = s.conn.execute("SELECT tenant_id, started_at, ended_at FROM step_runs").fetchall()
    assert len(runs) > 200
    assert max_overlap([(r[1], r[2]) for r in runs]) <= 3
    for t in "abcd":
        assert max_overlap([(r[1], r[2]) for r in runs if r[0] == t]) <= 2
    # the limits were actually reached (the test is not vacuous)
    assert max_overlap([(r[1], r[2]) for r in runs]) == 3


def test_limits_hold_across_two_worker_processes_sharing_the_db(tmp_path):
    """Limits are enforced inside the claim transaction, so they hold for several workers."""
    db = str(tmp_path / "s.db")
    cfg = fast_cfg(global_concurrency=2, per_tenant_concurrency=1)
    s = Store(db, cfg)
    ids = [s.create_job(t, params={"require_approval": False})[0]["job_id"] for t in ["a", "b", "c"] * 5]

    async def two():
        w1, w2 = make_worker(db, cfg, "w1"), make_worker(db, cfg, "w2")
        t2 = asyncio.create_task(w2.run())
        try:
            await run_until(w1, lambda: all(s.get_job(i)["state"] == "SUCCEEDED" for i in ids), timeout=60)
        finally:
            w2.stop()
            await t2

    asyncio.run(two())
    runs = s.conn.execute("SELECT tenant_id, started_at, ended_at, worker FROM step_runs").fetchall()
    assert len({r[3].split(":")[0] for r in runs}) == 2  # both workers did work
    assert max_overlap([(r[1], r[2]) for r in runs]) <= 2
    for t in "abc":
        assert max_overlap([(r[1], r[2]) for r in runs if r[0] == t]) <= 1


def test_admission_control_rejects_with_reason(tmp_path):
    s = Store(str(tmp_path / "s.db"), fast_cfg(max_queue_size=3, max_queue_per_tenant=2))
    s.create_job("a")
    s.create_job("a")
    with pytest.raises(Rejected) as e:
        s.create_job("a")
    assert e.value.reason == "tenant_queue_full"
    s.create_job("b")
    with pytest.raises(Rejected) as e:
        s.create_job("c")
    assert e.value.reason == "queue_full"
    assert s.counts()["rejected"] == 2


def _pick_order(policy_name, n_heavy=8, light_tenants=("l1", "l2")):
    s = Store(":memory:", fast_cfg(policy=policy_name, global_concurrency=1, per_tenant_concurrency=1,
                                   drr_quantum_s=0.2))  # quantum = cost of one short fetch step
    for _ in range(n_heavy):
        s.create_job("heavy", params={"require_approval": False})
    for t in light_tenants:
        s.create_job(t, params={"require_approval": False})
    pol = make_policy(s.cfg)
    order = []
    for _ in range(n_heavy + len(light_tenants)):
        j = s.claim_next("w:1", pol)
        order.append(j["tenant_id"])
        s.cancel(j["job_id"])  # free the slot (cancel -> run closed below)
        s.conn.execute("UPDATE step_runs SET ended_at=? WHERE ended_at IS NULL", (s.now(),))
    return order


def test_fifo_serves_heavy_backlog_first_drr_interleaves():
    fifo = _pick_order("fifo")
    drr = _pick_order("drr")
    assert fifo[:8] == ["heavy"] * 8  # light tenants wait behind the whole backlog
    assert set(drr[:3]) == {"heavy", "l1", "l2"}  # one round serves everybody


def test_aging_lets_old_low_priority_job_overtake():
    cfg = fast_cfg(policy="drr", aging_interval_s=1.0)  # +1 priority per 1 virtual s (20 ms real)
    s = Store(":memory:", cfg)
    old, _ = s.create_job("t", priority=0, params={"require_approval": False})
    import time

    time.sleep(cfg.real(3.0))  # old job ages by ~3 priority points
    s.create_job("t", priority=2, params={"require_approval": False})
    j = s.claim_next("w:1", make_policy(cfg))
    assert j["job_id"] == old["job_id"]
    s2 = Store(":memory:", cfg)
    s2.create_job("t", priority=0, params={"require_approval": False})
    hi, _ = s2.create_job("t", priority=2, params={"require_approval": False})
    assert s2.claim_next("w:1", make_policy(cfg))["job_id"] == hi["job_id"]  # without aging time, priority wins
