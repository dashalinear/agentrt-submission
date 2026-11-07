import asyncio

from agentrt.store import Store
from helpers import fast_cfg, make_worker, run_until


def test_happy_path_with_approval(tmp_path):
    db = str(tmp_path / "s.db")
    cfg = fast_cfg()
    store = Store(db, cfg)
    job, created = store.create_job("t1", "k1", "short", params={"require_approval": True, "min_docs": 5})
    assert created

    async def scenario():
        w = make_worker(db, cfg)
        await run_until(w, lambda: store.get_job(job["job_id"])["state"] == "WAITING_EVENT")
        j = store.get_job(job["job_id"])
        r = store.submit_event(j["job_id"], "e1", "approval", {"approval_id": j["approval_id"], "decision": "approve"})
        assert r["outcome"] == "applied"
        w2 = make_worker(db, cfg)
        await run_until(w2, lambda: store.get_job(job["job_id"])["state"] == "SUCCEEDED")

    asyncio.run(scenario())
    j = store.get_job(job["job_id"])
    steps = list(store.step_results(j["job_id"]).keys())
    # min_docs=5 > 3 docs per page -> agent calls the tool once more
    assert "fetch_documents#1" in steps and "publish#0" in steps
    assert j["result"]["publication_id"].startswith("pub-")
