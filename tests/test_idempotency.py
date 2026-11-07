"""Idempotency of job creation (unique index) and of publication (Idempotency-Key)."""
import asyncio
import threading

import pytest
from fastapi.testclient import TestClient

from agentrt.api import create_app
from agentrt.config import Config
from agentrt.errors import PermanentError
from agentrt.publisher import FakePublisherService, LocalPublisherClient, create_publisher_app
from agentrt.store import Store


def test_create_job_same_key_returns_same_job(tmp_path):
    s = Store(str(tmp_path / "s.db"))
    j1, c1 = s.create_job("t1", "key-1")
    j2, c2 = s.create_job("t1", "key-1")
    assert c1 and not c2 and j1["job_id"] == j2["job_id"]
    assert s.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
    j3, c3 = s.create_job("t2", "key-1")  # key is scoped per tenant
    assert c3 and j3["job_id"] != j1["job_id"]


def test_unique_index_is_the_backstop(tmp_path):
    s = Store(str(tmp_path / "s.db"))
    s.create_job("t1", "dup")
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        s.conn.execute("INSERT INTO jobs(job_id,tenant_id,idempotency_key,created_at,deadline,state,updated_at)"
                       " VALUES('x','t1','dup',0,0,'QUEUED',0)")


def test_concurrent_creates_with_same_key_create_one_job(tmp_path):
    db = str(tmp_path / "s.db")
    Store(db)  # init schema
    ids, errors = [], []

    def go():
        try:
            st = Store(db)
            j, _ = st.create_job("t1", "same-key")
            ids.append(j["job_id"])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ths = [threading.Thread(target=go) for _ in range(16)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    assert not errors
    assert len(set(ids)) == 1
    assert Store(db).conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


def test_publisher_dedups_by_idempotency_key(tmp_path):
    svc = FakePublisherService(str(tmp_path / "p.db"))
    cl = LocalPublisherClient(svc)

    async def go():
        a = await cl.publish("job-1:publish", {"x": 1})
        b = await cl.publish("job-1:publish", {"x": 1})
        return a, b

    a, b = asyncio.run(go())
    assert a["publication_id"] == b["publication_id"] and b["replayed"]
    st = svc.stats()
    assert st["physical_publish_requests"] == 2 and st["logical_publications"] == 1
    with pytest.raises(PermanentError):
        asyncio.run(cl.publish("job-1:publish", {"x": 2}))  # same key, different body -> 409


def test_publisher_ack_lost_then_retry_returns_previous_result(tmp_path):
    svc = FakePublisherService(str(tmp_path / "p.db"))
    svc.set_faults(lose_ack_first_n=1)
    cl = LocalPublisherClient(svc)
    from agentrt.errors import TransientError

    with pytest.raises(TransientError):
        asyncio.run(cl.publish("k", {"a": 1}))
    rec = asyncio.run(cl.publish("k", {"a": 1}))
    assert rec["replayed"] is True
    assert svc.stats()["logical_publications"] == 1 and svc.stats()["physical_publish_requests"] == 2


def test_http_api_create_idempotent_and_429(tmp_path):
    app = create_app(str(tmp_path / "s.db"), Config(max_queue_size=2))
    c = TestClient(app)
    r1 = c.post("/jobs", json={"tenant_id": "t1"}, headers={"Idempotency-Key": "abc"})
    r2 = c.post("/jobs", json={"tenant_id": "t1"}, headers={"Idempotency-Key": "abc"})
    assert r1.status_code == 201 and r2.status_code == 200 and r1.json()["job_id"] == r2.json()["job_id"]
    assert c.post("/jobs", json={"tenant_id": "t1"}).status_code == 201
    r4 = c.post("/jobs", json={"tenant_id": "t1"})
    assert r4.status_code == 429 and r4.json() == {"status": "REJECTED", "reason": "queue_full"}
    jid = r1.json()["job_id"]
    assert c.get(f"/jobs/{jid}").json()["state"] == "QUEUED"
    assert c.post(f"/jobs/{jid}/cancel").json()["outcome"] == "cancelled"
    assert c.get("/metrics").json()["rejected"] == 1


def test_http_publisher_app(tmp_path):
    c = TestClient(create_publisher_app(str(tmp_path / "p.db")))
    r1 = c.post("/v1/publications", json={"a": 1}, headers={"Idempotency-Key": "k1"})
    r2 = c.post("/v1/publications", json={"a": 1}, headers={"Idempotency-Key": "k1"})
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["publication_id"] == r2.json()["publication_id"]
    assert c.get("/v1/stats").json()["logical_publications"] == 1
