"""FakePublisherService: external test service with its OWN storage (separate SQLite file).

* Deduplicates publications by ``Idempotency-Key`` (unique primary key).
* Counts *physical* requests (every POST that reached the service) and *logical*
  publications (distinct keys actually published).
* Fault injection: ``lose_ack_first_n`` -> the first N publish requests are committed
  but answered with HTTP 503 ("side effect happened, ack lost").
Can run in-process (``LocalPublisherClient``) or as a separate HTTP process
(``python -m agentrt.cli publisher``) used through ``HttpPublisherClient``.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
import time
import uuid

from .errors import PermanentError, TransientError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS publications (
    idempotency_key TEXT PRIMARY KEY, publication_id TEXT NOT NULL, body_hash TEXT NOT NULL,
    body TEXT NOT NULL, created_at REAL NOT NULL, retracted_at REAL
);
CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, idempotency_key TEXT,
    ts REAL NOT NULL, outcome TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    message_id TEXT PRIMARY KEY, topic TEXT, payload TEXT, received_at REAL
);
CREATE TABLE IF NOT EXISTS faults (key TEXT PRIMARY KEY, value TEXT);
"""


class AckLost(TransientError):
    pass


class FakePublisherService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, timeout=60, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self.conn.executescript(_SCHEMA)

    # -- faults ---------------------------------------------------------------
    def set_faults(self, **kw) -> None:
        with self._lock:
            for k, v in kw.items():
                self.conn.execute("INSERT OR REPLACE INTO faults(key,value) VALUES(?,?)", (k, json.dumps(v)))

    def _fault(self, key, default=None):
        r = self.conn.execute("SELECT value FROM faults WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else default

    # -- API ------------------------------------------------------------------
    def publish(self, key: str, body: dict) -> tuple[int, dict]:
        """Returns (http_status, record). 201 = new, 200 = replay of the same key."""
        if not key:
            raise PermanentError("Idempotency-Key required")
        body_s = json.dumps(body, sort_keys=True, ensure_ascii=False)
        bh = hashlib.sha256(body_s.encode()).hexdigest()
        with self._lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT * FROM publications WHERE idempotency_key=?", (key,)).fetchone()
                if row is not None:
                    if row["body_hash"] != bh:
                        c.execute("INSERT INTO requests(kind,idempotency_key,ts,outcome) VALUES('publish',?,?,?)",
                                  (key, time.time(), "conflict"))
                        c.execute("COMMIT")
                        return 409, {"error": "idempotency key reused with different body"}
                    outcome, status = "replay", 200
                    rec = {"publication_id": row["publication_id"], "idempotency_key": key, "replayed": True}
                else:
                    pid = "pub-" + uuid.uuid4().hex[:12]
                    c.execute("INSERT INTO publications VALUES (?,?,?,?,?,NULL)", (key, pid, bh, body_s, time.time()))
                    outcome, status = "created", 201
                    rec = {"publication_id": pid, "idempotency_key": key, "replayed": False}
                lose_n = int(self._fault("lose_ack_first_n", 0) or 0)
                lost_so_far = c.execute("SELECT COUNT(*) FROM requests WHERE kind='publish' AND outcome LIKE '%ack_lost'").fetchone()[0]
                ack_lost = lost_so_far < lose_n
                c.execute("INSERT INTO requests(kind,idempotency_key,ts,outcome) VALUES('publish',?,?,?)",
                          (key, time.time(), outcome + ("+ack_lost" if ack_lost else "")))
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        lat = float(self._fault("latency_s", 0) or 0)
        if lat:
            time.sleep(lat)
        if ack_lost:
            raise AckLost("publication committed but response lost (simulated 503)")
        return status, rec

    def retract(self, key: str) -> bool:
        with self._lock:
            cur = self.conn.execute("UPDATE publications SET retracted_at=? WHERE idempotency_key=? AND retracted_at IS NULL",
                                    (time.time(), key))
            self.conn.execute("INSERT INTO requests(kind,idempotency_key,ts,outcome) VALUES('retract',?,?,?)",
                              (key, time.time(), "ok" if cur.rowcount else "noop"))
            return cur.rowcount > 0

    def notify(self, message_id: str, topic: str, payload: dict) -> bool:
        with self._lock:
            cur = self.conn.execute("INSERT OR IGNORE INTO notifications VALUES (?,?,?,?)",
                                    (message_id, topic, json.dumps(payload), time.time()))
            self.conn.execute("INSERT INTO requests(kind,idempotency_key,ts,outcome) VALUES('notify',?,?,?)",
                              (message_id, time.time(), "created" if cur.rowcount else "replay"))
            return cur.rowcount > 0

    def stats(self) -> dict:
        c = self.conn
        q = lambda s: c.execute(s).fetchone()[0]  # noqa: E731
        return {
            "physical_publish_requests": q("SELECT COUNT(*) FROM requests WHERE kind='publish'"),
            "logical_publications": q("SELECT COUNT(*) FROM publications"),
            "active_publications": q("SELECT COUNT(*) FROM publications WHERE retracted_at IS NULL"),
            "retracted": q("SELECT COUNT(*) FROM publications WHERE retracted_at IS NOT NULL"),
            "replays": q("SELECT COUNT(*) FROM requests WHERE kind='publish' AND outcome LIKE 'replay%'"),
            "acks_lost": q("SELECT COUNT(*) FROM requests WHERE kind='publish' AND outcome LIKE '%ack_lost'"),
            "conflicts": q("SELECT COUNT(*) FROM requests WHERE kind='publish' AND outcome='conflict'"),
            "physical_notifications": q("SELECT COUNT(*) FROM requests WHERE kind='notify'"),
            "logical_notifications": q("SELECT COUNT(*) FROM notifications"),
        }

    def publications_by_key(self) -> dict:
        return {r["idempotency_key"]: dict(r) for r in self.conn.execute("SELECT * FROM publications")}


# --------------------------------------------------------------------------- HTTP app
def create_publisher_app(db_path: str):
    from fastapi import FastAPI, Header, HTTPException
    from fastapi.responses import JSONResponse

    svc = FakePublisherService(db_path)
    app = FastAPI(title="FakePublisherService")
    app.state.svc = svc

    @app.post("/v1/publications")
    def publish(body: dict, idempotency_key: str = Header(..., alias="Idempotency-Key")):
        try:
            status, rec = svc.publish(idempotency_key, body)
        except AckLost as e:
            raise HTTPException(503, str(e))
        return JSONResponse(rec, status_code=status)

    @app.delete("/v1/publications/{key}")
    def retract(key: str):
        return {"retracted": svc.retract(key)}

    @app.post("/v1/notifications")
    def notify(body: dict, idempotency_key: str = Header(..., alias="Idempotency-Key")):
        created = svc.notify(idempotency_key, body.get("topic", ""), body.get("payload", {}))
        return JSONResponse({"created": created}, status_code=201 if created else 200)

    @app.get("/v1/stats")
    def stats():
        return svc.stats()

    @app.post("/admin/faults")
    def faults(body: dict):
        svc.set_faults(**body)
        return {"ok": True}

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


# --------------------------------------------------------------------------- clients
class LocalPublisherClient:
    """In-process client (same semantics, no network)."""

    def __init__(self, service: FakePublisherService):
        self.svc = service

    async def publish(self, key: str, body: dict) -> dict:
        status, rec = await asyncio.to_thread(self.svc.publish, key, body)
        if status == 409:
            raise PermanentError(rec["error"])
        return rec

    async def retract(self, key: str) -> bool:
        return await asyncio.to_thread(self.svc.retract, key)

    async def notify(self, message_id: str, topic: str, payload: dict) -> bool:
        return await asyncio.to_thread(self.svc.notify, message_id, topic, payload)


class HttpPublisherClient:
    def __init__(self, base_url: str, timeout_s: float = 10.0):
        import httpx

        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout_s)

    async def _req(self, method: str, path: str, **kw):
        import httpx

        try:
            r = await self._client.request(method, self.base_url + path, **kw)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise TransientError(f"publisher transport error: {type(e).__name__}") from e
        if r.status_code >= 500 or r.status_code == 429:
            raise TransientError(f"publisher HTTP {r.status_code}: {r.text[:200]}")
        if r.status_code >= 400:
            raise PermanentError(f"publisher HTTP {r.status_code}: {r.text[:200]}")
        return r.json()

    async def publish(self, key: str, body: dict) -> dict:
        return await self._req("POST", "/v1/publications", json=body, headers={"Idempotency-Key": key})

    async def retract(self, key: str) -> bool:
        return (await self._req("DELETE", f"/v1/publications/{key}"))["retracted"]

    async def notify(self, message_id: str, topic: str, payload: dict) -> bool:
        return (await self._req("POST", "/v1/notifications", json={"topic": topic, "payload": payload},
                                headers={"Idempotency-Key": message_id}))["created"]
