"""Call ledger for external calls (LLM / tool).

It plays the role of the provider-side billing log: a row is committed *before* the
call starts, and completed after it returns.  A row with ``ended_at IS NULL`` after a
crash is an external call whose outcome is unknown to the runtime (the provider may
have processed and billed it).  Stored in its own SQLite file, i.e. outside the
runtime's state store, so it survives a worker crash independently.
"""
from __future__ import annotations

import sqlite3
import threading
import time

_SCHEMA = """
CREATE TABLE IF NOT EXISTS calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL, name TEXT NOT NULL,
    job_id TEXT, step_key TEXT, attempt INTEGER,
    started_at REAL NOT NULL, ended_at REAL, outcome TEXT,
    prompt_tokens INTEGER NOT NULL DEFAULT 0, completion_tokens INTEGER NOT NULL DEFAULT 0,
    pid INTEGER, model TEXT
);
"""


class CallLedger:
    def __init__(self, path: str | None):
        self.path = path
        self._lock = threading.Lock()
        self._conn = None
        if path:
            self._conn = sqlite3.connect(path, timeout=60, isolation_level=None, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=60000")
            self._conn.executescript(_SCHEMA)

    def start(self, provider: str, name: str, job_id: str | None, step_key: str | None,
              attempt: int | None, prompt_tokens: int = 0, model: str | None = None) -> int | None:
        if not self._conn:
            return None
        import os
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO calls(provider,name,job_id,step_key,attempt,started_at,prompt_tokens,pid,model)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (provider, name, job_id, step_key, attempt, time.time(), prompt_tokens, os.getpid(), model))
            return cur.lastrowid

    def finish(self, call_id: int | None, outcome: str, prompt_tokens: int | None = None,
               completion_tokens: int = 0) -> None:
        if not self._conn or call_id is None:
            return
        with self._lock:
            if prompt_tokens is None:
                self._conn.execute("UPDATE calls SET ended_at=?, outcome=?, completion_tokens=? WHERE id=?",
                                   (time.time(), outcome, completion_tokens, call_id))
            else:
                self._conn.execute(
                    "UPDATE calls SET ended_at=?, outcome=?, prompt_tokens=?, completion_tokens=? WHERE id=?",
                    (time.time(), outcome, prompt_tokens, completion_tokens, call_id))

    def rows(self) -> list[dict]:
        if not self._conn:
            return []
        self._conn.row_factory = sqlite3.Row
        return [dict(r) for r in self._conn.execute("SELECT * FROM calls ORDER BY id")]
