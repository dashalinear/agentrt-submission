"""SQLite (WAL) durable state store: schema, connection and transaction helpers."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .states import ALLOWED_TRANSITIONS, SQL_TERMINAL

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS jobs (
    job_id            TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    idempotency_key   TEXT,
    job_class         TEXT NOT NULL DEFAULT 'short',
    priority          INTEGER NOT NULL DEFAULT 0,
    params            TEXT NOT NULL DEFAULT '{{}}',
    limits            TEXT NOT NULL DEFAULT '{{}}',
    created_at        REAL NOT NULL,
    deadline          REAL NOT NULL,
    state             TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 1,
    next_step         TEXT,
    step_seq          INTEGER NOT NULL DEFAULT 0,
    attempt           INTEGER NOT NULL DEFAULT 0,
    tool_calls        INTEGER NOT NULL DEFAULT 0,
    revisions         INTEGER NOT NULL DEFAULT 0,
    tokens_used       INTEGER NOT NULL DEFAULT 0,
    approval_id       TEXT,
    lease_owner       TEXT,
    lease_token       TEXT,
    lease_expires_at  REAL,
    run_after         REAL NOT NULL DEFAULT 0,
    ready_at          REAL,
    queue_wait_total  REAL NOT NULL DEFAULT 0,
    first_started_at  REAL,
    finished_at       REAL,
    updated_at        REAL NOT NULL,
    last_reason       TEXT,
    error             TEXT,
    result            TEXT
);
-- Idempotency of job creation: unique per tenant.
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_idem ON jobs(tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_jobs_state ON jobs(state, run_after);
CREATE INDEX IF NOT EXISTS ix_jobs_lease ON jobs(state, lease_expires_at);

CREATE TABLE IF NOT EXISTS allowed_transitions (
    src TEXT NOT NULL, dst TEXT NOT NULL, reason TEXT, PRIMARY KEY (src, dst)
);

-- Append-only transition log, written by triggers (cannot be forgotten by app code).
CREATE TABLE IF NOT EXISTS transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    src        TEXT,
    dst        TEXT NOT NULL,
    version    INTEGER NOT NULL,
    ts         REAL NOT NULL,
    reason     TEXT
);
CREATE INDEX IF NOT EXISTS ix_transitions_job ON transitions(job_id, id);

-- Terminal states are immutable: any UPDATE of a terminal row is aborted.
CREATE TRIGGER IF NOT EXISTS trg_terminal_immutable
BEFORE UPDATE ON jobs WHEN OLD.state IN {SQL_TERMINAL}
BEGIN SELECT RAISE(ABORT, 'terminal state is immutable'); END;

-- Only transitions from allowed_transitions are accepted.
CREATE TRIGGER IF NOT EXISTS trg_allowed_transition
BEFORE UPDATE OF state ON jobs
WHEN NEW.state <> OLD.state AND NOT EXISTS (
    SELECT 1 FROM allowed_transitions WHERE src = OLD.state AND dst = NEW.state)
BEGIN SELECT RAISE(ABORT, 'invalid state transition'); END;

-- Every state change must bump the row version by exactly one (CAS discipline).
CREATE TRIGGER IF NOT EXISTS trg_version_bump
BEFORE UPDATE OF state ON jobs
WHEN NEW.state <> OLD.state AND NEW.version <> OLD.version + 1
BEGIN SELECT RAISE(ABORT, 'state change without version bump'); END;

CREATE TRIGGER IF NOT EXISTS trg_log_transition
AFTER UPDATE OF state ON jobs WHEN NEW.state <> OLD.state
BEGIN
    INSERT INTO transitions(job_id, src, dst, version, ts, reason)
    VALUES (NEW.job_id, OLD.state, NEW.state, NEW.version, NEW.updated_at, NEW.last_reason);
END;

CREATE TRIGGER IF NOT EXISTS trg_log_create
AFTER INSERT ON jobs
BEGIN
    INSERT INTO transitions(job_id, src, dst, version, ts, reason)
    VALUES (NEW.job_id, NULL, NEW.state, NEW.version, NEW.created_at, 'created');
END;

-- Persistent step results (checkpoint). A step with a row here is never re-executed.
CREATE TABLE IF NOT EXISTS step_results (
    job_id     TEXT NOT NULL,
    step_key   TEXT NOT NULL,
    result     TEXT NOT NULL,
    attempt    INTEGER NOT NULL,
    tokens     INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    PRIMARY KEY (job_id, step_key)
);

-- Execution trace: one row per claimed step attempt; open rows (ended_at NULL) are
-- the execution slots used for global / per-tenant concurrency limits.
CREATE TABLE IF NOT EXISTS step_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    job_class   TEXT,
    step_key    TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    worker      TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    started_at  REAL NOT NULL,
    ended_at    REAL,
    outcome     TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS ix_step_runs_open ON step_runs(ended_at, tenant_id);
CREATE INDEX IF NOT EXISTS ix_step_runs_job ON step_runs(job_id);

-- Inbox of external events; event_id uniqueness gives deduplication.
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    job_id       TEXT NOT NULL,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    received_at  REAL NOT NULL,
    processed_at REAL,
    outcome      TEXT,
    detail       TEXT
);
-- Every receipt (including duplicates) for metrics.
CREATE TABLE IF NOT EXISTS event_receipts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    job_id      TEXT NOT NULL,
    type        TEXT,
    received_at REAL NOT NULL,
    committed_at REAL NOT NULL,
    outcome     TEXT NOT NULL,
    state_before TEXT,
    state_after  TEXT
);

-- Transactional outbox: written in the same transaction as the state change.
CREATE TABLE IF NOT EXISTS outbox (
    message_id   TEXT PRIMARY KEY,
    job_id       TEXT NOT NULL,
    topic        TEXT NOT NULL,
    payload      TEXT NOT NULL,
    created_at   REAL NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    delivered_at REAL,
    next_try_at  REAL NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox(delivered_at, next_try_at);

-- Durable timers (deadlines). They survive restarts; the timer loop fires due ones.
CREATE TABLE IF NOT EXISTS timers (
    timer_id  TEXT PRIMARY KEY,
    job_id    TEXT NOT NULL,
    kind      TEXT NOT NULL,
    fire_at   REAL NOT NULL,
    fired_at  REAL,
    outcome   TEXT
);
CREATE INDEX IF NOT EXISTS ix_timers_due ON timers(fired_at, fire_at);

-- Scheduler state (deficit round robin per tenant).
CREATE TABLE IF NOT EXISTS scheduler (
    tenant_id   TEXT PRIMARY KEY,
    weight      REAL NOT NULL DEFAULT 1.0,
    deficit     REAL NOT NULL DEFAULT 0,
    rr_order    INTEGER NOT NULL,
    served_work REAL NOT NULL DEFAULT 0,
    last_served REAL
);
CREATE TABLE IF NOT EXISTS scheduler_meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT, idempotency_key TEXT, job_class TEXT, reason TEXT, ts REAL
);

CREATE TABLE IF NOT EXISTS reports (
    job_id TEXT PRIMARY KEY, body TEXT NOT NULL, created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS compensations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL, action TEXT NOT NULL, detail TEXT, ts REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    incarnation TEXT PRIMARY KEY, name TEXT NOT NULL, pid INTEGER,
    started_at REAL NOT NULL, ready_at REAL
);

CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0);
"""


def connect(path: str) -> sqlite3.Connection:
    """Open a connection in autocommit mode; transactions are explicit (BEGIN IMMEDIATE)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=60, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL in WAL mode: committed transactions survive a *process* crash (SIGKILL);
    # the last transactions may be lost on OS crash / power loss. Set AGENTRT_SYNC=FULL
    # for durability against power loss.
    conn.execute(f"PRAGMA synchronous={os.environ.get('AGENTRT_SYNC', 'NORMAL')}")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str) -> sqlite3.Connection:
    conn = connect(path)
    conn.executescript(SCHEMA)
    with tx(conn):
        for (src, dst), reason in ALLOWED_TRANSITIONS.items():
            conn.execute("INSERT OR IGNORE INTO allowed_transitions(src,dst,reason) VALUES (?,?,?)",
                         (src, dst, reason))
    return conn


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Write transaction. BEGIN IMMEDIATE takes the write lock up-front, so all
    read-check-write sequences inside are serialized across processes."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def bump_counter(conn: sqlite3.Connection, key: str, n: int = 1) -> None:
    conn.execute("INSERT INTO counters(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value = value + excluded.value", (key, n))
