"""Job states and the table of allowed transitions.

The same table is loaded into SQLite (``allowed_transitions``) and enforced by a
trigger, so an invalid transition is rejected by the database itself, not only by
application code.
"""
from __future__ import annotations

QUEUED = "QUEUED"
RUNNING = "RUNNING"
WAITING_EVENT = "WAITING_EVENT"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
TIMED_OUT = "TIMED_OUT"

ALL_STATES = (QUEUED, RUNNING, WAITING_EVENT, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT)
TERMINAL = (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT)
ACTIVE = (QUEUED, RUNNING)  # counted by admission control (max_queue_size)

# (from, to): reason
ALLOWED_TRANSITIONS: dict[tuple[str, str], str] = {
    (QUEUED, RUNNING): "worker claimed job (lease acquired)",
    (QUEUED, CANCELLED): "cancel while queued",
    (QUEUED, TIMED_OUT): "deadline expired while queued",
    (RUNNING, QUEUED): "step finished -> yield at step boundary / transient error -> backoff / lease expired -> recovery",
    (RUNNING, WAITING_EVENT): "agent requested approval",
    (RUNNING, SUCCEEDED): "last step (publish) committed",
    (RUNNING, FAILED): "permanent error / attempts or budget exhausted",
    (RUNNING, CANCELLED): "cancel while running",
    (RUNNING, TIMED_OUT): "deadline expired while running",
    (WAITING_EVENT, QUEUED): "approval/rejection event accepted",
    (WAITING_EVENT, FAILED): "rejected and no revisions left",
    (WAITING_EVENT, CANCELLED): "cancel while waiting",
    (WAITING_EVENT, TIMED_OUT): "deadline expired while waiting",
}

SQL_TERMINAL = "('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')"


def is_allowed(src: str, dst: str) -> bool:
    return (src, dst) in ALLOWED_TRANSITIONS


def is_terminal(state: str) -> bool:
    return state in TERMINAL
