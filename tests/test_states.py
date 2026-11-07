"""Terminal states are immutable and only allowed transitions happen - enforced in SQL."""
import sqlite3

import pytest

from agentrt import states as S
from agentrt.store import Store


def _job(tmp_path):
    s = Store(str(tmp_path / "s.db"))
    j, _ = s.create_job("t")
    return s, j


def test_allowed_transitions_table_matches_code(tmp_path):
    s, _ = _job(tmp_path)
    db = {(r[0], r[1]) for r in s.conn.execute("SELECT src, dst FROM allowed_transitions")}
    assert db == set(S.ALLOWED_TRANSITIONS)
    assert not any(src in S.TERMINAL for src, _ in S.ALLOWED_TRANSITIONS)


@pytest.mark.parametrize("terminal", S.TERMINAL)
def test_terminal_row_cannot_be_updated_even_with_raw_sql(tmp_path, terminal):
    s, j = _job(tmp_path)
    # drive to the terminal state through an allowed path
    path = {"CANCELLED": ["CANCELLED"], "TIMED_OUT": ["TIMED_OUT"], "SUCCEEDED": ["RUNNING", "SUCCEEDED"],
            "FAILED": ["RUNNING", "FAILED"]}[terminal]
    for dst in path:
        s.conn.execute("UPDATE jobs SET state=?, version=version+1 WHERE job_id=?", (dst, j["job_id"]))
    for new in S.ALL_STATES:
        with pytest.raises(sqlite3.IntegrityError, match="terminal state is immutable|invalid state transition"):
            s.conn.execute("UPDATE jobs SET state=?, version=version+1 WHERE job_id=?", (new, j["job_id"]))
    with pytest.raises(sqlite3.IntegrityError):
        s.conn.execute("UPDATE jobs SET result='{}' WHERE job_id=?", (j["job_id"],))
    # and the API-level operations are no-ops
    assert s.cancel(j["job_id"])["outcome"] == "already_terminal"
    r = s.submit_event(j["job_id"], "late", "approval", {"approval_id": "x"})
    assert r["outcome"] == "ignored_terminal" and s.get_job(j["job_id"])["state"] == terminal


def test_invalid_transition_rejected_by_trigger(tmp_path):
    s, j = _job(tmp_path)
    with pytest.raises(sqlite3.IntegrityError, match="invalid state transition"):
        s.conn.execute("UPDATE jobs SET state='SUCCEEDED', version=version+1 WHERE job_id=?", (j["job_id"],))
    with pytest.raises(sqlite3.IntegrityError, match="invalid state transition"):
        s.conn.execute("UPDATE jobs SET state='WAITING_EVENT', version=version+1 WHERE job_id=?", (j["job_id"],))


def test_state_change_requires_version_bump(tmp_path):
    s, j = _job(tmp_path)
    with pytest.raises(sqlite3.IntegrityError, match="version bump"):
        s.conn.execute("UPDATE jobs SET state='RUNNING' WHERE job_id=?", (j["job_id"],))


def test_cas_with_stale_version_fails(tmp_path):
    from agentrt.db import tx

    s, j = _job(tmp_path)
    stale = dict(j)
    with tx(s.conn) as c:
        assert s._cas(c, j, S.RUNNING, "claim")
    with tx(s.conn) as c:
        assert not s._cas(c, stale, S.CANCELLED, "stale writer")  # version moved on
    assert s.get_job(j["job_id"])["state"] == S.RUNNING


def test_transition_log_written_by_trigger(tmp_path):
    s, j = _job(tmp_path)
    s.cancel(j["job_id"])
    log = [(t["src"], t["dst"], t["version"]) for t in s.transitions(j["job_id"])]
    assert log == [(None, "QUEUED", 1), ("QUEUED", "CANCELLED", 2)]
