"""Job store: every state change is a compare-and-set (CAS) on the row ``version``
inside a ``BEGIN IMMEDIATE`` transaction, together with its side records (step
result, outbox message, timers, step_runs trace).  See README "Race resolution".
"""
from __future__ import annotations

import json
import logging
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass

from . import states as S
from .config import Config, JobLimits
from .db import bump_counter, connect, init_db, tx
from .logs import get_logger, log

logger = get_logger("store")


class Rejected(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _j(x) -> str:
    return json.dumps(x, ensure_ascii=False, sort_keys=True)


def row_to_job(r: sqlite3.Row | None) -> dict | None:
    if r is None:
        return None
    d = dict(r)
    d["params"] = json.loads(d["params"] or "{}")
    d["limits"] = json.loads(d["limits"] or "{}")
    d["result"] = json.loads(d["result"]) if d.get("result") else None
    return d


@dataclass
class StepCompletion:
    result: dict
    tokens: int = 0
    action: str = "continue"          # continue | wait_approval | succeed | fail
    next_step: str | None = None
    job_updates: dict | None = None   # extra columns (tool_calls, revisions, ...)
    side_writes: list | None = None   # [(sql, params)] executed in the same transaction
    fail_reason: str | None = None


class Store:
    def __init__(self, path: str, cfg: Config | None = None, init: bool = True):
        self.path = path
        self.cfg = cfg or Config()
        self.conn = init_db(path) if init else connect(path)

    def now(self) -> float:
        return time.time()

    # ------------------------------------------------------------------ helpers
    def get_job(self, job_id: str) -> dict | None:
        return row_to_job(self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())

    def limits_of(self, job: dict) -> JobLimits:
        return JobLimits.from_dict(job["limits"])

    def _outbox(self, conn, job_id: str, topic: str, payload: dict, version: int) -> None:
        conn.execute("INSERT OR IGNORE INTO outbox(message_id,job_id,topic,payload,created_at) VALUES(?,?,?,?,?)",
                     (f"{job_id}:{topic}:v{version}", job_id, topic, _j(payload), self.now()))

    def _cas(self, conn, job: dict, dst: str, reason: str, **sets) -> bool:
        """Compare-and-set transition. Succeeds only if the row still has the version we
        read and is not terminal (the trigger additionally forbids touching terminal rows
        and invalid (src,dst) pairs)."""
        now = sets.pop("_now", None) or self.now()
        if dst in S.TERMINAL:
            sets.setdefault("finished_at", now)
            sets.setdefault("lease_owner", None)
            sets.setdefault("lease_token", None)
            sets.setdefault("lease_expires_at", None)
        cols = ", ".join(f"{k}=?" for k in sets)
        sql = (f"UPDATE jobs SET state=?, version=version+1, updated_at=?, last_reason=?"
               f"{', ' + cols if cols else ''} WHERE job_id=? AND version=? AND state NOT IN {S.SQL_TERMINAL}")
        try:
            cur = conn.execute(sql, (dst, now, reason, *sets.values(), job["job_id"], job["version"]))
        except sqlite3.IntegrityError as e:  # trigger RAISE(ABORT)
            bump_counter(conn, "invalid_transition_attempts")
            log(logger, logging.WARNING, "transition rejected by DB trigger", job_id=job["job_id"],
                src=job["state"], dst=dst, error=str(e))
            return False
        ok = cur.rowcount == 1
        if ok:
            if dst in S.TERMINAL:
                self._outbox(conn, job["job_id"], f"job.{dst.lower()}", {"job_id": job["job_id"], "state": dst,
                                                                       "reason": reason}, job["version"] + 1)
            log(logger, logging.INFO, "transition", job_id=job["job_id"], step=job.get("next_step"),
                attempt=job.get("attempt"), src=job["state"], dst=dst, reason=reason, state=dst)
        else:
            bump_counter(conn, "cas_conflicts")
        return ok

    # ------------------------------------------------------------------ create
    def create_job(self, tenant_id: str, idempotency_key: str | None = None, job_class: str = "short",
                   priority: int = 0, params: dict | None = None, limits: dict | None = None,
                   job_id: str | None = None) -> tuple[dict, bool]:
        """Returns (job, created). Raises Rejected('queue_full'|...) on admission control."""
        lim = JobLimits.from_dict(limits)
        params = {"collection_id": "default", "require_approval": True, "min_docs": 0, **(params or {})}
        now = self.now()
        rejected = None
        with tx(self.conn) as c:
            if idempotency_key:
                r = c.execute("SELECT * FROM jobs WHERE tenant_id=? AND idempotency_key=?",
                              (tenant_id, idempotency_key)).fetchone()
                if r is not None:
                    return row_to_job(r), False
            active = c.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('QUEUED','RUNNING')").fetchone()[0]
            if active >= self.cfg.max_queue_size:
                rejected = "queue_full"
            elif self.cfg.max_queue_per_tenant is not None:
                t_active = c.execute("SELECT COUNT(*) FROM jobs WHERE tenant_id=? AND state IN ('QUEUED','RUNNING')",
                                     (tenant_id,)).fetchone()[0]
                if t_active >= self.cfg.max_queue_per_tenant:
                    rejected = "tenant_queue_full"
            if rejected:
                c.execute("INSERT INTO rejections(tenant_id,idempotency_key,job_class,reason,ts) VALUES(?,?,?,?,?)",
                          (tenant_id, idempotency_key, job_class, rejected, now))
                bump_counter(c, "rejected_" + rejected)
            else:
                job_id = job_id or "job-" + uuid.uuid4().hex[:16]
                deadline = now + self.cfg.real(lim.deadline_s)
                c.execute(
                    "INSERT INTO jobs(job_id,tenant_id,idempotency_key,job_class,priority,params,limits,created_at,"
                    "deadline,state,version,next_step,ready_at,run_after,updated_at,last_reason)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?)",
                    (job_id, tenant_id, idempotency_key, job_class, priority, _j(params), _j(lim.__dict__), now,
                     deadline, S.QUEUED, "fetch_documents#0", now, now, now, "created"))
                c.execute("INSERT INTO timers(timer_id,job_id,kind,fire_at) VALUES(?,?,?,?)",
                          (f"{job_id}:deadline", job_id, "deadline", deadline))
        if rejected:
            log(logger, logging.WARNING, "job rejected", tenant_id=tenant_id, reason=rejected)
            raise Rejected(rejected)
        log(logger, logging.INFO, "job created", job_id=job_id, tenant_id=tenant_id, job_class=job_class)
        return self.get_job(job_id), True

    # ------------------------------------------------------------------ cancel
    def cancel(self, job_id: str, reason: str = "cancel requested") -> dict:
        with tx(self.conn) as c:
            job = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())
            if job is None:
                return {"outcome": "not_found"}
            if job["state"] in S.TERMINAL:
                bump_counter(c, "cancel_after_terminal")
                return {"outcome": "already_terminal", "state": job["state"]}
            ok = self._cas(c, job, S.CANCELLED, reason)
            return {"outcome": "cancelled" if ok else "conflict", "state": S.CANCELLED if ok else job["state"],
                    "was": job["state"], "ts": self.now()}

    # ------------------------------------------------------------------ events
    def submit_event(self, job_id: str, event_id: str, etype: str, payload: dict | None = None) -> dict:
        """Inbox + transition in ONE transaction: an event changes state at most once
        (dedup by event_id primary key)."""
        payload = payload or {}
        received = self.now()
        with tx(self.conn) as c:
            cur = c.execute("INSERT OR IGNORE INTO events(event_id,job_id,type,payload,received_at) VALUES(?,?,?,?,?)",
                            (event_id, job_id, etype, _j(payload), received))
            job = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())
            before = job["state"] if job else None
            if cur.rowcount == 0:
                outcome = "duplicate"
                bump_counter(c, "events_duplicate")
            elif job is None:
                outcome = "unknown_job"
            else:
                outcome = self._apply_event(c, job, event_id, etype, payload)
            after_row = c.execute("SELECT state FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            after = after_row[0] if after_row else None
            committed = self.now()
            if outcome != "duplicate":
                c.execute("UPDATE events SET processed_at=?, outcome=? WHERE event_id=?", (committed, outcome, event_id))
            c.execute("INSERT INTO event_receipts(event_id,job_id,type,received_at,committed_at,outcome,state_before,state_after)"
                      " VALUES(?,?,?,?,?,?,?,?)", (event_id, job_id, etype, received, committed, outcome, before, after))
        lvl = logging.INFO if outcome in ("applied", "cancelled") else logging.WARNING
        log(logger, lvl, f"event {outcome}", job_id=job_id, event_id=event_id, etype=etype, state=after)
        return {"outcome": outcome, "state_before": before, "state": after, "received_at": received,
                "committed_at": committed}

    def _apply_event(self, c, job: dict, event_id: str, etype: str, payload: dict) -> str:
        if job["state"] in S.TERMINAL:
            bump_counter(c, "events_late")
            return "ignored_terminal"
        if etype == "cancel":
            return "cancelled" if self._cas(c, job, S.CANCELLED, f"cancel event {event_id}") else "conflict"
        if etype != "approval":
            return "ignored_unknown_type"
        if job["state"] != S.WAITING_EVENT:
            bump_counter(c, "events_wrong_state")
            return "ignored_state"
        if payload.get("approval_id") != job["approval_id"]:
            bump_counter(c, "events_stale_approval")
            return "ignored_stale_approval"
        now = self.now()
        if now >= job["deadline"]:  # deadline beats approval
            self._cas(c, job, S.TIMED_OUT, "deadline passed before approval was applied")
            return "deadline_won"
        lim = self.limits_of(job)
        decision = payload.get("decision", "approve")
        if decision == "approve":
            ok = self._cas(c, job, S.QUEUED, f"approved ({event_id})", next_step="save_report#0", attempt=0,
                           ready_at=now, run_after=now, _now=now)
            return "applied" if ok else "conflict"
        if decision == "reject":
            if job["revisions"] < lim.max_revisions:
                rnd = job["revisions"] + job["tool_calls"] + 1
                c.execute("INSERT OR IGNORE INTO step_results(job_id,step_key,result,attempt,created_at) VALUES(?,?,?,?,?)",
                          (job["job_id"], f"feedback#{rnd}", _j({"comment": payload.get("comment", "")}), 1, now))
                ok = self._cas(c, job, S.QUEUED, f"rejected -> revision ({event_id})", next_step=f"draft#{rnd}.0",
                               attempt=0, revisions=job["revisions"] + 1, ready_at=now, run_after=now, _now=now)
            else:
                ok = self._cas(c, job, S.FAILED, f"rejected by approver ({event_id})",
                               error="rejected_by_approver", _now=now)
            return "applied" if ok else "conflict"
        return "ignored_bad_payload"

    # ------------------------------------------------------------------ worker side
    def register_worker(self, name: str, incarnation: str, pid: int) -> None:
        with tx(self.conn) as c:
            c.execute("INSERT INTO workers(incarnation,name,pid,started_at) VALUES(?,?,?,?)",
                      (incarnation, name, pid, self.now()))

    def worker_ready(self, incarnation: str) -> None:
        with tx(self.conn) as c:
            c.execute("UPDATE workers SET ready_at=? WHERE incarnation=?", (self.now(), incarnation))

    def claim_next(self, incarnation: str, policy) -> dict | None:
        cfg = self.cfg
        with tx(self.conn) as c:
            now = self.now()
            running = {r[0]: r[1] for r in c.execute(
                "SELECT tenant_id, COUNT(*) FROM step_runs WHERE ended_at IS NULL GROUP BY tenant_id")}
            if sum(running.values()) >= cfg.global_concurrency:
                return None
            cands = c.execute("SELECT job_id, tenant_id, priority, ready_at, run_after, created_at, job_class, "
                              "next_step, version FROM jobs WHERE state='QUEUED' AND run_after<=? AND deadline>?",
                              (now, now)).fetchall()
            if not cands:
                return None
            backlogged = {r["tenant_id"] for r in cands}
            eligible = [r for r in cands if running.get(r["tenant_id"], 0) < cfg.per_tenant_concurrency]
            pick = policy.pick(c, eligible, backlogged, now)
            if pick is None:
                return None
            job = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (pick["job_id"],)).fetchone())
            token = uuid.uuid4().hex
            lease_until = now + cfg.real(cfg.lease_timeout_s)
            waited = max(0.0, now - max(job["ready_at"] or now, job["run_after"] or 0))
            ok = self._cas(c, job, S.RUNNING, "claimed", lease_owner=incarnation, lease_token=token,
                           lease_expires_at=lease_until, attempt=job["attempt"] + 1,
                           queue_wait_total=job["queue_wait_total"] + waited,
                           first_started_at=job["first_started_at"] or now, _now=now)
            if not ok:
                return None
            cur = c.execute("INSERT INTO step_runs(job_id,tenant_id,job_class,step_key,attempt,worker,lease_token,started_at)"
                            " VALUES(?,?,?,?,?,?,?,?)", (job["job_id"], job["tenant_id"], job["job_class"],
                                                         job["next_step"], job["attempt"] + 1, incarnation, token, now))
            run_id = cur.lastrowid
        job = self.get_job(pick["job_id"])
        job["_run_id"] = run_id
        job["_waited"] = waited
        return job

    def heartbeat(self, job_id: str, token: str, run_id: int) -> bool:
        with tx(self.conn) as c:
            until = self.now() + self.cfg.real(self.cfg.lease_timeout_s)
            cur = c.execute("UPDATE jobs SET lease_expires_at=? WHERE job_id=? AND lease_token=? AND state='RUNNING'",
                            (until, job_id, token))
            return cur.rowcount == 1

    def close_run(self, run_id: int, outcome: str, detail: str | None = None, conn=None) -> None:
        sql = "UPDATE step_runs SET ended_at=?, outcome=?, detail=? WHERE id=? AND ended_at IS NULL"
        if conn is not None:
            conn.execute(sql, (self.now(), outcome, detail, run_id))
        else:
            with tx(self.conn) as c:
                c.execute(sql, (self.now(), outcome, detail, run_id))

    def is_owner(self, job_id: str, token: str) -> bool:
        r = self.conn.execute("SELECT 1 FROM jobs WHERE job_id=? AND lease_token=? AND state='RUNNING'",
                              (job_id, token)).fetchone()
        return r is not None

    def step_result(self, job_id: str, step_key: str) -> dict | None:
        r = self.conn.execute("SELECT result FROM step_results WHERE job_id=? AND step_key=?", (job_id, step_key)).fetchone()
        return json.loads(r[0]) if r else None

    def step_results(self, job_id: str, prefix: str = "") -> dict[str, dict]:
        return {r[0]: json.loads(r[1]) for r in self.conn.execute(
            "SELECT step_key, result FROM step_results WHERE job_id=? AND step_key LIKE ? ORDER BY created_at",
            (job_id, prefix + "%"))}

    def complete_step(self, job: dict, sc: StepCompletion) -> str:
        """Checkpoint + transition in one transaction, fenced by lease_token.
        Returns 'ok' | 'fenced' (late result discarded) | 'deadline'."""
        step_key, token, run_id = job["next_step"], job["lease_token"], job["_run_id"]
        with tx(self.conn) as c:
            cur = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone())
            if cur is None or cur["state"] != S.RUNNING or cur["lease_token"] != token:
                self.close_run(run_id, "discarded_late_result", f"state={cur and cur['state']}", conn=c)
                bump_counter(c, "late_results_discarded")
                return "fenced"
            now = self.now()
            if now >= cur["deadline"]:  # deadline beats step completion
                self.close_run(run_id, "deadline", None, conn=c)
                self._cas(c, cur, S.TIMED_OUT, f"deadline passed during step {step_key}", _now=now)
                return "deadline"
            c.execute("INSERT OR IGNORE INTO step_results(job_id,step_key,result,attempt,tokens,created_at) VALUES(?,?,?,?,?,?)",
                      (cur["job_id"], step_key, _j(sc.result), cur["attempt"], sc.tokens, now))
            for sql, params in (sc.side_writes or []):
                c.execute(sql, params)
            upd = dict(sc.job_updates or {})
            upd.update(tokens_used=cur["tokens_used"] + sc.tokens, step_seq=cur["step_seq"] + 1, attempt=0, error=None,
                       lease_owner=None, lease_token=None, lease_expires_at=None, _now=now)
            if sc.action == "continue":
                ok = self._cas(c, cur, S.QUEUED, f"step {step_key} done", next_step=sc.next_step,
                               ready_at=now, run_after=now, **upd)
            elif sc.action == "wait_approval":
                approval_id = upd.pop("approval_id")
                ok = self._cas(c, cur, S.WAITING_EVENT, f"approval requested after {step_key}",
                               next_step=None, approval_id=approval_id, **upd)
                if ok:
                    self._outbox(c, cur["job_id"], "approval.requested",
                                 {"job_id": cur["job_id"], "approval_id": approval_id}, cur["version"] + 1)
            elif sc.action == "succeed":
                ok = self._cas(c, cur, S.SUCCEEDED, f"step {step_key} done", result=_j(sc.result), next_step=None, **upd)
            else:
                upd["error"] = sc.fail_reason
                ok = self._cas(c, cur, S.FAILED, sc.fail_reason or "agent failed", **upd)
            if not ok:
                raise RuntimeError("CAS failed while holding the write lock (should not happen)")
            self.close_run(run_id, "ok", None, conn=c)
        return "ok"

    def backoff(self, attempt: int, rnd: random.Random | None = None) -> float:
        """Equal-jitter exponential backoff in virtual seconds."""
        r = rnd or random
        d = min(self.cfg.backoff_cap_s, self.cfg.backoff_base_s * (2 ** max(0, attempt - 1)))
        return d / 2 + r.uniform(0, d / 2)

    def fail_step(self, job: dict, error: str, transient: bool, run_outcome: str | None = None) -> str:
        step_key, token, run_id = job["next_step"], job["lease_token"], job["_run_id"]
        lim = self.limits_of(job)
        with tx(self.conn) as c:
            cur = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone())
            if cur is None or cur["state"] != S.RUNNING or cur["lease_token"] != token:
                self.close_run(run_id, "discarded_late_error", error, conn=c)
                return "fenced"
            now = self.now()
            self.close_run(run_id, run_outcome or ("transient_error" if transient else "permanent_error"), error, conn=c)
            clear = dict(lease_owner=None, lease_token=None, lease_expires_at=None, _now=now)
            if transient and cur["attempt"] < lim.max_attempts:
                delay = self.cfg.real(self.backoff(cur["attempt"]))
                self._cas(c, cur, S.QUEUED, f"transient error at {step_key} (attempt {cur['attempt']}): {error}; "
                                            f"retry in {delay:.3f}s", run_after=now + delay, ready_at=now + delay,
                          error=error, **clear)
                return "retry"
            why = (f"step {step_key} failed after {cur['attempt']} attempts (max_attempts={lim.max_attempts}); "
                   f"last error: {error}") if transient else f"permanent error at step {step_key}: {error}"
            self._cas(c, cur, S.FAILED, why, error=why, **clear)
            return "failed"

    # ------------------------------------------------------------------ recovery
    def recover_own(self, name: str, incarnation: str) -> list[str]:
        """At startup: leases held by a previous incarnation of the SAME worker name are
        known dead -> requeue immediately (no need to wait for lease expiry)."""
        with tx(self.conn) as c:
            runs = c.execute("SELECT id, job_id, lease_token FROM step_runs WHERE ended_at IS NULL AND worker LIKE ? "
                             "AND worker <> ?", (name + ":%", incarnation)).fetchall()
            return self._requeue_runs(c, runs, "owner_restarted")

    def reap_expired(self) -> list[str]:
        """Any worker: requeue RUNNING jobs whose lease expired (visibility timeout)."""
        with tx(self.conn) as c:
            now = self.now()
            runs = c.execute("SELECT sr.id, sr.job_id, sr.lease_token FROM step_runs sr LEFT JOIN jobs j "
                             "ON j.job_id = sr.job_id AND j.lease_token = sr.lease_token "
                             "WHERE sr.ended_at IS NULL AND (j.job_id IS NULL OR j.lease_expires_at < ?)",
                             (now,)).fetchall()
            # j.job_id IS NULL: job already left RUNNING (cancel/timeout) and the worker is gone
            stale = [r for r in runs if self._run_lease_dead(c, r, now)]
            return self._requeue_runs(c, stale, "lease_expired")

    def _run_lease_dead(self, c, run, now: float) -> bool:
        j = c.execute("SELECT lease_token, lease_expires_at, state, updated_at FROM jobs WHERE job_id=?", (run["job_id"],)).fetchone()
        if j is not None and j["lease_token"] == run["lease_token"]:
            return j["lease_expires_at"] is not None and j["lease_expires_at"] < now
        # job no longer leased by this run: the live worker would have closed the run on its
        # next heartbeat; give it one lease period after the job's last change.
        return j is None or (now - j["updated_at"]) > self.cfg.real(self.cfg.lease_timeout_s)

    def _requeue_runs(self, c, runs, why: str) -> list[str]:
        requeued = []
        for run in runs:
            self.close_run(run["id"], why, None, conn=c)
            job = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (run["job_id"],)).fetchone())
            if job is None or job["state"] != S.RUNNING or job["lease_token"] != run["lease_token"]:
                continue
            lim = self.limits_of(job)
            now = self.now()
            clear = dict(lease_owner=None, lease_token=None, lease_expires_at=None, _now=now)
            bump_counter(c, "recovered_" + why)
            if job["attempt"] >= lim.max_attempts:
                self._cas(c, job, S.FAILED, f"{why}: step {job['next_step']} lost {job['attempt']} times "
                                           f"(max_attempts={lim.max_attempts})", error=why, **clear)
            else:
                self._cas(c, job, S.QUEUED, f"recovery: {why} at step {job['next_step']}",
                          ready_at=now, run_after=now, **clear)
            requeued.append(job["job_id"])
            log(logger, logging.WARNING, "recovered job", job_id=job["job_id"], step=job["next_step"],
                attempt=job["attempt"], why=why)
        return requeued

    # ------------------------------------------------------------------ timers
    def fire_due_timers(self) -> list[tuple[str, str]]:
        fired = []
        with tx(self.conn) as c:
            now = self.now()
            due = c.execute("SELECT * FROM timers WHERE fired_at IS NULL AND fire_at<=? ORDER BY fire_at LIMIT 500",
                            (now,)).fetchall()
            for t in due:
                job = row_to_job(c.execute("SELECT * FROM jobs WHERE job_id=?", (t["job_id"],)).fetchone())
                outcome = "obsolete"
                if job is not None and job["state"] not in S.TERMINAL:
                    ok = self._cas(c, job, S.TIMED_OUT, f"deadline expired in state {job['state']}", _now=now)
                    outcome = f"timed_out_from_{job['state']}" if ok else "conflict"
                    if ok:
                        fired.append((job["job_id"], job["state"]))
                c.execute("UPDATE timers SET fired_at=?, outcome=? WHERE timer_id=?", (now, outcome, t["timer_id"]))
        return fired

    # ------------------------------------------------------------------ outbox
    def pending_outbox(self, limit: int = 50) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM outbox WHERE delivered_at IS NULL AND next_try_at<=? ORDER BY created_at LIMIT ?",
            (self.now(), limit))]

    def outbox_delivered(self, message_id: str) -> None:
        with tx(self.conn) as c:
            c.execute("UPDATE outbox SET delivered_at=?, attempts=attempts+1 WHERE message_id=?", (self.now(), message_id))

    def outbox_failed(self, message_id: str, error: str, attempts: int) -> None:
        delay = self.cfg.real(min(self.cfg.backoff_cap_s, self.cfg.backoff_base_s * 2 ** attempts))
        with tx(self.conn) as c:
            c.execute("UPDATE outbox SET attempts=attempts+1, last_error=?, next_try_at=? WHERE message_id=?",
                      (error, self.now() + delay, message_id))

    def record_compensation(self, job_id: str, action: str, detail: str) -> None:
        with tx(self.conn) as c:
            c.execute("INSERT INTO compensations(job_id,action,detail,ts) VALUES(?,?,?,?)",
                      (job_id, action, detail, self.now()))

    # ------------------------------------------------------------------ queries
    def counts(self) -> dict:
        out = {s: 0 for s in S.ALL_STATES}
        for st, n in self.conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state"):
            out[st] = n
        out["rejected"] = self.conn.execute("SELECT COUNT(*) FROM rejections").fetchone()[0]
        out["counters"] = {k: v for k, v in self.conn.execute("SELECT key, value FROM counters")}
        return out

    def transitions(self, job_id: str | None = None) -> list[dict]:
        if job_id:
            q = self.conn.execute("SELECT * FROM transitions WHERE job_id=? ORDER BY id", (job_id,))
        else:
            q = self.conn.execute("SELECT * FROM transitions ORDER BY id")
        return [dict(r) for r in q]

    def close(self) -> None:
        self.conn.close()
