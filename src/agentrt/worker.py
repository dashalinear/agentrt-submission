"""Asyncio worker: claim (lease) -> execute ONE step -> checkpoint -> release.

Loops: scheduler (claims work at step boundaries), heartbeat (per running step),
timers (durable deadlines), reaper (expired leases), outbox relay.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid

from .agent import ReportAgent
from .config import Config
from .errors import LeaseLost, PermanentError, TransientError
from .logs import get_logger, log
from .scheduler import make_policy
from .store import Store

logger = get_logger("worker")


class Worker:
    def __init__(self, db_path: str, cfg: Config, name: str, llm, tool, publisher, notifier=None):
        self.cfg = cfg
        self.name = name
        self.incarnation = f"{name}:{uuid.uuid4().hex[:8]}"
        self.store = Store(db_path, cfg)
        self.policy = make_policy(cfg)
        self.agent = ReportAgent(self.store, cfg, llm, tool, publisher)
        self.publisher = publisher
        self.notifier = notifier if notifier is not None else publisher
        self.active: dict[str, asyncio.Task] = {}
        self._step_tasks: dict[str, asyncio.Task] = {}
        self._abort_reason: dict[str, str] = {}
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._paused_until = 0.0
        self.stats = {"claimed": 0, "completed": 0, "aborted": 0, "fenced": 0}
        self.ready_at: float | None = None

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    # ------------------------------------------------------------------ main
    async def run(self) -> None:
        self.store.register_worker(self.name, self.incarnation, os.getpid())
        own = self.store.recover_own(self.name, self.incarnation)
        expired = self.store.reap_expired()
        fired = self.store.fire_due_timers()
        self.store.worker_ready(self.incarnation)
        self.ready_at = time.time()
        log(logger, logging.INFO, "worker ready", worker=self.incarnation, recovered_own=own,
            recovered_expired=expired, timers_fired=fired)
        loops = [asyncio.create_task(x) for x in (self._scheduler_loop(), self._timer_loop(),
                                                    self._reaper_loop(), self._outbox_loop())]
        try:
            await self._stop.wait()
        finally:
            for t in loops:
                t.cancel()
            for t in list(self.active.values()):
                t.cancel()
            await asyncio.gather(*loops, *self.active.values(), return_exceptions=True)

    async def _sleep_or_wake(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        self._wake.clear()

    async def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            if time.time() >= self._paused_until:
                while len(self.active) < self.cfg.global_concurrency:
                    job = self.store.claim_next(self.incarnation, self.policy)
                    if job is None:
                        break
                    self.stats["claimed"] += 1
                    self.active[job["job_id"]] = asyncio.create_task(self._execute(job))
            await self._sleep_or_wake(self.cfg.poll_interval_s)

    async def _timer_loop(self) -> None:
        while not self._stop.is_set():
            for job_id, _ in self.store.fire_due_timers():
                self._abort_local(job_id, "deadline")
            await asyncio.sleep(self.cfg.poll_interval_s)

    async def _reaper_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.cfg.real(self.cfg.reaper_interval_s))
            if self.store.reap_expired():
                self.wake()

    async def _outbox_loop(self) -> None:
        while not self._stop.is_set():
            for msg in self.store.pending_outbox():
                try:
                    import json
                    await self.notifier.notify(msg["message_id"], msg["topic"], json.loads(msg["payload"]))
                    if self.cfg.faults.get("crash_after_outbox_send"):
                        os._exit(137)
                    self.store.outbox_delivered(msg["message_id"])
                except TransientError as e:
                    self.store.outbox_failed(msg["message_id"], str(e), msg["attempts"])
                except Exception as e:  # noqa: BLE001
                    self.store.outbox_failed(msg["message_id"], repr(e), msg["attempts"])
            await asyncio.sleep(max(self.cfg.poll_interval_s, 0.02))

    def _abort_local(self, job_id: str, reason: str) -> None:
        t = self._step_tasks.get(job_id)
        if t is not None and not t.done():
            self._abort_reason[job_id] = reason
            t.cancel()

    # ------------------------------------------------------------------ one step
    async def _heartbeat(self, job: dict) -> None:
        interval = self.cfg.real(self.cfg.heartbeat_interval_s)
        while True:
            await asyncio.sleep(interval)
            if not self.store.heartbeat(job["job_id"], job["lease_token"], job["_run_id"]):
                log(logger, logging.WARNING, "lease lost -> aborting step", job_id=job["job_id"],
                    step=job["next_step"], attempt=job["attempt"], worker=self.incarnation)
                self._abort_local(job["job_id"], "lease_lost")
                return

    async def _execute(self, job: dict) -> None:
        jid, step, attempt = job["job_id"], job["next_step"], job["attempt"]
        lim = self.store.limits_of(job)
        log(logger, logging.INFO, "step start", job_id=jid, step=step, attempt=attempt, worker=self.incarnation,
            tenant_id=job["tenant_id"])
        hb = asyncio.create_task(self._heartbeat(job))
        try:
            if job["step_seq"] >= lim.max_steps:
                raise PermanentError(f"max_steps exceeded ({lim.max_steps})")
            st = asyncio.create_task(self.agent.run_step(job))
            self._step_tasks[jid] = st
            try:
                sc = await asyncio.wait_for(st, timeout=self.cfg.real(lim.step_timeout_s))
            except asyncio.TimeoutError:
                raise TransientError(f"step timeout after {lim.step_timeout_s}s (virtual)")
            res = self.store.complete_step(job, sc)
            if res == "ok":
                self.stats["completed"] += 1
                log(logger, logging.INFO, "step done", job_id=jid, step=step, attempt=attempt, action=sc.action,
                    tokens=sc.tokens)
                pause = (self.cfg.faults.get("pause_after_step") or {}).get(step.split("#")[0])
                if pause:
                    self._paused_until = time.time() + self.cfg.real(pause)
            else:
                self.stats["fenced"] += 1
                log(logger, logging.WARNING, f"late step result discarded ({res})", job_id=jid, step=step,
                    attempt=attempt)
                await self._compensate_if_needed(job, sc)
        except asyncio.CancelledError:
            reason = self._abort_reason.pop(jid, None)
            if reason is None:
                raise  # worker shutdown: lease stays, recovered later
            self.stats["aborted"] += 1
            self.store.close_run(job["_run_id"], f"aborted_{reason}")
            log(logger, logging.WARNING, "step aborted", job_id=jid, step=step, attempt=attempt, reason=reason)
            if step.startswith("publish"):
                # outcome of the in-flight publish is unknown -> idempotent retract (noop if never published)
                await self._retract(jid, f"{jid}:publish")
        except LeaseLost as e:
            self.store.close_run(job["_run_id"], "aborted_lease_lost", str(e))
        except PermanentError as e:
            self.store.fail_step(job, f"{type(e).__name__}: {e}", transient=False)
            log(logger, logging.ERROR, "step permanent error", job_id=jid, step=step, attempt=attempt, error=str(e))
        except TransientError as e:
            r = self.store.fail_step(job, f"{type(e).__name__}: {e}", transient=True)
            log(logger, logging.WARNING, f"step transient error -> {r}", job_id=jid, step=step, attempt=attempt,
                error=str(e))
        except Exception as e:  # noqa: BLE001 - unknown errors are treated as transient (bounded by max_attempts)
            r = self.store.fail_step(job, f"{type(e).__name__}: {e}", transient=True)
            log(logger, logging.ERROR, f"step unexpected error -> {r}", job_id=jid, step=step, attempt=attempt,
                error=repr(e))
        finally:
            hb.cancel()
            self._step_tasks.pop(jid, None)
            self.active.pop(jid, None)
            self._wake.set()

    async def _compensate_if_needed(self, job: dict, sc) -> None:
        """A publish that succeeded externally while the job was cancelled / timed out is
        retracted (compensating action) so that no live publication belongs to a
        non-SUCCEEDED job."""
        if job["next_step"].startswith("publish") and sc is not None and sc.result.get("idempotency_key"):
            await self._retract(job["job_id"], sc.result["idempotency_key"])

    async def _retract(self, job_id: str, key: str) -> None:
        job = self.store.get_job(job_id)
        if job is None or job["state"] not in ("CANCELLED", "TIMED_OUT", "FAILED"):
            return  # job still alive (e.g. requeued after lease loss): another attempt will reuse the key
        try:
            done = await self.publisher.retract(key)
            self.store.record_compensation(job_id, "retract_publication", f"{key} retracted={done}")
            log(logger, logging.WARNING, "compensation: retract publication", job_id=job_id, step="publish#0",
                retracted=done)
        except Exception as e:  # noqa: BLE001
            log(logger, logging.ERROR, "compensation failed", job_id=job_id, error=repr(e))
