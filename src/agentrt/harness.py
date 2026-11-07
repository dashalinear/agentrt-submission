"""Run a Worker in a background thread (own event loop). Used by tests and experiments
that need to drive events from the main thread with precise timing."""
from __future__ import annotations

import asyncio
import threading
import time

from .runtime import build_worker
from .states import ALLOWED_TRANSITIONS, TERMINAL


class ThreadWorker:
    def __init__(self, db: str, cfg, name: str = "w1", **kw):
        self.worker = build_worker(db, cfg, name, **kw)
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.error: BaseException | None = None

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.worker.run())
        except BaseException as e:  # noqa: BLE001
            self.error = e
        finally:
            self.loop.close()

    def start(self, timeout: float = 10.0) -> "ThreadWorker":
        self.thread.start()
        t0 = time.time()
        while self.worker.ready_at is None:
            if time.time() - t0 > timeout or self.error:
                raise RuntimeError(f"worker did not start: {self.error!r}")
            time.sleep(0.002)
        return self

    def stop(self) -> None:
        if self.loop and self.thread.is_alive():
            self.loop.call_soon_threadsafe(self.worker.stop)
            self.thread.join(15)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def check_invariants(conn) -> dict:
    """Verify transition-log invariants. Returns violation counts (all must be 0)."""
    rows = conn.execute("SELECT job_id, src, dst, version FROM transitions ORDER BY job_id, id").fetchall()
    invalid = terminal_changes = version_gaps = 0
    last: dict[str, tuple] = {}
    terminal_count: dict[str, int] = {}
    for job_id, src, dst, ver in rows:
        if src is not None and (src, dst) not in ALLOWED_TRANSITIONS:
            invalid += 1
        if src in TERMINAL:
            terminal_changes += 1
        prev = last.get(job_id)
        if prev is not None and ver != prev[1] + 1:
            version_gaps += 1
        if prev is not None and prev[0] in TERMINAL:
            terminal_changes += 1
        last[job_id] = (dst, ver)
        if dst in TERMINAL:
            terminal_count[job_id] = terminal_count.get(job_id, 0) + 1
    multi_terminal = sum(1 for v in terminal_count.values() if v > 1)
    state_mismatch = 0
    for job_id, state in conn.execute("SELECT job_id, state FROM jobs"):
        if job_id in last and last[job_id][0] != state:
            state_mismatch += 1
    return {"transitions_checked": len(rows), "invalid_transitions": invalid, "terminal_changes": terminal_changes,
            "version_gaps": version_gaps, "multiple_terminal": multi_terminal, "log_state_mismatch": state_mismatch}
