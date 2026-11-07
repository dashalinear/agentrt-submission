"""Shared test helpers."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time

from agentrt.config import Config
from agentrt.runtime import build_worker

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")


def fast_cfg(**kw) -> Config:
    base = dict(time_scale=0.02, poll_interval_s=0.002, lease_timeout_s=10.0, heartbeat_interval_s=0.5,
                reaper_interval_s=1.0, backoff_base_s=0.5, backoff_cap_s=2.0, global_concurrency=4,
                per_tenant_concurrency=4)
    base.update(kw)
    return Config(**base)


async def run_until(worker, cond, timeout: float = 15.0, interval: float = 0.005):
    """Run the worker until cond() is truthy (or timeout). Returns cond()'s value."""
    task = asyncio.create_task(worker.run())
    t0 = time.time()
    try:
        while time.time() - t0 < timeout:
            v = cond()
            if v:
                return v
            if task.done():
                task.result()
            await asyncio.sleep(interval)
        raise TimeoutError("condition not reached")
    finally:
        worker.stop()
        await asyncio.gather(task, return_exceptions=True)


def make_worker(db, cfg, name="w1", **kw):
    kw.setdefault("publisher_db", db + ".pub.db")
    return build_worker(db, cfg, name, **kw)


def spawn_worker(db, cfg_path, name, ledger=None, publisher_url=None, publisher_db=None, log_file=None):
    env = dict(os.environ, PYTHONPATH=SRC + os.pathsep + os.environ.get("PYTHONPATH", ""))
    cmd = [sys.executable, "-m", "agentrt.cli", "worker", "--db", db, "--name", name, "--config", cfg_path]
    if ledger:
        cmd += ["--ledger", ledger]
    if publisher_url:
        cmd += ["--publisher-url", publisher_url]
    if publisher_db:
        cmd += ["--publisher-db", publisher_db]
    if log_file:
        cmd += ["--log-file", log_file]
    return subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_for(cond, timeout=20.0, interval=0.01):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = cond()
        if v:
            return v
        time.sleep(interval)
    raise TimeoutError("condition not reached")
