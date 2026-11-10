"""Diagnostic for the E5 harness fix: time from job creation to WAITING_EVENT with the worker in a thread of this
process, (a) alone and (b) with 20 threads polling the DB every 3 ms (the old E5 actor pattern).

  python -m experiments.diag_e5_polling --out results/raw            # Windows
  scripts/run_linux.ps1 -Parts diag                                   # Linux container -> results/raw_linux
Writes diag_e5_polling.json into --out.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics as st
import sys
import tempfile
import threading
import time

from agentrt.harness import ThreadWorker
from agentrt.store import Store
from experiments.common import RAW, environment, wait_for
from experiments.exp_events import TERMINAL, cfg_fast


def run(with_pollers: bool, n_jobs: int = 10, pollers_per_job: int = 2) -> dict:
    d = tempfile.mkdtemp()
    db = os.path.join(d, "s.db")
    cfg = cfg_fast()
    store = Store(db, cfg)
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        time.sleep(0.3)
        ids = [store.create_job("t", f"k{i}", limits={"deadline_s": 60})[0]["job_id"] for i in range(n_jobs)]
        t0 = time.time()
        stop = threading.Event()

        def poller(jid):
            s2 = Store(db, cfg, init=False)
            while not stop.is_set():
                s2.get_job(jid)
                time.sleep(0.003)

        ths = [threading.Thread(target=poller, args=(j,)) for j in ids for _ in range(pollers_per_job)] if with_pollers else []
        [t.start() for t in ths]
        times = []
        for jid in ids:
            wait_for(lambda: store.get_job(jid)["state"] in ("WAITING_EVENT",) + TERMINAL, timeout=120, interval=0.003)
            r = store.conn.execute("SELECT ts FROM transitions WHERE job_id=? AND dst='WAITING_EVENT'", (jid,)).fetchone()
            times.append(r[0] - t0)
        stop.set()
        [t.join() for t in ths]
    return {"pollers": len(ths), "n_jobs": n_jobs, "create_to_waiting_p50_s": round(st.median(times), 4),
            "create_to_waiting_max_s": round(max(times), 4)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=RAW)
    a = ap.parse_args(argv)
    sys.setswitchinterval(0.0005)  # same as exp_events
    res = {"env": environment(), "without_pollers": run(False), "with_20_pollers": run(True)}
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "diag_e5_polling.json"), "w", encoding="utf-8") as fh:
        json.dump(res, fh, indent=2)
    print(json.dumps({k: v for k, v in res.items() if k != "env"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
