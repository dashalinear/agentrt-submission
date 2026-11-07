"""Scheduling policies, applied at *step boundaries* inside the claim transaction.

* ``FifoPolicy``  – one global queue ordered by the time a work item became runnable
  (``ready_at``); ignores tenants and priorities.
* ``DrrPolicy``   – deficit round robin across tenants (weights, quantum measured in
  virtual seconds of estimated step work) + priority aging inside each tenant's queue.
  DRR state (deficits, round-robin pointer) lives in the ``scheduler`` table, so it is
  shared by all worker processes and survives restarts.
"""
from __future__ import annotations

import sqlite3

from .config import DEFAULT_SIM, Config


def step_cost(cfg: Config, job_class: str, step_key: str) -> float:
    """Estimated virtual duration of a step (used as DRR cost)."""
    table = {**DEFAULT_SIM.get(job_class, DEFAULT_SIM["short"]), **(cfg.sim.get(job_class, {}) or {})}
    name = step_key.split("#")[0]
    return float(table.get(name, 0.1))


class FifoPolicy:
    name = "fifo"

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def pick(self, conn: sqlite3.Connection, candidates: list[sqlite3.Row], backlogged: set[str],
             now: float):
        if not candidates:
            return None
        return min(candidates, key=lambda r: (r["ready_at"], r["created_at"], r["job_id"]))


class DrrPolicy:
    name = "drr"

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _eff_priority(self, r, now: float) -> float:
        aging = self.cfg.real(self.cfg.aging_interval_s) or 1.0
        return r["priority"] + max(0.0, now - r["ready_at"]) / aging

    def pick(self, conn: sqlite3.Connection, candidates: list[sqlite3.Row], backlogged: set[str],
             now: float):
        if not candidates:
            return None
        heads: dict[str, sqlite3.Row] = {}
        for r in candidates:
            h = heads.get(r["tenant_id"])
            if h is None or (self._eff_priority(r, now), -r["ready_at"]) > (self._eff_priority(h, now), -h["ready_at"]):
                heads[r["tenant_id"]] = r
        rows = {r["tenant_id"]: dict(r) for r in conn.execute("SELECT * FROM scheduler ORDER BY rr_order")}
        for t in sorted(set(heads) | backlogged):
            if t not in rows:
                order = (max((x["rr_order"] for x in rows.values()), default=-1) + 1)
                w = float(self.cfg.tenant_weights.get(t, 1.0))
                conn.execute("INSERT OR IGNORE INTO scheduler(tenant_id,weight,deficit,rr_order) VALUES(?,?,0,?)",
                             (t, w, order))
                rows[t] = {"tenant_id": t, "weight": w, "deficit": 0.0, "rr_order": order, "served_work": 0.0}
        order = [t for t, _ in sorted(rows.items(), key=lambda kv: kv[1]["rr_order"])]
        meta = {k: v for k, v in conn.execute("SELECT key, value FROM scheduler_meta")}
        ptr = int(meta.get("rr_ptr", 0)) % len(order)
        fresh = meta.get("rr_fresh", "0") == "1"
        q = self.cfg.drr_quantum_s
        chosen = None
        for _ in range(64 * len(order)):
            t = order[ptr]
            st = rows[t]
            if t not in backlogged:
                st["deficit"] = 0.0
                ptr, fresh = (ptr + 1) % len(order), False
                continue
            if t not in heads:  # backlogged but blocked by per-tenant limit: keep deficit
                ptr, fresh = (ptr + 1) % len(order), False
                continue
            if not fresh:
                st["deficit"] += q * st["weight"]
                fresh = True
            head = heads[t]
            cost = step_cost(self.cfg, head["job_class"], head["next_step"])
            if st["deficit"] >= cost:
                st["deficit"] -= cost
                st["served_work"] = st.get("served_work", 0.0) + cost
                chosen = head
                break
            ptr, fresh = (ptr + 1) % len(order), False
        for t, st in rows.items():
            conn.execute("UPDATE scheduler SET deficit=?, served_work=?, last_served=CASE WHEN ?=tenant_id THEN ? ELSE last_served END WHERE tenant_id=?",
                         (st["deficit"], st.get("served_work", 0.0), chosen["tenant_id"] if chosen else None, now, t))
        conn.execute("INSERT OR REPLACE INTO scheduler_meta(key,value) VALUES('rr_ptr',?)", (str(ptr),))
        conn.execute("INSERT OR REPLACE INTO scheduler_meta(key,value) VALUES('rr_fresh',?)", ("1" if fresh else "0",))
        return chosen


def make_policy(cfg: Config):
    return {"fifo": FifoPolicy, "drr": DrrPolicy}[cfg.policy](cfg)
