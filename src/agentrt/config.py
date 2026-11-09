"""Runtime configuration.

All *durations* in this module are expressed in **virtual seconds**. The runtime
multiplies them by ``time_scale`` to obtain real (wall-clock) seconds.  With
``time_scale = 1.0`` virtual == real; the load test uses ``time_scale = 0.1``
(1 virtual second == 100 real milliseconds).
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field, fields
from typing import Any


@dataclass
class JobLimits:
    """Per-job limits. Documented defaults (see README, section "Job")."""

    max_steps: int = 30            # max executed steps (incl. tool loops / revisions)
    max_tokens: int = 20_000       # token budget for all LLM calls of the job
    max_attempts: int = 4          # attempts per step (crash/lease loss counts as an attempt)
    step_timeout_s: float = 30.0   # virtual seconds per step attempt
    deadline_s: float = 600.0      # job deadline relative to created_at (virtual seconds)
    max_tool_calls: int = 3        # extra fetch_documents calls the agent policy may request
    max_revisions: int = 1         # how many times a rejected draft may be revised

    @classmethod
    def from_dict(cls, d: dict | None) -> "JobLimits":
        d = d or {}
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})


@dataclass
class Config:
    time_scale: float = 1.0

    # --- workload control -------------------------------------------------
    global_concurrency: int = 4
    per_tenant_concurrency: int = 2
    worker_concurrency: int | None = None  # local slots of ONE worker process (default: global)
    max_queue_size: int = 1000           # max admitted QUEUED+RUNNING jobs (admission control)
    max_queue_per_tenant: int | None = None

    # --- scheduling ---------------------------------------------------------
    policy: str = "drr"                  # "fifo" | "drr"
    tenant_weights: dict[str, float] = field(default_factory=dict)  # default weight 1.0
    drr_quantum_s: float = 1.0           # virtual seconds of step-work added per DRR round
    aging_interval_s: float = 10.0       # +1 effective priority per this many virtual s of waiting

    # --- leases / recovery --------------------------------------------------
    lease_timeout_s: float = 3.0         # virtual seconds
    heartbeat_interval_s: float = 0.25   # virtual seconds (also bounds cancel-stop latency)
    poll_interval_s: float = 0.02        # REAL seconds: scheduler/timer poll fallback
    reaper_interval_s: float = 0.5       # virtual seconds

    # --- retries --------------------------------------------------------------
    backoff_base_s: float = 0.5          # virtual seconds
    backoff_cap_s: float = 8.0           # virtual seconds

    # --- pricing (illustrative values for cost metrics, NOT a real price list) --
    price_per_1k_prompt: float = 0.00015
    price_per_1k_completion: float = 0.0006

    # --- simulated providers -------------------------------------------------
    sim: dict[str, Any] = field(default_factory=dict)

    # --- fault injection (experiments only) ------------------------------------
    faults: dict[str, Any] = field(default_factory=dict)

    seed: int = 42

    def real(self, virtual_seconds: float) -> float:
        return virtual_seconds * self.time_scale

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Config":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})

    @classmethod
    def load(cls, path: str | None) -> "Config":
        if not path:
            return cls()
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    def dump(self, path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.to_dict(), fh, indent=2)


# Default simulated step latencies (virtual seconds) per job class.
DEFAULT_SIM = {
    "short": {"fetch_documents": 0.2, "extract": 0.1, "draft": 0.5, "decide": 0.05,
              "save_report": 0.05, "publish": 0.1, "draft_chunks": 1, "docs": 3},
    "long": {"fetch_documents": 1.5, "extract": 1.0, "draft": 1.5, "decide": 0.2,
             "save_report": 0.3, "publish": 0.5, "draft_chunks": 4, "docs": 12},
}
