"""Real-LLM run (OpenAI-compatible endpoint: OpenAI / Ollama / LM Studio / vLLM).

Reads OPENAI_API_KEY, OPENAI_BASE_URL, MODEL (and optional PRICE_PER_1K_PROMPT /
PRICE_PER_1K_COMPLETION) from the environment or from ./.env.  Writes ONLY to
results/real_llm/ - never mixes with the simulated results in results/raw/.

  A) happy path: N jobs (approval sent automatically) -> per-call latency, tokens, cost
  B) recovery: kill the worker process during the real LLM call (s2b) and after the
     publish side effect (s3a) -> repeated calls, extra tokens, duplicates
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid

from experiments.common import ROOT, environment, fresh_dir, write_jsonl

OUT = os.path.join(ROOT, "results", "real_llm")


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=3)
    ap.add_argument("--skip-recovery", action="store_true")
    ap.add_argument("--recovery-reps", type=int, default=5, help="repetitions per recovery scenario")
    ap.add_argument("--decide-mode", default="llm", choices=["llm", "rules"],
                    help="llm: the model chooses the next action (with code guardrails)")
    ap.add_argument("--out", default=OUT, help="output dir (default results/real_llm)")
    a = ap.parse_args(argv)
    out = a.out
    load_dotenv(os.path.join(ROOT, ".env"))
    if not (os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_BASE_URL")):
        print("Set OPENAI_API_KEY (and optionally OPENAI_BASE_URL, MODEL) or create .env from .env.example",
              file=sys.stderr)
        return 2
    os.makedirs(out, exist_ok=True)
    from agentrt.config import Config
    from agentrt.harness import ThreadWorker
    from agentrt.ledger import CallLedger
    from agentrt.store import Store

    pp = float(os.environ.get("PRICE_PER_1K_PROMPT", "0.00015"))
    pc = float(os.environ.get("PRICE_PER_1K_COMPLETION", "0.0006"))
    d = fresh_dir("real_llm")
    db, ledger = os.path.join(d, "s.db"), os.path.join(d, "ledger.db")
    cfg = Config(time_scale=1.0, poll_interval_s=0.01, lease_timeout_s=30, heartbeat_interval_s=1.0,
                 price_per_1k_prompt=pp, price_per_1k_completion=pc,
                 sim={"short": {"fetch_documents": 0.2, "extract": 0.1, "decide": 0.05, "save_report": 0.05,
                                "publish": 0.1, "draft_chunks": 2, "docs": 3}})
    store = Store(db, cfg)
    t0 = time.time()
    jobs = []
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db"), ledger_path=ledger, llm_kind="real"):
        for i in range(a.jobs):
            j, _ = store.create_job("real-tenant", f"real-{uuid.uuid4().hex[:6]}", params={
                "topic": "operational review of a web service", "collection_id": f"real-{i}",
                "min_docs": 5, "decide_mode": a.decide_mode},
                limits={"step_timeout_s": 120, "deadline_s": 900})
            jobs.append(j["job_id"])
        pending = set(jobs)
        while pending and time.time() - t0 < 1200:
            for jid in list(pending):
                j = store.get_job(jid)
                if j["state"] == "WAITING_EVENT":
                    store.submit_event(jid, "auto-approve-" + jid, "approval",
                                       {"approval_id": j["approval_id"], "decision": "approve"})
                elif j["state"] in ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"):
                    pending.discard(jid)
            time.sleep(0.2)
    calls = [c for c in CallLedger(ledger).rows() if c["provider"] == "llm"]
    lat = [c["ended_at"] - c["started_at"] for c in calls if c["outcome"] == "ok"]
    ptok = sum(c["prompt_tokens"] for c in calls if c["outcome"] == "ok")
    ctok = sum(c["completion_tokens"] for c in calls if c["outcome"] == "ok")
    job_rows = []
    for jid in jobs:
        j = store.get_job(jid)
        job_rows.append({"job_id": jid, "state": j["state"], "error": j["error"], "tokens_used": j["tokens_used"],
                         "e2e_s": round((j["finished_at"] or time.time()) - j["created_at"], 3),
                         "steps": list(store.step_results(jid).keys()),
                         "decisions": [{k: v for k, v in r.items() if k in ("action", "decision_source", "llm_choice",
                                                                             "llm_raw", "n_docs")}
                                       for r in store.step_results(jid, "decide#").values()]})
    summary = {
        "kind": "REAL LLM RUN", "model": os.environ.get("MODEL", "gpt-4o-mini"), "decide_mode": a.decide_mode,
        "recovery_reps": 0 if a.skip_recovery else a.recovery_reps,
        "base_url": os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        "env": environment(), "jobs": job_rows,
        "llm_calls": len(calls), "llm_calls_ok": len(lat),
        "llm_latency_s": {"n": len(lat), "p50": statistics.median(lat) if lat else None,
                          "max": max(lat) if lat else None},
        "prompt_tokens": ptok, "completion_tokens": ctok,
        "cost_usd": round(ptok / 1000 * pp + ctok / 1000 * pc, 6), "price_per_1k": [pp, pc],
    }
    write_jsonl(os.path.join(out, "calls.jsonl"), calls)
    with open(os.path.join(out, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    print(json.dumps({k: v for k, v in summary.items() if k != "env"}, indent=2, default=str))

    if not a.skip_recovery:
        from experiments import exp_recovery

        exp_recovery.main(["--reps", str(a.recovery_reps), "--llm", "real", "--out", out,
                           "--cases", "baseline,s2b_kill_during_llm_restart,s3a_crash_after_publish"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
