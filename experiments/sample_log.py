"""Produce a sample of the structured JSON log (results/raw/sample_structured_log.jsonl):
one job with approval, a duplicate event, a late event and a transient LLM error."""
from __future__ import annotations

import os
import time

from experiments.common import RAW, fresh_dir


def main():
    path = os.path.join(RAW, "sample_structured_log.jsonl")
    if os.path.exists(path):
        os.remove(path)
    os.environ["AGENTRT_LOG_LEVEL"] = "INFO"
    from agentrt.config import Config
    from agentrt.harness import ThreadWorker
    from agentrt.logs import setup_logging
    from agentrt.store import Store

    setup_logging(level="INFO", path=path)
    d = fresh_dir("sample_log")
    db = os.path.join(d, "s.db")
    cfg = Config(time_scale=0.05, poll_interval_s=0.005, faults={"llm_fail": {"draft": 1}}, backoff_base_s=0.5)
    s = Store(db, cfg)
    j, _ = s.create_job("tenant-demo", "demo-key")
    jid = j["job_id"]
    with ThreadWorker(db, cfg, publisher_db=os.path.join(d, "pub.db")):
        while s.get_job(jid)["state"] != "WAITING_EVENT":
            time.sleep(0.01)
        apr = s.get_job(jid)["approval_id"]
        s.submit_event(jid, "evt-demo-1", "approval", {"approval_id": apr, "decision": "approve"})
        s.submit_event(jid, "evt-demo-1", "approval", {"approval_id": apr, "decision": "approve"})  # duplicate
        while s.get_job(jid)["state"] != "SUCCEEDED":
            time.sleep(0.01)
        s.submit_event(jid, "evt-demo-2", "approval", {"approval_id": apr})  # late
        time.sleep(0.2)
    print(path)


if __name__ == "__main__":
    main()
