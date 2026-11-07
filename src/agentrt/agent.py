"""The report agent: step implementations and the decision policy.

Step keys:  fetch_documents#k, extract#k, draft#r.c, decide#r, save_report#0, publish#0
  k = tool-call index, r = drafting round (tool loops + revisions), c = draft chunk.
Each step reads its inputs only from persisted step_results, so re-running a step after
a crash uses exactly the same inputs.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections import Counter

from .config import DEFAULT_SIM, Config
from .errors import BudgetExceeded, PermanentError
from .llm import CallMeta, LLMClient, estimate_tokens
from .store import StepCompletion, Store

SYSTEM_PROMPT = ("You are a careful analyst. Write a concise, factual section of a report "
                 "based only on the provided facts.")


class ReportAgent:
    def __init__(self, store: Store, cfg: Config, llm: LLMClient, tool, publisher):
        self.store, self.cfg, self.llm, self.tool, self.publisher = store, cfg, llm, tool, publisher

    def sim(self, job: dict) -> dict:
        cls = job["job_class"]
        return {**DEFAULT_SIM.get(cls, DEFAULT_SIM["short"]), **(self.cfg.sim.get(cls, {}) or {})}

    async def _work(self, seconds_virtual: float) -> None:
        await asyncio.sleep(self.cfg.real(seconds_virtual))

    def round_of(self, job: dict) -> int:
        return job["tool_calls"] + job["revisions"]

    async def run_step(self, job: dict) -> StepCompletion:
        name, _, idx = job["next_step"].partition("#")
        fn = getattr(self, f"step_{name}", None)
        if fn is None:
            raise PermanentError(f"unknown step {job['next_step']}")
        return await fn(job, idx)

    # ------------------------------------------------------------------ steps
    async def step_fetch_documents(self, job, idx) -> StepCompletion:
        k = int(idx)
        sim = self.sim(job)
        docs = await self.tool.fetch_documents(job["params"].get("collection_id", "default"), k, int(sim["docs"]),
                                               job_id=job["job_id"], step_key=job["next_step"],
                                               attempt=job["attempt"], latency_s=sim["fetch_documents"])
        return StepCompletion(result={"docs": docs}, next_step=f"extract#{k}")

    async def step_extract(self, job, idx) -> StepCompletion:
        k = int(idx)
        docs = self.store.step_result(job["job_id"], f"fetch_documents#{k}")["docs"]
        await self._work(self.sim(job)["extract"])
        cnt = Counter(w for d in docs for w in d["text"].split())
        facts = [f"{w} mentioned {n} times" for w, n in cnt.most_common(8)]
        return StepCompletion(result={"facts": facts, "n_docs": len(docs)},
                              next_step=f"draft#{self.round_of(job)}.0")

    async def step_draft(self, job, idx) -> StepCompletion:
        r, c = (int(x) for x in idx.split("."))
        sim = self.sim(job)
        lim = self.store.limits_of(job)
        facts = [f for res in self.store.step_results(job["job_id"], "extract#").values() for f in res["facts"]]
        fb = self.store.step_result(job["job_id"], f"feedback#{r}")
        prompt = (f"Report topic: {job['params'].get('topic', 'collection summary')}\n"
                  f"Section {c + 1} of {int(sim['draft_chunks'])}.\nFacts:\n- " + "\n- ".join(facts))
        if fb:
            prompt += f"\nReviewer feedback to address: {fb['comment']}"
        max_out = int(job["params"].get("max_output_tokens", 200))
        if job["tokens_used"] + estimate_tokens(SYSTEM_PROMPT + prompt) + max_out > lim.max_tokens:
            raise BudgetExceeded(f"token budget exceeded: used={job['tokens_used']} max_tokens={lim.max_tokens}")
        res = await self.llm.complete(prompt, max_tokens=max_out, system=SYSTEM_PROMPT,
                                      meta=CallMeta(job["job_id"], job["next_step"], job["attempt"], sim["draft"]))
        nxt = f"draft#{r}.{c + 1}" if c + 1 < int(sim["draft_chunks"]) else f"decide#{r}"
        return StepCompletion(result={"text": res.text, "prompt_tokens": res.prompt_tokens,
                                      "completion_tokens": res.completion_tokens, "model": res.model},
                              tokens=res.total_tokens, next_step=nxt)

    async def step_decide(self, job, idx) -> StepCompletion:
        """Agent policy (rule-based, guarded by limits):
        1. not enough documents and tool budget left -> call_tool (fetch next page)
        2. approval required                         -> request_approval (WAITING_EVENT)
        3. otherwise                                 -> finish (save_report)"""
        await self._work(self.sim(job)["decide"])
        lim = self.store.limits_of(job)
        n_docs = sum(r["n_docs"] for r in self.store.step_results(job["job_id"], "extract#").values())
        p = job["params"]
        if n_docs < int(p.get("min_docs", 0)) and job["tool_calls"] < lim.max_tool_calls:
            k = job["tool_calls"] + 1
            return StepCompletion(result={"action": "call_tool", "n_docs": n_docs}, next_step=f"fetch_documents#{k}",
                                  job_updates={"tool_calls": k})
        if p.get("require_approval", True):
            return StepCompletion(result={"action": "request_approval", "n_docs": n_docs}, action="wait_approval",
                                  job_updates={"approval_id": "apr-" + uuid.uuid4().hex[:12]})
        return StepCompletion(result={"action": "finish", "n_docs": n_docs}, next_step="save_report#0")

    def _report_body(self, job) -> dict:
        drafts = self.store.step_results(job["job_id"], "draft#")
        last_round = max(int(k.split("#")[1].split(".")[0]) for k in drafts)
        sections = [v["text"] for k, v in sorted(drafts.items()) if k.startswith(f"draft#{last_round}.")]
        return {"job_id": job["job_id"], "tenant_id": job["tenant_id"],
                "title": f"Report for {job['params'].get('collection_id', 'default')}", "sections": sections}

    async def step_save_report(self, job, idx) -> StepCompletion:
        await self._work(self.sim(job)["save_report"])
        body = self._report_body(job)
        body_s = json.dumps(body, sort_keys=True, ensure_ascii=False)
        h = hashlib.sha256(body_s.encode()).hexdigest()
        return StepCompletion(result={"report_sha256": h, "n_sections": len(body["sections"])},
                              next_step="publish#0",
                              side_writes=[("INSERT OR REPLACE INTO reports(job_id, body, created_at) VALUES(?,?,?)",
                                            (job["job_id"], body_s, self.store.now()))])

    async def step_publish(self, job, idx) -> StepCompletion:
        row = self.store.conn.execute("SELECT body FROM reports WHERE job_id=?", (job["job_id"],)).fetchone()
        body = json.loads(row[0])
        # fence check right before the external side effect (narrows, does not close, the race window)
        if not self.store.is_owner(job["job_id"], job["lease_token"]):
            from .errors import LeaseLost
            raise LeaseLost("lost lease before publish")
        await self._work(self.sim(job)["publish"])
        key = f"{job['job_id']}:publish"
        rec = await self.publisher.publish(key, body)
        f = self.cfg.faults
        if f.get("crash_after_publish_response"):
            marker = f.get("crash_marker")
            if not marker or not os.path.exists(marker):
                if marker:
                    open(marker, "w").close()
                os._exit(137)  # hard crash: side effect done, ack/checkpoint never committed
        return StepCompletion(result={"publication_id": rec["publication_id"], "replayed": rec.get("replayed", False),
                                      "idempotency_key": key}, action="succeed")
