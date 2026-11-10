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

    def _rule_action(self, job, n_docs: int, lim) -> str:
        """Rule policy (also the fallback for the LLM policy):
        1. not enough documents and tool budget left -> call_tool (fetch next page)
        2. approval required                         -> request_approval (WAITING_EVENT)
        3. otherwise                                 -> finish (save_report)"""
        p = job["params"]
        if n_docs < int(p.get("min_docs", 0)) and job["tool_calls"] < lim.max_tool_calls:
            return "call_tool"
        if p.get("require_approval", True):
            return "request_approval"
        return "finish"

    def _allowed_actions(self, job, lim) -> set[str]:
        """Hard guardrails the LLM cannot override: tool budget, and a required human approval cannot be skipped."""
        allowed = {"request_approval"}
        if job["tool_calls"] < lim.max_tool_calls:
            allowed.add("call_tool")
        if not job["params"].get("require_approval", True):
            allowed.add("finish")
        return allowed

    async def _llm_action(self, job, n_docs: int, lim) -> tuple[str | None, dict]:
        drafts = self.store.step_results(job["job_id"], "draft#")
        excerpt = " ".join(v.get("text", "") for v in drafts.values())[:600]
        p = job["params"]
        prompt = (
            "You control a report-writing agent. Choose its next action.\n"
            f"Documents read so far: {n_docs}. Documents wanted: {int(p.get('min_docs', 0))}.\n"
            f"Extra document fetches used: {job['tool_calls']} of {lim.max_tool_calls}.\n"
            f"Human approval required by policy: {'yes' if p.get('require_approval', True) else 'no'}.\n"
            f"Draft excerpt: {excerpt}\n\n"
            "Actions: CALL_TOOL (fetch more documents), REQUEST_APPROVAL (send the draft to a human), "
            "FINISH (publish without approval).\nAnswer with exactly one word: CALL_TOOL, REQUEST_APPROVAL or FINISH.")
        max_out = 10
        if job["tokens_used"] + estimate_tokens(prompt) + max_out > lim.max_tokens:
            raise BudgetExceeded(f"token budget exceeded: used={job['tokens_used']} max_tokens={lim.max_tokens}")
        res = await self.llm.complete(prompt, max_tokens=max_out, system="Reply with one word only.",
                                      meta=CallMeta(job["job_id"], job["next_step"], job["attempt"], self.sim(job)["decide"]))
        raw = (res.text or "").upper()
        found = [(raw.find(tok), act) for tok, act in (("CALL_TOOL", "call_tool"), ("REQUEST_APPROVAL", "request_approval"),
                                                       ("FINISH", "finish")) if tok in raw]
        choice = min(found)[1] if found else None
        return choice, {"llm_raw": (res.text or "")[:80], "tokens": res.total_tokens}

    async def step_decide(self, job, idx) -> StepCompletion:
        """The agent chooses its next action: call a tool, request approval, or finish.
        decide_mode = "rules" (default) or "llm" (job param or Config.decide_mode). In "llm" mode the model
        proposes the action; guardrails (_allowed_actions) are enforced in code and an invalid / disallowed
        answer falls back to the rule policy. The decision is checkpointed like any step result, so a retry
        after a crash may ask the model again but a committed decision is never re-made."""
        lim = self.store.limits_of(job)
        n_docs = sum(r["n_docs"] for r in self.store.step_results(job["job_id"], "extract#").values())
        mode = job["params"].get("decide_mode") or getattr(self.cfg, "decide_mode", "rules")
        info: dict = {"decide_mode": mode}
        tokens = 0
        if mode == "llm":
            choice, meta = await self._llm_action(job, n_docs, lim)
            tokens = meta.pop("tokens")
            info.update(meta, llm_choice=choice)
            if choice is None:
                action, info["decision_source"] = self._rule_action(job, n_docs, lim), "fallback_unparseable"
            elif choice not in self._allowed_actions(job, lim):
                action, info["decision_source"] = self._rule_action(job, n_docs, lim), "fallback_guardrail"
            else:
                action, info["decision_source"] = choice, "llm"
        else:
            await self._work(self.sim(job)["decide"])
            action, info["decision_source"] = self._rule_action(job, n_docs, lim), "rules"
        result = {"action": action, "n_docs": n_docs, **info}
        if action == "call_tool":
            k = job["tool_calls"] + 1
            return StepCompletion(result=result, next_step=f"fetch_documents#{k}", job_updates={"tool_calls": k},
                                  tokens=tokens)
        if action == "request_approval":
            return StepCompletion(result=result, action="wait_approval", tokens=tokens,
                                  job_updates={"approval_id": "apr-" + uuid.uuid4().hex[:12]})
        return StepCompletion(result=result, next_step="save_report#0", tokens=tokens)

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
