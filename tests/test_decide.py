"""The decision step: rule policy and LLM policy with code-enforced guardrails."""
import asyncio
import json
import time

import pytest

from agentrt.agent import ReportAgent
from agentrt.config import Config
from agentrt.harness import ThreadWorker
from agentrt.llm import LLMResult
from agentrt.store import Store
from helpers import wait_for


class FakeLLM:
    def __init__(self, text):
        self.text, self.calls = text, 0

    async def complete(self, prompt, *, max_tokens, meta, system=None):
        self.calls += 1
        assert "CALL_TOOL" in prompt and "REQUEST_APPROVAL" in prompt and "FINISH" in prompt
        return LLMResult(self.text, 50, 2, "fake", 0.0)


def _decide(tmp_path, llm_text, *, require_approval=True, min_docs=0, n_docs=3, tool_calls=0, mode="llm"):
    cfg = Config(time_scale=0.0)
    store = Store(str(tmp_path / "s.db"), cfg)
    job, _ = store.create_job("t", "k", params={"require_approval": require_approval, "min_docs": min_docs,
                                                "decide_mode": mode})
    store.conn.execute("INSERT INTO step_results(job_id, step_key, result, attempt, created_at) VALUES(?,?,?,?,?)",
                       (job["job_id"], "extract#0", json.dumps({"facts": [], "n_docs": n_docs}), 1, time.time()))
    job = store.get_job(job["job_id"])
    job.update(next_step="decide#0", attempt=1, tool_calls=tool_calls)
    llm = FakeLLM(llm_text)
    sc = asyncio.run(ReportAgent(store, cfg, llm, None, None).step_decide(job, "0"))
    return sc, llm


def test_llm_choice_is_applied_when_allowed(tmp_path):
    sc, llm = _decide(tmp_path, "CALL_TOOL", min_docs=10)
    assert llm.calls == 1
    assert sc.result["action"] == "call_tool" and sc.result["decision_source"] == "llm"
    assert sc.next_step == "fetch_documents#1" and sc.job_updates == {"tool_calls": 1}
    assert sc.tokens == 52  # decision tokens count against the job budget


def test_llm_cannot_skip_required_approval(tmp_path):
    sc, _ = _decide(tmp_path, "FINISH", require_approval=True)
    assert sc.result["llm_choice"] == "finish"
    assert sc.result["decision_source"] == "fallback_guardrail"
    assert sc.result["action"] == "request_approval" and sc.action == "wait_approval"


def test_llm_finish_allowed_without_approval(tmp_path):
    sc, _ = _decide(tmp_path, "finish.", require_approval=False)
    assert sc.result["action"] == "finish" and sc.result["decision_source"] == "llm"
    assert sc.next_step == "save_report#0"


def test_llm_cannot_exceed_tool_budget(tmp_path):
    sc, _ = _decide(tmp_path, "CALL_TOOL", min_docs=10, tool_calls=3)  # default max_tool_calls = 3
    assert sc.result["decision_source"] == "fallback_guardrail"
    assert sc.result["action"] == "request_approval"


def test_unparseable_answer_falls_back_to_rules(tmp_path):
    sc, _ = _decide(tmp_path, "I am not sure, maybe later", min_docs=10)
    assert sc.result["decision_source"] == "fallback_unparseable"
    assert sc.result["action"] == "call_tool"  # what the rule policy says for n_docs < min_docs


def test_rules_mode_does_not_call_llm(tmp_path):
    sc, llm = _decide(tmp_path, "FINISH", mode="rules")
    assert llm.calls == 0 and sc.result["decision_source"] == "rules"


@pytest.mark.parametrize("require_approval", [False, True])
def test_llm_decide_end_to_end_with_simulated_llm(tmp_path, require_approval):
    sim = {"short": {"fetch_documents": 0.01, "extract": 0.01, "draft": 0.01, "draft_chunks": 1, "decide": 0.01,
                     "save_report": 0.01, "publish": 0.01, "docs": 3}}
    cfg = Config(time_scale=1.0, poll_interval_s=0.005, sim=sim, decide_mode="llm")
    db = str(tmp_path / "s.db")
    store = Store(db, cfg)
    with ThreadWorker(db, cfg, publisher_db=str(tmp_path / "pub.db")):
        job, _ = store.create_job("t", "k", params={"require_approval": require_approval})
        jid = job["job_id"]
        if require_approval:
            wait_for(lambda: store.get_job(jid)["state"] == "WAITING_EVENT", timeout=20)
            j = store.get_job(jid)
            store.submit_event(jid, "evt-1", "approval", {"approval_id": j["approval_id"], "decision": "approve"})
        wait_for(lambda: store.get_job(jid)["state"] in ("SUCCEEDED", "FAILED"), timeout=20)
    assert store.get_job(jid)["state"] == "SUCCEEDED"
    dec = store.step_result(jid, "decide#0")
    assert dec["decide_mode"] == "llm" and dec["decision_source"] in ("llm", "fallback_guardrail", "fallback_unparseable")
