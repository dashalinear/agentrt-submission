"""LLM provider: deterministic simulator; OpenAI-compatible client against a MOCK HTTP
transport (no network - this does NOT replace the real-LLM run)."""
import asyncio
import json

import httpx
import pytest

from agentrt.errors import PermanentError, TransientError
from agentrt.ledger import CallLedger
from agentrt.llm import CallMeta, OpenAICompatibleLLM, SimulatedLLM


def test_simulated_llm_is_deterministic():
    a = SimulatedLLM(time_scale=0.0, seed=1)
    b = SimulatedLLM(time_scale=0.0, seed=1)
    m = CallMeta("j", "draft#0.0", 1)
    ra = asyncio.run(a.complete("some longer prompt about latency and throughput", max_tokens=100, meta=m))
    rb = asyncio.run(b.complete("some longer prompt about latency and throughput", max_tokens=100, meta=m))
    assert (ra.text, ra.prompt_tokens, ra.completion_tokens) == (rb.text, rb.prompt_tokens, rb.completion_tokens)


def _client(handler, ledger=None):
    return OpenAICompatibleLLM(api_key="test", base_url="http://mock/v1", model="mock-model",
                               transport=httpx.MockTransport(handler), ledger=ledger)


def test_openai_compatible_success_and_usage(tmp_path):
    seen = {}

    def handler(req: httpx.Request):
        seen["auth"] = req.headers.get("authorization")
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"model": "mock-model", "choices": [{"message": {"content": "hello"}}],
                                         "usage": {"prompt_tokens": 11, "completion_tokens": 3}})

    led = CallLedger(str(tmp_path / "l.db"))
    r = asyncio.run(_client(handler, led).complete("p", max_tokens=5, meta=CallMeta("j", "draft#0.0", 1), system="s"))
    assert r.text == "hello" and r.prompt_tokens == 11 and r.completion_tokens == 3
    assert seen["auth"] == "Bearer test" and seen["body"]["messages"][0]["role"] == "system"
    row = led.rows()[0]
    assert row["outcome"] == "ok" and row["completion_tokens"] == 3


@pytest.mark.parametrize("status,exc", [(429, TransientError), (503, TransientError), (400, PermanentError),
                                        (401, PermanentError)])
def test_openai_compatible_error_classification(status, exc):
    cl = _client(lambda req: httpx.Response(status, text="err"))
    with pytest.raises(exc):
        asyncio.run(cl.complete("p", max_tokens=5, meta=CallMeta()))


def test_openai_compatible_transport_error_is_transient():
    def boom(req):
        raise httpx.ConnectError("refused")

    with pytest.raises(TransientError):
        asyncio.run(_client(boom).complete("p", max_tokens=5, meta=CallMeta()))
