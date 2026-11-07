"""LLM provider abstraction: ``LLMClient`` with a deterministic simulator and an
OpenAI-compatible HTTP client (OpenAI, Ollama, LM Studio, vLLM, ...)."""
from __future__ import annotations

import asyncio
import hashlib
import math
import os
import random
import time
from dataclasses import dataclass, field
from typing import Protocol

from .errors import PermanentError, TransientError
from .ledger import CallLedger


@dataclass
class CallMeta:
    job_id: str | None = None
    step_key: str | None = None
    attempt: int | None = None
    latency_s: float | None = None   # virtual seconds (simulator only)


@dataclass
class LLMResult:
    text: str
    prompt_tokens: int
    completion_tokens: int
    model: str
    latency_s: float  # real seconds

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class LLMClient(Protocol):
    name: str

    async def complete(self, prompt: str, *, max_tokens: int, meta: CallMeta,
                       system: str | None = None) -> LLMResult: ...


def estimate_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))


def _rng(*parts) -> random.Random:
    h = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return random.Random(int(h[:16], 16))


@dataclass
class SimulatedLLM:
    """Deterministic LLM: the same (seed, prompt) gives the same text and token counts.

    Faults: ``fail_steps = {"draft": 2}`` -> first 2 attempts of every ``draft`` step raise
    TransientError (-1 = always). ``permanent_steps = {"draft"}`` -> PermanentError.
    ``error_rate`` -> random transient errors (seeded per job/step/attempt).
    """

    time_scale: float = 1.0
    latency_s: float = 0.5
    jitter: float = 0.0
    seed: int = 42
    error_rate: float = 0.0
    fail_steps: dict = field(default_factory=dict)
    permanent_steps: set = field(default_factory=set)
    ledger: CallLedger | None = None
    name: str = "simulated"

    async def complete(self, prompt: str, *, max_tokens: int, meta: CallMeta,
                       system: str | None = None) -> LLMResult:
        base = (meta.step_key or "").split("#")[0]
        rng = _rng(self.seed, meta.job_id, meta.step_key, meta.attempt)
        ptoks = estimate_tokens((system or "") + prompt)
        call_id = self.ledger.start("llm", base, meta.job_id, meta.step_key, meta.attempt, ptoks,
                                    model=self.name) if self.ledger else None
        t0 = time.perf_counter()
        try:
            lat = meta.latency_s if meta.latency_s is not None else self.latency_s
            if self.jitter:
                lat *= 1 + rng.uniform(-self.jitter, self.jitter)
            await asyncio.sleep(max(0.0, lat * self.time_scale))
            n_fail = self.fail_steps.get(base, 0)
            if n_fail == -1 or (meta.attempt or 1) <= n_fail:
                raise TransientError(f"simulated transient LLM error (503) on {meta.step_key}")
            if base in self.permanent_steps:
                raise PermanentError(f"simulated permanent LLM error (400) on {meta.step_key}")
            if self.error_rate and rng.random() < self.error_rate:
                raise TransientError("simulated random transient LLM error (429)")
            words = sorted({w.strip(".,:;").lower() for w in prompt.split() if len(w) > 6})
            pick = _rng(self.seed, prompt).sample(words, k=min(8, len(words))) if words else []
            ctoks = min(max_tokens, 60 + int(hashlib.md5(prompt.encode()).hexdigest()[:2], 16) % 40)
            text = f"[sim-draft] Key findings: {', '.join(pick)}."
            res = LLMResult(text, ptoks, ctoks, self.name, time.perf_counter() - t0)
        except BaseException as e:  # includes CancelledError (cancel / lease loss)
            outcome = "cancelled" if isinstance(e, asyncio.CancelledError) else type(e).__name__
            if self.ledger:
                self.ledger.finish(call_id, outcome)
            raise
        if self.ledger:
            self.ledger.finish(call_id, "ok", res.prompt_tokens, res.completion_tokens)
        return res


class OpenAICompatibleLLM:
    """Chat Completions client. Env: OPENAI_API_KEY, OPENAI_BASE_URL, MODEL.

    Ollama: OPENAI_BASE_URL=http://localhost:11434/v1, OPENAI_API_KEY=ollama, MODEL=llama3.1
    """

    def __init__(self, api_key: str | None = None, base_url: str | None = None, model: str | None = None,
                 timeout_s: float = 120.0, ledger: CallLedger | None = None, transport=None):
        import httpx

        self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self.base_url = (base_url or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        self.model = model or os.environ.get("MODEL") or "gpt-4o-mini"
        self.name = f"openai-compatible:{self.model}"
        self.ledger = ledger
        self._client = httpx.AsyncClient(timeout=timeout_s, transport=transport)

    async def complete(self, prompt: str, *, max_tokens: int, meta: CallMeta,
                       system: str | None = None) -> LLMResult:
        import httpx

        base = (meta.step_key or "").split("#")[0]
        call_id = self.ledger.start("llm", base, meta.job_id, meta.step_key, meta.attempt,
                                    estimate_tokens((system or "") + prompt), model=self.model) if self.ledger else None
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        t0 = time.perf_counter()
        try:
            try:
                r = await self._client.post(
                    f"{self.base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {},
                    json={"model": self.model, "messages": messages, "max_tokens": max_tokens,
                          "temperature": 0},
                )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                raise TransientError(f"LLM transport error: {type(e).__name__}: {e}") from e
            if r.status_code == 429 or r.status_code >= 500:
                raise TransientError(f"LLM HTTP {r.status_code}: {r.text[:200]}")
            if r.status_code >= 400:
                raise PermanentError(f"LLM HTTP {r.status_code}: {r.text[:200]}")
            data = r.json()
            text = data["choices"][0]["message"]["content"] or ""
            usage = data.get("usage") or {}
            res = LLMResult(text, int(usage.get("prompt_tokens") or estimate_tokens(prompt)),
                            int(usage.get("completion_tokens") or estimate_tokens(text)),
                            data.get("model", self.model), time.perf_counter() - t0)
        except BaseException as e:
            if self.ledger:
                self.ledger.finish(call_id, "cancelled" if isinstance(e, asyncio.CancelledError) else type(e).__name__)
            raise
        if self.ledger:
            self.ledger.finish(call_id, "ok", res.prompt_tokens, res.completion_tokens)
        return res


def make_llm(kind: str, cfg, ledger: CallLedger | None = None) -> LLMClient:
    if kind in ("real", "openai"):
        return OpenAICompatibleLLM(ledger=ledger)
    f = cfg.faults or {}
    return SimulatedLLM(time_scale=cfg.time_scale, seed=cfg.seed, ledger=ledger,
                        jitter=float(cfg.sim.get("llm_jitter", 0.0)),
                        error_rate=float(f.get("llm_error_rate", 0.0)),
                        fail_steps=dict(f.get("llm_fail", {})),
                        permanent_steps=set(f.get("llm_permanent", [])))
