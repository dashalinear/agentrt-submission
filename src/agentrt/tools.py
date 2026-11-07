"""Simulated document-collection tool (``fetch_documents``)."""
from __future__ import annotations

import asyncio
import hashlib
import random
from dataclasses import dataclass, field

from .errors import TransientError
from .ledger import CallLedger

_VOCAB = ("latency throughput availability incident postmortem rollback capacity "
          "regression deployment telemetry backlog customer revenue retention churn "
          "forecast anomaly compliance encryption migration database replication "
          "benchmark scheduler fairness isolation recovery").split()


@dataclass
class DocumentTool:
    time_scale: float = 1.0
    seed: int = 42
    fail_steps: dict = field(default_factory=dict)  # {"fetch_documents": N}
    ledger: CallLedger | None = None

    async def fetch_documents(self, collection_id: str, page: int, n_docs: int, *, job_id: str,
                              step_key: str, attempt: int, latency_s: float) -> list[dict]:
        call_id = self.ledger.start("tool", "fetch_documents", job_id, step_key, attempt) if self.ledger else None
        try:
            await asyncio.sleep(latency_s * self.time_scale)
            n_fail = self.fail_steps.get("fetch_documents", 0)
            if n_fail == -1 or attempt <= n_fail:
                raise TransientError("simulated tool timeout (fetch_documents)")
            docs = []
            for i in range(n_docs):
                h = hashlib.sha256(f"{self.seed}|{collection_id}|{page}|{i}".encode()).hexdigest()
                rnd = random.Random(int(h[:12], 16))
                body = " ".join(rnd.choice(_VOCAB) for _ in range(40))
                docs.append({"doc_id": f"{collection_id}-p{page}-d{i}", "title": f"Doc {page}.{i}",
                             "text": body})
        except BaseException as e:
            if self.ledger:
                self.ledger.finish(call_id, "cancelled" if isinstance(e, asyncio.CancelledError) else type(e).__name__)
            raise
        if self.ledger:
            self.ledger.finish(call_id, "ok")
        return docs
