"""Wiring helpers: build a Worker from config + CLI options."""
from __future__ import annotations

from .config import Config
from .ledger import CallLedger
from .llm import make_llm
from .publisher import FakePublisherService, HttpPublisherClient, LocalPublisherClient
from .tools import DocumentTool
from .worker import Worker


def build_worker(db_path: str, cfg: Config, name: str, *, llm_kind: str = "sim", ledger_path: str | None = None,
                 publisher_url: str | None = None, publisher_db: str | None = None,
                 publisher_client=None) -> Worker:
    ledger = CallLedger(ledger_path) if ledger_path else None
    llm = make_llm(llm_kind, cfg, ledger)
    tool = DocumentTool(time_scale=cfg.time_scale, seed=cfg.seed, ledger=ledger,
                        fail_steps=dict((cfg.faults or {}).get("tool_fail", {})))
    if publisher_client is not None:
        pub = publisher_client
    elif publisher_url:
        pub = HttpPublisherClient(publisher_url)
    else:
        pub = LocalPublisherClient(FakePublisherService(publisher_db or db_path + ".publisher.db"))
    return Worker(db_path, cfg, name, llm, tool, pub)
