"""Structured JSON logging. Every record carries job_id, step, attempt, event_id (null if n/a)."""
from __future__ import annotations

import json
import logging
import os
import sys
import time

_FIELDS = ("job_id", "step", "attempt", "event_id", "worker", "tenant_id", "state")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": round(record.created, 6),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for f in _FIELDS:
            out[f] = getattr(record, f, None)
        extra = getattr(record, "data", None)
        if extra:
            out["data"] = extra
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False, default=str)


def setup_logging(level: str | None = None, path: str | None = None) -> None:
    root = logging.getLogger("agentrt")
    if getattr(root, "_configured", False):
        return
    root.setLevel(level or os.environ.get("AGENTRT_LOG_LEVEL", "INFO"))
    handler: logging.Handler
    path = path or os.environ.get("AGENTRT_LOG_FILE")
    if path:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.propagate = False
    root._configured = True  # type: ignore[attr-defined]


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"agentrt.{name}")


def log(logger: logging.Logger, level: int, msg: str, *, job_id=None, step=None, attempt=None,
        event_id=None, **data) -> None:
    if not logger.isEnabledFor(level):
        return
    extra = {"job_id": job_id, "step": step, "attempt": attempt, "event_id": event_id,
             "worker": data.pop("worker", None), "tenant_id": data.pop("tenant_id", None),
             "state": data.pop("state", None), "data": data or None}
    logger.log(level, msg, extra=extra)


now = time.time
