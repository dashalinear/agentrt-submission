"""HTTP API (FastAPI): create job, get status, submit event, cancel, metrics."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import Config
from .store import Rejected, Store


class CreateJob(BaseModel):
    tenant_id: str
    job_class: str = "short"
    priority: int = 0
    params: dict[str, Any] = Field(default_factory=dict)
    limits: dict[str, Any] = Field(default_factory=dict)


class EventIn(BaseModel):
    event_id: str
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)


def create_app(db_path: str, cfg: Config | None = None) -> FastAPI:
    store = Store(db_path, cfg or Config())
    app = FastAPI(title="agentrt")
    app.state.store = store

    @app.post("/jobs")
    def create_job(body: CreateJob, idempotency_key: str | None = Header(None, alias="Idempotency-Key")):
        try:
            job, created = store.create_job(body.tenant_id, idempotency_key, body.job_class, body.priority,
                                            body.params, body.limits)
        except Rejected as e:
            return JSONResponse({"status": "REJECTED", "reason": e.reason}, status_code=429,
                                headers={"Retry-After": "1"})
        return JSONResponse({"job_id": job["job_id"], "state": job["state"], "created": created},
                            status_code=201 if created else 200)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        job["steps_done"] = list(store.step_results(job_id).keys())
        job["transitions"] = [{k: t[k] for k in ("src", "dst", "version", "ts", "reason")}
                              for t in store.transitions(job_id)]
        job.pop("lease_token", None)
        return job

    @app.post("/jobs/{job_id}/events", status_code=202)
    def submit_event(job_id: str, ev: EventIn):
        if store.get_job(job_id) is None:
            raise HTTPException(404, "job not found")
        return store.submit_event(job_id, ev.event_id, ev.type, ev.payload)

    @app.post("/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        r = store.cancel(job_id)
        if r["outcome"] == "not_found":
            raise HTTPException(404, "job not found")
        return r

    @app.get("/metrics")
    def metrics():
        return store.counts()

    return app
