"""CLI wrapper.

  python -m agentrt.cli worker    --db state.db --name w1 [--config cfg.json] [--publisher-url URL] [--ledger L.db] [--llm sim|real]
  python -m agentrt.cli serve     --db state.db --port 8000
  python -m agentrt.cli publisher --db publisher.db --port 8765
  python -m agentrt.cli create    --db state.db --tenant t1 [--key K] [--class short] [--no-approval]
  python -m agentrt.cli status    --db state.db JOB_ID
  python -m agentrt.cli event     --db state.db JOB_ID --event-id E --type approval --approval-id A [--decision approve]
  python -m agentrt.cli cancel    --db state.db JOB_ID
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid

from .config import Config
from .logs import setup_logging


def _store(args):
    from .store import Store

    return Store(args.db, Config.load(getattr(args, "config", None)))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="agentrt")
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("worker")
    w.add_argument("--db", required=True)
    w.add_argument("--name", default="w1")
    w.add_argument("--config")
    w.add_argument("--publisher-url")
    w.add_argument("--publisher-db")
    w.add_argument("--ledger")
    w.add_argument("--llm", default="sim", choices=["sim", "real"])
    w.add_argument("--log-file")

    s = sub.add_parser("serve")
    s.add_argument("--db", required=True)
    s.add_argument("--config")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)

    pb = sub.add_parser("publisher")
    pb.add_argument("--db", required=True)
    pb.add_argument("--host", default="127.0.0.1")
    pb.add_argument("--port", type=int, default=8765)

    c = sub.add_parser("create")
    c.add_argument("--db", required=True)
    c.add_argument("--config")
    c.add_argument("--tenant", required=True)
    c.add_argument("--key")
    c.add_argument("--class", dest="job_class", default="short")
    c.add_argument("--priority", type=int, default=0)
    c.add_argument("--no-approval", action="store_true")
    c.add_argument("--deadline", type=float, help="deadline_s (virtual seconds)")

    st = sub.add_parser("status")
    st.add_argument("--db", required=True)
    st.add_argument("job_id")

    e = sub.add_parser("event")
    e.add_argument("--db", required=True)
    e.add_argument("job_id")
    e.add_argument("--event-id", default=None)
    e.add_argument("--type", default="approval")
    e.add_argument("--approval-id")
    e.add_argument("--decision", default="approve", choices=["approve", "reject"])
    e.add_argument("--comment", default="")

    cc = sub.add_parser("cancel")
    cc.add_argument("--db", required=True)
    cc.add_argument("job_id")

    args = p.parse_args(argv)

    if args.cmd == "worker":
        setup_logging(path=args.log_file)
        from .runtime import build_worker

        cfg = Config.load(args.config)
        worker = build_worker(args.db, cfg, args.name, llm_kind=args.llm, ledger_path=args.ledger,
                              publisher_url=args.publisher_url, publisher_db=args.publisher_db)
        try:
            asyncio.run(worker.run())
        except KeyboardInterrupt:
            pass
        return 0
    if args.cmd == "serve":
        import uvicorn

        from .api import create_app

        setup_logging()
        uvicorn.run(create_app(args.db, Config.load(args.config)), host=args.host, port=args.port, log_level="warning")
        return 0
    if args.cmd == "publisher":
        import uvicorn

        from .publisher import create_publisher_app

        uvicorn.run(create_publisher_app(args.db), host=args.host, port=args.port, log_level="warning")
        return 0

    setup_logging()
    store = _store(args)
    if args.cmd == "create":
        from .store import Rejected

        limits = {"deadline_s": args.deadline} if args.deadline else {}
        try:
            job, created = store.create_job(args.tenant, args.key, args.job_class, args.priority,
                                            {"require_approval": not args.no_approval}, limits)
        except Rejected as ex:
            print(json.dumps({"status": "REJECTED", "reason": ex.reason}))
            return 2
        print(json.dumps({"job_id": job["job_id"], "state": job["state"], "created": created}))
    elif args.cmd == "status":
        job = store.get_job(args.job_id)
        if job is None:
            print(json.dumps({"error": "not found"}))
            return 1
        job.pop("lease_token", None)
        job["steps_done"] = list(store.step_results(args.job_id).keys())
        print(json.dumps(job, indent=2, default=str))
    elif args.cmd == "event":
        job = store.get_job(args.job_id)
        payload = {"approval_id": args.approval_id or (job or {}).get("approval_id"), "decision": args.decision,
                   "comment": args.comment}
        print(json.dumps(store.submit_event(args.job_id, args.event_id or "evt-" + uuid.uuid4().hex[:12],
                                            args.type, payload)))
    elif args.cmd == "cancel":
        print(json.dumps(store.cancel(args.job_id)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
