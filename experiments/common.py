"""Shared helpers for experiments (process management, stats)."""
from __future__ import annotations

import csv
import json
import math
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
RAW = os.path.join(ROOT, "results", "raw")
FIG = os.path.join(ROOT, "results", "figures")
WORK = os.path.join(ROOT, "results", "work")
sys.path.insert(0, SRC)


RUN_ID = time.strftime("%Y%m%d-%H%M%S") + f"-{os.getpid()}"


def fresh_dir(*parts: str) -> str:
    """New, empty working directory unique to this process run (never reuses old state)."""
    d = os.path.join(WORK, RUN_ID, *parts)
    os.makedirs(d, exist_ok=False)
    return d


def env() -> dict:
    return dict(os.environ, PYTHONPATH=SRC + os.pathsep + os.environ.get("PYTHONPATH", ""),
                AGENTRT_LOG_LEVEL=os.environ.get("AGENTRT_LOG_LEVEL", "INFO"))


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def start_publisher(db: str) -> tuple[subprocess.Popen, str]:
    import httpx

    port = free_port()
    proc = subprocess.Popen([sys.executable, "-m", "agentrt.cli", "publisher", "--db", db, "--port", str(port)],
                            env=env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://127.0.0.1:{port}"
    t0 = time.time()
    while time.time() - t0 < 20:
        try:
            if httpx.get(url + "/health", timeout=0.5).status_code == 200:
                return proc, url
        except Exception:  # noqa: BLE001
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("publisher did not start")


def spawn_worker(db: str, cfg_path: str, name: str, *, ledger: str | None = None, publisher_url: str | None = None,
                 log_file: str | None = None, llm: str = "sim") -> subprocess.Popen:
    cmd = [sys.executable, "-m", "agentrt.cli", "worker", "--db", db, "--name", name, "--config", cfg_path,
           "--llm", llm]
    if ledger:
        cmd += ["--ledger", ledger]
    if publisher_url:
        cmd += ["--publisher-url", publisher_url]
    if log_file:
        cmd += ["--log-file", log_file]
    return subprocess.Popen(cmd, env=env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def hard_kill(proc: subprocess.Popen) -> float:
    """SIGKILL on POSIX; TerminateProcess on Windows (no handlers, no finally blocks)."""
    proc.kill()
    t = time.time()
    proc.wait(10)
    return t


def ro(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c


def wait_for(cond, timeout=30.0, interval=0.005):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = cond()
        except sqlite3.OperationalError:
            v = None
        if v:
            return v
        time.sleep(interval)
    raise TimeoutError("condition not reached")


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x)))
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100.0
    f, c = math.floor(k), math.ceil(k)
    return xs[int(k)] if f == c else xs[f] + (xs[c] - xs[f]) * (k - f)


def write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path: str, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def environment() -> dict:
    import importlib.metadata as md

    info = {"os": platform.platform(), "python": sys.version.split()[0], "machine": platform.machine(),
            "processor": platform.processor(), "cpu_count": os.cpu_count(), "sqlite": sqlite3.sqlite_version}
    try:
        import ctypes

        class MS(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("a", ctypes.c_ulonglong), ("b", ctypes.c_ulonglong),
                        ("c", ctypes.c_ulonglong), ("d", ctypes.c_ulonglong), ("e", ctypes.c_ulonglong),
                        ("f", ctypes.c_ulonglong)]
        if os.name == "nt":
            m = MS()
            m.dwLength = ctypes.sizeof(MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
            info["ram_gb"] = round(m.ullTotalPhys / 2 ** 30, 1)
        else:
            info["ram_gb"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2 ** 30, 1)
    except Exception:  # noqa: BLE001
        pass
    for pkg in ("fastapi", "uvicorn", "httpx", "pydantic", "starlette", "pytest", "matplotlib", "numpy", "Markdown"):
        try:
            info[pkg] = md.version(pkg)
        except md.PackageNotFoundError:
            pass
    return info


def high_res_timer() -> None:
    """Windows: raise timer resolution to 1 ms (default ~15.6 ms) for scaled-time runs."""
    if os.name == "nt":
        try:
            import ctypes

            ctypes.windll.winmm.timeBeginPeriod(1)
        except Exception:  # noqa: BLE001
            pass
