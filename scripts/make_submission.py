"""Build submission.zip: the repository incl. .git, results/ (raw CSV/JSONL, figures), REPORT.md/.pdf.
Excludes the virtualenv, scratch state (results/work), SQLite files and .env."""
import os
import zipfile

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
skip_dirs = {".venv", "__pycache__", ".pytest_cache", "work"}
out = os.path.join(root, "submission.zip")
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in skip_dirs]
        for fn in fns:
            if fn in ("submission.zip", ".env") or fn.endswith((".db", ".db-wal", ".db-shm")):
                continue
            p = os.path.join(dp, fn)
            z.write(p, os.path.join("agentrt-submission", os.path.relpath(p, root)))
print(out, os.path.getsize(out), "bytes")
