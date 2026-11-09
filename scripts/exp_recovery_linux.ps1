# Part 1 recovery experiment inside a Linux container (real POSIX SIGKILL) -> results/raw_linux/
# Needs Docker Desktop. The repo is mounted read-only; only results/raw_linux is writable.
$ErrorActionPreference = "Continue"   # docker writes progress to stderr
Set-Location (Join-Path $PSScriptRoot "..")
$src = (Get-Location).Path
docker build -q -t agentrt:local . | Out-Null
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
New-Item -ItemType Directory -Force results\raw_linux | Out-Null
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/w/src:/w `
    -v "${src}:/w:ro" -v "${src}\results\raw_linux:/out" --tmpfs /w/results/work -w /w `
    agentrt:local python -m experiments.exp_recovery --reps 5 --out /out
exit $LASTEXITCODE
