# Tests + Part 1/2/3 experiments inside a Linux container (real POSIX SIGKILL) -> results/raw_linux/
# Needs Docker Desktop. The repo is mounted read-only; only results/raw_linux is writable.
# Usage: .\scripts\run_linux.ps1 [-Parts tests,recovery,events,load]
param([string]$Parts = "tests,recovery,events,load")
$ErrorActionPreference = "Continue"   # docker writes progress to stderr; failures are checked via $LASTEXITCODE
Set-Location (Join-Path $PSScriptRoot "..")
$src = (Get-Location).Path
docker build -q -t agentrt:local . | Out-Null
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
New-Item -ItemType Directory -Force results\raw_linux | Out-Null
$cmds = @{
    tests    = "python -m pytest -v -p no:cacheprovider > /out/pytest_output.txt 2>&1; tail -1 /out/pytest_output.txt"
    recovery = "python -m experiments.exp_recovery --reps 5 --out /out"
    events   = "python -m experiments.exp_events --race-reps 300 --out /out"
    load     = "python -m experiments.exp_load --seeds 11,12,13 --policies fifo,drr,drr_iso --scale 0.1 --out /out"
}
foreach ($p in ($Parts -split ",")) {
    Write-Host "== linux: $p"
    docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/w/src:/w `
        -v "${src}:/w:ro" -v "${src}\results\raw_linux:/out" --tmpfs /w/results/work -w /w `
        agentrt:local sh -c $cmds[$p.Trim()]
    if ($LASTEXITCODE -ne 0) { throw "linux run '$p' failed" }
}
