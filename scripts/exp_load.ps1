$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$py = if (Test-Path .venv\Scripts\python.exe) { ".venv\Scripts\python.exe" } else { "python" }
$env:PYTHONPATH = "src"
& $py -m experiments.exp_load --seeds 11,12,13,14,15 --policies fifo,drr,drr_iso --scale 0.1 @args
exit $LASTEXITCODE
