# Clean state -> tests -> all experiments -> figures + REPORT.md/.pdf (Windows)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$py = if (Test-Path .venv\Scripts\python.exe) { ".venv\Scripts\python.exe" } else { "python" }
$env:PYTHONPATH = "src"
Remove-Item -Recurse -Force results\work, results\raw, results\figures -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force results\raw, results\figures | Out-Null
& $py -m pytest -v | Tee-Object -FilePath results\pytest_output.txt
& $py -m experiments.exp_recovery --reps 5
& $py -m experiments.exp_events --race-reps 300
& $py -m experiments.exp_load --seeds 11,12,13 --policies fifo,drr,drr_iso --scale 0.1
& $py -m experiments.make_report
