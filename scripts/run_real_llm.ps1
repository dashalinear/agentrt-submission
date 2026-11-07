# One command real-LLM run (Windows). Configure via .env (see .env.example) or:
#   $env:OPENAI_API_KEY="sk-..."; $env:MODEL="gpt-4o-mini"; .\scripts\run_real_llm.ps1
# Ollama: $env:OPENAI_API_KEY="ollama"; $env:OPENAI_BASE_URL="http://localhost:11434/v1"; $env:MODEL="llama3.1"
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$py = if (Test-Path .venv\Scripts\python.exe) { ".venv\Scripts\python.exe" } else { "python" }
$env:PYTHONPATH = "src"
& $py -m experiments.real_llm_run @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $py -m experiments.make_report
exit $LASTEXITCODE
