# Manual end-to-end demo (Windows PowerShell), separate processes, no test harness:
#   idempotent create -> worker killed mid-job (TerminateProcess) -> restart -> resume without manual state change
#   -> duplicate approval event (same event_id) -> late event after terminal -> 1 logical publication.
# Usage: .\scripts\demo_manual.ps1 [-KillAfter 3] [-Port 8765]
param([double]$KillAfter = 3, [int]$Port = 8765)
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")
$root = (Get-Location).Path
$py = if (Test-Path .venv\Scripts\python.exe) { (Resolve-Path .venv\Scripts\python.exe).Path } else { "python" }
$env:PYTHONPATH = "src"
$d = "results\work\demo-" + (Get-Date -Format "yyyyMMdd-HHmmss")
New-Item -ItemType Directory -Force $d | Out-Null
$env:AGENTRT_LOG_FILE = "$d\cli.log"          # CLI logs go to a file; stdout keeps only JSON answers
$state = "$d\state.db"; $pubdb = "$d\publisher.db"; $pubUrl = "http://127.0.0.1:$Port"

function AgentCli([string[]]$a) { (& $py -m agentrt.cli @a) -join "`n" }
function Status($id) { AgentCli @("status", "--db", $state, $id) | ConvertFrom-Json }
function WaitState($id, [string[]]$want, [int]$timeoutS = 120) {
    $t = Get-Date
    while (((Get-Date) - $t).TotalSeconds -lt $timeoutS) {
        $s = Status $id
        if ($want -contains $s.state) { return $s }
        Start-Sleep -Milliseconds 200
    }
    throw "job $id did not reach $($want -join '/') in $timeoutS s"
}
function StartWorker([string]$log) {
    Start-Process $py -WorkingDirectory $root -WindowStyle Hidden -PassThru -ArgumentList @(
        "-m", "agentrt.cli", "worker", "--db", $state, "--name", "w1", "--publisher-url", $pubUrl, "--log-file", $log)
}
function StepStarts([string]$log) {
    if (-not (Test-Path $log)) { return @() }
    Get-Content $log | ForEach-Object { $_ | ConvertFrom-Json } | Where-Object { $_.msg -eq "step start" } |
        ForEach-Object { "$($_.step)#a$($_.attempt)" }
}

$pub = Start-Process $py -WorkingDirectory $root -WindowStyle Hidden -PassThru -ArgumentList @(
    "-m", "agentrt.cli", "publisher", "--db", $pubdb, "--port", "$Port")
$w = $null
try {
    for ($i = 0; $i -lt 100; $i++) { try { Invoke-RestMethod "$pubUrl/health" | Out-Null; break } catch { Start-Sleep -Milliseconds 200 } }

    Write-Host "`n== 1. create twice with the same Idempotency-Key (class long, ~10 s of simulated work)"
    $c1 = AgentCli @("create", "--db", $state, "--tenant", "demo", "--key", "demo-key-1", "--class", "long") | ConvertFrom-Json
    $c2 = AgentCli @("create", "--db", $state, "--tenant", "demo", "--key", "demo-key-1", "--class", "long") | ConvertFrom-Json
    $c1, $c2 | Format-Table job_id, state, created
    $job = $c1.job_id

    Write-Host "== 2. start worker, kill it after $KillAfter s (TerminateProcess, no cleanup handlers)"
    $w = StartWorker "$d\worker1.log"
    Start-Sleep -Seconds $KillAfter
    Stop-Process -Id $w.Id -Force; $w.WaitForExit()
    $s = Status $job
    Write-Host ("state after kill: {0}; checkpointed steps: {1}" -f $s.state, ($s.steps_done -join ", "))
    Write-Host ("steps started by killed worker: {0}" -f ((StepStarts "$d\worker1.log") -join ", "))

    Write-Host "`n== 3. restart worker with the same name; nobody touches the job state"
    $w = StartWorker "$d\worker2.log"
    $s = WaitState $job @("WAITING_EVENT", "FAILED", "TIMED_OUT")
    Write-Host ("state: {0}; approval_id: {1}" -f $s.state, $s.approval_id)
    Write-Host ("steps started after restart: {0}" -f ((StepStarts "$d\worker2.log") -join ", "))

    Write-Host "`n== 4. the same approval event delivered twice (same event_id), then a late event after terminal"
    $e1 = AgentCli @("event", "--db", $state, $job, "--event-id", "evt-demo-1", "--approval-id", $s.approval_id)
    $e2 = AgentCli @("event", "--db", $state, $job, "--event-id", "evt-demo-1", "--approval-id", $s.approval_id)
    Write-Host "first : $e1"; Write-Host "second: $e2"
    $s = WaitState $job @("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
    $e3 = AgentCli @("event", "--db", $state, $job, "--event-id", "evt-demo-2", "--approval-id", $s.approval_id)
    $cx = AgentCli @("cancel", "--db", $state, $job)
    Write-Host "late approval: $e3"; Write-Host "late cancel  : $cx"
    Write-Host ("final state: {0}" -f (Status $job).state)

    Write-Host "`n== 5. external service (FakePublisherService) counters"
    Invoke-RestMethod "$pubUrl/v1/stats" | Format-List
    Write-Host "artifacts (DBs, JSON logs): $d"
}
finally {
    if ($w -and -not $w.HasExited) { Stop-Process -Id $w.Id -Force }
    if ($pub -and -not $pub.HasExited) { Stop-Process -Id $pub.Id -Force }
}
