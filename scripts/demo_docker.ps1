# End-to-end check of docker-compose.yml through the HTTP API (Windows PowerShell + Docker Desktop).
# Linux containers; the worker is killed with SIGKILL (`docker compose kill` default signal).
#   1. idempotent create (same Idempotency-Key twice)          4. duplicate approval (same event_id) + late cancel
#   2. SIGKILL the worker mid-job, start it again -> resumes    5. containers recreated while WAITING_EVENT -> state,
#   3. job reaches WAITING_EVENT with an approval_id              deadline and approval_id survive (named volume)
# Usage: .\scripts\demo_docker.ps1 [-KillAfter 3] [-KeepRunning]
param([double]$KillAfter = 3, [switch]$KeepRunning)
$ErrorActionPreference = "Continue"   # docker writes progress to stderr; failures are checked via $LASTEXITCODE / throw
Set-Location (Join-Path $PSScriptRoot "..")
$api = "http://127.0.0.1:8000"; $pub = "http://127.0.0.1:8765"; $p = "agentrt"
$env:LLM = "sim"

function Dc([string[]]$a) { & docker compose -p $p @a 2>&1 | Out-Null; if ($LASTEXITCODE -ne 0) { throw "docker compose $a failed" } }
function WaitHttp($url) { for ($i = 0; $i -lt 150; $i++) { try { Invoke-RestMethod $url | Out-Null; return } catch { Start-Sleep -Milliseconds 200 } }; throw "$url not up" }
function Post($url, $body, $headers = @{}) {
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri $url -Headers $headers -ContentType "application/json" `
        -Body ($body | ConvertTo-Json -Compress -Depth 5)
    [pscustomobject]@{ code = $r.StatusCode; body = ($r.Content | ConvertFrom-Json) }
}
function Job($id) { Invoke-RestMethod "$api/jobs/$id" }
function WaitState($id, [string[]]$want, [int]$timeoutS = 120) {
    $t = Get-Date
    while (((Get-Date) - $t).TotalSeconds -lt $timeoutS) { $j = Job $id; if ($want -contains $j.state) { return $j }; Start-Sleep -Milliseconds 250 }
    throw "job $id did not reach $($want -join '/') in $timeoutS s"
}

Write-Host "== 0. build and start: publisher, api, worker (fresh volume)"
& docker compose -p $p down -v 2>&1 | Out-Null
Dc @("up", "-d", "--build")
WaitHttp "$api/metrics"; WaitHttp "$pub/health"
try {
    Write-Host "`n== 1. POST /jobs twice with the same Idempotency-Key"
    $body = @{ tenant_id = "demo"; job_class = "long" }
    $c1 = Post "$api/jobs" $body @{ "Idempotency-Key" = "docker-demo-1" }
    $c2 = Post "$api/jobs" $body @{ "Idempotency-Key" = "docker-demo-1" }
    "HTTP {0} {1} created={2}" -f $c1.code, $c1.body.job_id, $c1.body.created
    "HTTP {0} {1} created={2}" -f $c2.code, $c2.body.job_id, $c2.body.created
    $id = $c1.body.job_id

    Write-Host "`n== 2. SIGKILL the worker container after $KillAfter s, then start it again"
    Start-Sleep -Seconds $KillAfter
    Dc @("kill", "-s", "SIGKILL", "worker")
    $j = Job $id
    "after kill: state={0} checkpointed={1}" -f $j.state, ($j.steps_done -join ",")
    "worker exit: " + (& docker inspect -f "{{.State.ExitCode}} OOMKilled={{.State.OOMKilled}}" "$p-worker-1")
    Dc @("start", "worker")

    Write-Host "`n== 3. resumes on its own -> WAITING_EVENT"
    $j = WaitState $id @("WAITING_EVENT", "FAILED", "TIMED_OUT")
    "state={0} approval_id={1} checkpointed={2}" -f $j.state, $j.approval_id, ($j.steps_done -join ",")
    $starts = (& docker compose -p $p logs --no-log-prefix worker 2>$null) | Where-Object { $_ -like '{*' } |
        ForEach-Object { $_ | ConvertFrom-Json } | Where-Object { $_.msg -eq "step start" -and $_.job_id -eq $id }
    "all step starts of this job (before and after the kill): " +(($starts | ForEach-Object { "$($_.step)#a$($_.attempt)" }) -join ", ")

    Write-Host "`n== 4. the same approval twice (same event_id), then a late cancel"
    $ev = @{ event_id = "evt-docker-1"; type = "approval"; payload = @{ approval_id = $j.approval_id; decision = "approve" } }
    $e1 = Post "$api/jobs/$id/events" $ev; $e2 = Post "$api/jobs/$id/events" $ev
    "first : HTTP {0} outcome={1}" -f $e1.code, $e1.body.outcome
    "second: HTTP {0} outcome={1}" -f $e2.code, $e2.body.outcome
    $j = WaitState $id @("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
    $cx = Post "$api/jobs/$id/cancel" @{}
    "final state={0}; late cancel outcome={1}" -f $j.state, $cx.body.outcome

    Write-Host "`n== 5. second job: recreate ALL containers while it waits for approval"
    $k = Post "$api/jobs" @{ tenant_id = "demo"; job_class = "short" } @{ "Idempotency-Key" = "docker-demo-2" }
    $id2 = $k.body.job_id
    $w = WaitState $id2 @("WAITING_EVENT")
    "before: state={0} approval_id={1} deadline={2}" -f $w.state, $w.approval_id, $w.deadline
    Dc @("down")                       # containers removed, named volume kept
    Dc @("up", "-d")
    WaitHttp "$api/metrics"
    $w2 = Job $id2
    "after : state={0} approval_id={1} deadline={2}" -f $w2.state, $w2.approval_id, $w2.deadline
    $e = Post "$api/jobs/$id2/events" @{ event_id = "evt-docker-2"; type = "approval"; payload = @{ approval_id = $w2.approval_id; decision = "approve" } }
    "approval after recreate: outcome={0}" -f $e.body.outcome
    "final state: " + (WaitState $id2 @("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")).state

    Write-Host "`n== 6. FakePublisherService counters (2 jobs -> expect 2 logical publications)"
    Invoke-RestMethod "$pub/v1/stats" | Format-List
}
finally {
    if (-not $KeepRunning) { & docker compose -p $p down -v 2>&1 | Out-Null; Write-Host "stack removed (docker compose down -v)" }
}
