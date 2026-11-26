# Live demo on Kubernetes (Windows PowerShell + Docker Desktop with Kubernetes enabled, or any local cluster
# that can use the locally built image agentrt:local). Manifests: k8s/agentrt.yaml.
#   1. idempotent create (same Idempotency-Key twice)
#   2. kill -9 of the worker process inside the pod -> kubelet restarts the container -> job resumes,
#      checkpointed steps are not executed again
#   3. job waits for approval (WAITING_EVENT, approval_id)
#   4. the whole pod is deleted while the job waits -> StatefulSet re-creates it on the same PVC:
#      state, deadline and approval_id are preserved
#   5. the same approval twice (same event_id) -> applied once; late cancel -> already_terminal
#   6. FakePublisherService counters: exactly one logical publication per job
# Usage: .\scripts\demo_k8s.ps1 [-Pause] [-Fresh] [-SkipBuild] [-KeepRunning] [-KillAfter 3]
#   -Pause        wait for Enter before every step (for a live presentation)
#   -Fresh        delete the namespace first (empty databases)
#   -KeepRunning  leave the deployment running afterwards (default: delete the namespace)
param([switch]$Pause, [switch]$Fresh, [switch]$SkipBuild, [switch]$KeepRunning, [double]$KillAfter = 3)
$ErrorActionPreference = "Continue"   # kubectl/docker write progress to stderr; failures are checked explicitly
Set-Location (Join-Path $PSScriptRoot "..")
$ns = "agentrt"; $pod = "agentrt-0"; $port = 18000; $api = "http://127.0.0.1:$port"
$script:pf = $null

function Step([string]$title) {
    Write-Host ""
    Write-Host "== $title" -ForegroundColor Cyan
    if ($Pause) { Read-Host "   (Enter - continue)" | Out-Null }
}
function K([string[]]$a) {
    $out = & kubectl -n $ns @a 2>&1
    if ($LASTEXITCODE -ne 0) { throw "kubectl $a failed: $out" }
    $out
}
function Post($url, $body, $headers = @{}) {
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri $url -Headers $headers -ContentType "application/json" `
        -Body ($body | ConvertTo-Json -Compress -Depth 5)
    [pscustomobject]@{ code = $r.StatusCode; body = ($r.Content | ConvertFrom-Json) }
}
function Job($id) { Invoke-RestMethod "$api/jobs/$id" }
function Clock($t) { if ($null -eq $t) { return "-" }; [DateTimeOffset]::FromUnixTimeMilliseconds([long]([double]$t * 1000)).LocalDateTime.ToString("HH:mm:ss") }
function WaitState($id, [string[]]$want, [int]$timeoutS = 120) {
    $t = Get-Date
    while (((Get-Date) - $t).TotalSeconds -lt $timeoutS) {
        try { $j = Job $id; if ($want -contains $j.state) { return $j } } catch { }
        Start-Sleep -Milliseconds 250
    }
    throw "job $id did not reach $($want -join '/') in $timeoutS s"
}
function StopForward() {
    if ($script:pf -and -not $script:pf.HasExited) { Stop-Process -Id $script:pf.Id -Force -ErrorAction SilentlyContinue }
    $script:pf = $null
}
function StartForward() {
    StopForward
    & kubectl -n $ns wait --for=condition=Ready "pod/$pod" --timeout=120s 2>&1 | Out-Null
    $script:pf = Start-Process kubectl -ArgumentList "-n", $ns, "port-forward", "svc/api", "$($port):8000" `
        -PassThru -WindowStyle Hidden
    for ($i = 0; $i -lt 100; $i++) {
        try { Invoke-RestMethod "$api/metrics" | Out-Null; return } catch { Start-Sleep -Milliseconds 200 }
    }
    throw "API is not reachable through port-forward on $api"
}
function PublisherStats() {
    $py = "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8765/v1/stats').read().decode())"
    (K @("exec", "publisher-0", "--", "python", "-c", $py)) | ConvertFrom-Json
}
function WorkerInfo() {
    $c = (K @("get", "pod", $pod, "-o", "json") | Out-String | ConvertFrom-Json).status.containerStatuses |
        Where-Object name -eq "worker"
    $last = $c.lastState.terminated
    "worker container: restarts={0}; last exit: {1}" -f $c.restartCount, $(if ($last) { "code $($last.exitCode) ($($last.reason))" } else { "-" })
}
function StepStarts($id, [switch]$Previous) {
    $a = @("logs", $pod, "-c", "worker"); if ($Previous) { $a += "--previous" }
    $lines = & kubectl -n $ns @a 2>$null
    $lines | Where-Object { $_ -like '{*' } | ForEach-Object { try { $_ | ConvertFrom-Json } catch { } } |
        Where-Object { $_.msg -eq "step start" -and $_.job_id -eq $id } | ForEach-Object { "$($_.step)#a$($_.attempt)" }
}

Write-Host "kubectl context: $(& kubectl config current-context 2>&1)"
& kubectl get nodes 2>&1 | Out-Host
if ($LASTEXITCODE -ne 0) { throw "no Kubernetes cluster reachable (Docker Desktop: Settings -> Kubernetes -> Enable)" }

try {
    Step "0. build the image and deploy k8s/agentrt.yaml (publisher + api/worker, each with a PVC)"
    if ($Fresh) { & kubectl delete namespace $ns --wait=true --ignore-not-found 2>&1 | Out-Host }
    if (-not $SkipBuild) {
        & docker build -q -t agentrt:local . 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
    }
    & kubectl apply -f k8s/agentrt.yaml 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "kubectl apply failed" }
    K @("rollout", "status", "statefulset/publisher", "--timeout=180s") | Out-Host
    K @("rollout", "status", "statefulset/agentrt", "--timeout=180s") | Out-Host
    K @("get", "pods,svc,pvc") | Out-Host
    StartForward

    Step "1. POST /jobs twice with the same Idempotency-Key"
    $key = "k8s-demo-" + (Get-Date -Format "HHmmss")
    $body = @{ tenant_id = "demo"; job_class = "long" }
    $c1 = Post "$api/jobs" $body @{ "Idempotency-Key" = $key }
    $c2 = Post "$api/jobs" $body @{ "Idempotency-Key" = $key }
    Write-Host ("HTTP {0} {1} created={2}" -f $c1.code, $c1.body.job_id, $c1.body.created)
    Write-Host ("HTTP {0} {1} created={2}" -f $c2.code, $c2.body.job_id, $c2.body.created)
    $id = $c1.body.job_id

    Step "2. kill -9 the worker process inside the pod (after $KillAfter s of work)"
    Start-Sleep -Seconds $KillAfter
    $j = Job $id
    Write-Host ("before kill: state={0} checkpointed={1}" -f $j.state, ($j.steps_done -join ","))
    $kill = "import os,signal`nfor p in os.listdir('/proc'):`n    if p.isdigit():`n        try: c=open('/proc/'+p+'/cmdline','rb').read()`n        except OSError: continue`n        if b'agentrt.cli' in c and b'worker' in c and int(p)!=os.getpid(): os.kill(int(p), signal.SIGKILL); print('SIGKILL ->', p)"
    K @("exec", $pod, "-c", "api", "--", "python", "-c", $kill) | Out-Host
    Start-Sleep -Seconds 2
    Write-Host (WorkerInfo)
    Write-Host ("job right after the kill: state={0}" -f (Job $id).state)

    Step "3. the restarted worker resumes the job on its own -> WAITING_EVENT"
    $j = WaitState $id @("WAITING_EVENT", "FAILED", "TIMED_OUT")
    Write-Host ("state={0} approval_id={1} checkpointed={2}" -f $j.state, $j.approval_id, ($j.steps_done -join ","))
    Write-Host ("step starts before the kill: " + ((StepStarts $id -Previous) -join ", "))
    Write-Host ("step starts after  the kill: " + ((StepStarts $id) -join ", "))

    Step "4. delete the whole pod while the job waits for approval"
    Write-Host ("before: state={0} approval_id={1} deadline={2}" -f $j.state, $j.approval_id, (Clock $j.deadline))
    StopForward
    & kubectl -n $ns delete pod $pod --grace-period=0 --force 2>&1 | Out-Null
    & kubectl -n $ns rollout status statefulset/agentrt --timeout=180s 2>&1 | Out-Null
    StartForward
    $j2 = Job $id
    Write-Host ("after : state={0} approval_id={1} deadline={2} (new pod, same PVC)" -f $j2.state, $j2.approval_id, (Clock $j2.deadline))

    Step "5. the same approval twice (same event_id), then a late cancel"
    $ev = @{ event_id = "evt-$key"; type = "approval"; payload = @{ approval_id = $j2.approval_id; decision = "approve" } }
    $e1 = Post "$api/jobs/$id/events" $ev; $e2 = Post "$api/jobs/$id/events" $ev
    Write-Host ("first : HTTP {0} outcome={1}" -f $e1.code, $e1.body.outcome)
    Write-Host ("second: HTTP {0} outcome={1}" -f $e2.code, $e2.body.outcome)
    $j = WaitState $id @("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
    $cx = Post "$api/jobs/$id/cancel" @{}
    Write-Host ("final state={0}; late cancel outcome={1}" -f $j.state, $cx.body.outcome)
    Write-Host ("transitions: " + (($j.transitions | ForEach-Object { "$(if ($_.src) { $_.src } else { "new" })->$($_.dst)" }) -join ", "))

    Step "6. FakePublisherService counters"
    $s = PublisherStats
    Write-Host ("physical publish requests={0}, logical publications={1}, replays={2}" -f $s.physical_publish_requests, $s.logical_publications, $s.replays)
}
finally {
    StopForward
    if (-not $KeepRunning) {
        & kubectl delete namespace $ns --wait=false 2>&1 | Out-Null
        Write-Host "namespace $ns deleted (use -KeepRunning to keep it)"
    }
}
