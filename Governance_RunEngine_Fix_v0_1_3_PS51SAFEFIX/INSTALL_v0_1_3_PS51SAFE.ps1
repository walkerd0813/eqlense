param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir($p){ if(-not(Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null } }

Write-Host "============================================================"
Write-Host "[start] Fix Run-Engine v0_1_3 (PS5.1-safe)"
Write-Host "============================================================"
Write-Host ("  root:   {0}" -f $Root)
Write-Host ("  dryrun: {0}" -f $DryRun)

$src = Join-Path $PSScriptRoot "payload\scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"
$dst = Join-Path $Root "scripts\governance\Run-Engine_v0_1_PS51SAFE.ps1"

if(-not (Test-Path $src)) { throw "[error] missing payload file: $src" }
Ensure-Dir (Split-Path $dst -Parent)

$bak = $dst + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
if(Test-Path $dst){ Copy-Item -Path $dst -Destination $bak -Force; Write-Host ("[backup] {0}" -f $bak) }

if($DryRun){
  Write-Host ("[dryrun] would write {0}" -f $dst)
  exit 0
}

Copy-Item -Path $src -Destination $dst -Force
Write-Host ("[ok] wrote {0}" -f $dst)
Write-Host "[done] Run-Engine repaired"

Write-Host ""
Write-Host "Next (smoke test):"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\\scripts\\governance\\Validate-Registry_v0_1_PS51SAFE.ps1 -Root C:\\seller-app\\backend"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\\scripts\\governance\\Run-Engine_v0_1_PS51SAFE.ps1 -Root C:\\seller-app\\backend -EngineId market_radar.runbook_probes_v0_1 -Cmd python -CmdArgsLine \".\\scripts\\market_radar\\qa\\runbook_probes_v0_1.py --root C:\\seller-app\\backend --zip 02139 --assetBucket MF_5_PLUS --windowDays 30\""
